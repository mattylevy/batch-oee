import pandas as pd
from datetime import datetime, timedelta

class OEECalculator:
    def __init__(self, config_file=None):
        """
        Initialize the OEECalculator with an optional configuration file.
        
        Parameters:
            config_file (str): Path to a YAML or JSON file containing value-added times for each operation.
        """
        if config_file:
            self.value_added_times = self.load_value_added_times(config_file)
        else:
            self.value_added_times = {}

    def load_value_added_times(self, config_file):
        """
        Load value-added times from a configuration file.

        Parameters:
            config_file (str): Path to the configuration file.

        Returns:
            dict: A dictionary mapping operations to value-added times.
        """
        import yaml
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def truncate_events(self, operations_df, start_time, end_time):
        """
        Truncate operations to fit within the specified time range.

        Parameters:
            operations_df (pd.DataFrame): DataFrame of operations with timestamps.
            start_time (datetime): Start of the time range.
            end_time (datetime): End of the time range.

        Returns:
            pd.DataFrame: DataFrame with truncated event durations and value-added times.
        """
        # Ensure timestamps are datetime objects
        operations_df["timestamp_start"] = pd.to_datetime(operations_df["timestamp_start"])
        operations_df["timestamp_end"] = pd.to_datetime(operations_df["timestamp_end"])

        # Handle events with missing end timestamps (assume they end at the time range's end)
        operations_df["timestamp_end"] = operations_df["timestamp_end"].fillna(end_time)

        # Calculate effective duration within the time range
        operations_df["effective_start"] = operations_df[["timestamp_start", pd.Timestamp(start_time)]].max(axis=1)
        operations_df["effective_end"] = operations_df[["timestamp_end", pd.Timestamp(end_time)]].min(axis=1)
        operations_df["effective_duration"] = (
            (operations_df["effective_end"] - operations_df["effective_start"]).dt.total_seconds().clip(lower=0)
        )

        # Prorate value-added time for truncated events
        operations_df["prorated_value_added_time"] = operations_df.apply(
            lambda row: self.calculate_prorated_value_added_time(
                row["operation"], row["effective_duration"]
            ),
            axis=1
        )

        return operations_df

    def calculate_prorated_value_added_time(self, operation, effective_duration):
        """
        Calculate prorated value-added time for a given operation.

        Parameters:
            operation (str): The name of the operation.
            effective_duration (float): The effective duration of the operation in seconds.

        Returns:
            float: Prorated value-added time in seconds.
        """
        standard_value_added_time = self.value_added_times.get(operation, None)
        if standard_value_added_time:
            return min(effective_duration, standard_value_added_time)
        return 0

    def calculate_oee(self, operations_df, start_time, end_time, overrides=None):
        """
        Calculate OEE metrics within a specific time range, accounting for default and overridden loss categories.

        Parameters:
            operations_df (pd.DataFrame): Table of batch operations with default loss categories.
            start_time (datetime): Start of the time range for OEE calculation.
            end_time (datetime): End of the time range for OEE calculation.
            overrides (dict): Optional mapping of operation IDs to overridden loss categories.
                              Example: {"operation_1": "speed_loss"}

        Returns:
            dict: OEE metrics including availability, performance, quality, and overall OEE.
        """
        # Apply truncation for the time range
        truncated_df = self.truncate_events(operations_df, start_time, end_time)

        # Filter out operations with zero effective duration
        truncated_df = truncated_df[truncated_df["effective_duration"] > 0]

        # Handle overrides for loss categories
        if overrides:
            # Apply overrides where specified
            truncated_df["loss_category"] = truncated_df.apply(
                lambda row: overrides.get(row["operation"], row["loss_category"]), axis=1
            )

        # Calculate total duration in the time range
        total_time = truncated_df["effective_duration"].sum()

        if total_time == 0:
            # No effective time means zero OEE components
            return {
                "availability": 0,
                "performance": 0,
                "quality": 0,
                "oee": 0,
            }

        # Categorize losses
        availability_losses = truncated_df[truncated_df["loss_category"].isin(
            ["unplanned_stop", "planned_stop"]
        )]["effective_duration"].sum()

        performance_losses = truncated_df[truncated_df["loss_category"].isin(
            ["small_stop", "speed_loss"]
        )]["effective_duration"].sum()

        quality_losses = truncated_df[truncated_df["loss_category"].isin(
            ["rework/scrap", "startup_loss"]
        )]["effective_duration"].sum()

        # Value-added time
        value_added_time = truncated_df["prorated_value_added_time"].sum()

        # OEE components
        availability = (total_time - availability_losses) / total_time
        performance = 1 - performance_losses / total_time
        quality = 1 - quality_losses / total_time

        # Overall OEE
        oee = availability * performance * quality

        return {
            "availability": round(availability, 3),
            "performance": round(performance, 3),
            "quality": round(quality, 3),
            "oee": round(oee, 3),
        }