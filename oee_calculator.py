import pandas as pd
from datetime import datetime
import warnings
import yaml


class OEECalculator:
    def __init__(self, config_file=None):
        """
        Initialize the OEECalculator with an optional configuration file.

        Parameters:
            config_file (str): Path to a YAML file containing value-added times for each operation.
        """
        # Load value-added times if a config file is provided; default to an empty dictionary otherwise
        self.value_added_times = self.load_value_added_times(config_file) if config_file else {}

    def load_value_added_times(self, config_file):
        """
        Load value-added times from a YAML configuration file.

        Parameters:
            config_file (str): Path to the configuration file.

        Returns:
            dict: A dictionary mapping operations to value-added times.
        """
        try:
            with open(config_file, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file '{config_file}' not found.")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML config file: {e}")

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
        # Ensure timestamps are converted to datetime objects
        operations_df["timestamp_start"] = pd.to_datetime(operations_df["timestamp_start"])
        operations_df["timestamp_end"] = pd.to_datetime(operations_df["timestamp_end"], errors="coerce")

        # Fill missing end timestamps with the specified end_time
        operations_df["timestamp_end"] = operations_df["timestamp_end"].fillna(pd.Timestamp(end_time))

        # Calculate effective start and end times, clipped to the specified time range
        operations_df["effective_start"] = operations_df["timestamp_start"].clip(lower=pd.Timestamp(start_time))
        operations_df["effective_end"] = operations_df["timestamp_end"].clip(upper=pd.Timestamp(end_time))

        # Calculate effective duration in seconds, ensuring non-negative values
        operations_df["effective_duration"] = (
            (operations_df["effective_end"] - operations_df["effective_start"]).dt.total_seconds()
        ).clip(lower=0)

        # Remove rows with zero effective duration (completely outside the time range)
        operations_df = operations_df[operations_df["effective_duration"] > 0]

        # Calculate prorated value-added time for each row
        operations_df["prorated_value_added_time"] = operations_df.apply(
            lambda row: self.calculate_prorated_value_added_time(
                row["operation"], row["timestamp_start"], row["timestamp_end"], start_time, end_time
            ),
            axis=1,
        )

        return operations_df

    def calculate_prorated_value_added_time(self, operation, timestamp_start, timestamp_end, range_start, range_end):
        """
        Calculate prorated value-added time for a given operation within a specific time range.

        Parameters:
            operation (str): The name of the operation.
            timestamp_start (datetime): Actual start time of the operation.
            timestamp_end (datetime): Actual end time of the operation.
            range_start (datetime): Start of the time range.
            range_end (datetime): End of the time range.

        Returns:
            float: Prorated value-added time in seconds.
        """
        if operation not in self.value_added_times:
            warnings.warn(f"Operation '{operation}' not found in value-added times config. Defaulting to 0.")
            return 0

        # Get the standard value-added time for the operation
        standard_value_added_time = self.value_added_times[operation]

        # Calculate expected end time using the standard value-added time
        expected_end = timestamp_start + pd.Timedelta(seconds=standard_value_added_time)

        # Determine the overlap between the expected value-added window and the specified range
        overlap_start = max(timestamp_start, pd.Timestamp(range_start))
        overlap_end = min(expected_end, pd.Timestamp(range_end))

        # Calculate the overlap duration in seconds
        overlap_duration = max((overlap_end - overlap_start).total_seconds(), 0)

        # Return the prorated value-added time as the overlap duration
        return overlap_duration

    def calculate_oee(self, operations_df, start_time, end_time, overrides=None):
        """
        Calculate OEE metrics within a specific time range, accounting for default and overridden loss categories.

        Parameters:
            operations_df (pd.DataFrame): Table of batch operations with default loss categories.
            start_time (datetime): Start of the time range for OEE calculation.
            end_time (datetime): End of the time range for OEE calculation.
            overrides (dict): Optional mapping of operation IDs to overridden loss categories.

        Returns:
            dict: OEE metrics including availability, performance, quality, and overall OEE.
        """
        # Apply truncation for the time range
        truncated_df = self.truncate_events(operations_df, start_time, end_time)

        # Apply overrides for loss categories if provided
        if overrides:
            truncated_df["loss_category"] = truncated_df["operation"].map(overrides).fillna(truncated_df["loss_category"])

        # Handle case with no valid operations in the time range
        if truncated_df.empty:
            print(f"No valid operations found within the time range: {start_time} to {end_time}")
            return {
                "availability": 0,
                "performance": 0,
                "quality": 0,
                "oee": 0,
            }

        # Calculate total time in the time range
        total_time = (pd.Timestamp(end_time) - pd.Timestamp(start_time)).total_seconds()

        # Aggregate losses by category
        availability_losses = truncated_df.loc[
            truncated_df["loss_category"].isin(["unplanned_stop", "planned_stop"]), "effective_duration"
        ].sum()

        performance_losses = truncated_df.loc[
            truncated_df["loss_category"].isin(["small_stop", "speed_loss"]), "effective_duration"
        ].sum()

        quality_losses = truncated_df.loc[
            truncated_df["loss_category"].isin(["rework/scrap", "startup_loss"]), "effective_duration"
        ].sum()

        # Aggregate value-added time
        value_added_time = truncated_df["prorated_value_added_time"].sum()

        # Calculate OEE components (prevent division by zero)
        availability = max((total_time - availability_losses) / total_time, 0) if total_time > 0 else 0
        performance = max(1 - (performance_losses / total_time), 0) if total_time > 0 else 0
        quality = max(1 - (quality_losses / total_time), 0) if total_time > 0 else 0

        # Calculate overall OEE
        oee = availability * performance * quality

        return {
            "availability": round(availability, 3),
            "performance": round(performance, 3),
            "quality": round(quality, 3),
            "oee": round(oee, 3),
            "value_added_time": round(value_added_time, 2),
            "availability_losses": round(availability_losses, 2),
            "performance_losses": round(performance_losses, 2),
            "quality_losses": round(quality_losses, 2),
            "total_time": round(total_time, 2),
        }