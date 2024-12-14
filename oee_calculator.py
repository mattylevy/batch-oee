import pandas as pd

class OEECalculator:
    def __init__(self, config_file="value_added_times.yaml"):
        """
        Initialize the OEE Calculator with a configuration file.
        """
        self.config = self.load_config(config_file)

    @staticmethod
    def load_config(config_file):
        """
        Load the YAML configuration file.
        """
        import yaml
        with open(config_file, "r") as file:
            return yaml.safe_load(file)

    def truncate_events(self, events_df, start_time, end_time):
        """
        Adjust events to fit within the provided time range.
        Handles overlapping operations and prorates value-added times.
        """
        # Truncate start and end times to fit within the time window
        events_df["truncated_start"] = events_df["timestamp_start"].apply(
            lambda x: max(x, start_time)
        )
        events_df["truncated_end"] = events_df["timestamp_end"].apply(
            lambda x: min(x, end_time) if pd.notnull(x) else end_time
        )

        # Calculate actual duration within the time window
        events_df["effective_duration"] = (
            (events_df["truncated_end"] - events_df["truncated_start"])
            .dt.total_seconds()
            .clip(lower=0)
        )

        # Add a standard value-added duration column based on the config
        events_df["standard_duration"] = events_df["operation"].map(
            lambda op: self.config["value_added_operations"].get(op, 0) * 60  # Convert minutes to seconds
        )

        # Prorate the standard value-added time based on the effective duration
        events_df["prorated_value_added_time"] = (
            events_df["effective_duration"] / 
            (events_df["timestamp_end"] - events_df["timestamp_start"]).dt.total_seconds()
        ).fillna(1) * events_df["standard_duration"]

        # Ensure valid prorated times (handle divide-by-zero or missing data issues)
        events_df["prorated_value_added_time"] = events_df["prorated_value_added_time"].clip(lower=0)

        return events_df

    def calculate_oee(self, operations_df, start_time, end_time):
        """
        Calculate OEE metrics within a specific time range.
        Truncates events to fit within this time range and calculates metrics.
        """
        # Truncate operations to fit time range
        truncated_df = self.truncate_events(operations_df, start_time, end_time)

        # Filter out operations with zero effective duration
        truncated_df = truncated_df[truncated_df["effective_duration"] > 0]

        # Calculate total duration and categorized losses
        total_time = truncated_df["effective_duration"].sum()

        if total_time == 0:
            # No effective time means zero OEE components
            return {
                "availability": 0,
                "performance": 0,
                "quality": 0,
                "oee": 0,
            }

        # Calculate categorized durations
        grouped_durations = truncated_df.groupby("loss_category")["effective_duration"].sum()

        # Value-added time (prorated sum)
        value_added_time = truncated_df["prorated_value_added_time"].sum()

        # OEE component calculations
        availability = value_added_time / total_time if total_time else 0
        performance = 1 - grouped_durations.get("speed_loss", 0) / total_time
        quality = 1 - grouped_durations.get("rework/scrap", 0) / total_time

        # Overall OEE
        oee = availability * performance * quality

        return {
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "oee": oee,
        }