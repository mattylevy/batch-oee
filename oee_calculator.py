# oee_calculator.py

import pandas as pd
import numpy as np
from datetime import datetime
import yaml

class OEECalculator:
    def __init__(self, config_path):
        """
        Initialize the calculator with configuration for operations and value-added thresholds.
        :param config_path: Path to the YAML/JSON config file.
        """
        self.config = self.load_config(config_path)

    @staticmethod
    def load_config(config_path):
        """
        Load configuration from a YAML/JSON file.
        """
        with open(config_path, "r") as file:
            return yaml.safe_load(file)

    def calculate_oee(self, operations_df, start_time, end_time):
        """
        Calculate OEE for a given time range.
        :param operations_df: DataFrame with batch operation data.
        :param start_time: Start of the time range for OEE calculation.
        :param end_time: End of the time range for OEE calculation.
        :return: Dictionary with OEE metrics and processed data.
        """
        # Step 1: Handle missing timestamp_end
        operations_df = self.handle_missing_timestamps(operations_df, end_time)

        # Step 2: Filter operations within the time range
        operations_df = self.filter_time_range(operations_df, start_time, end_time)

        # Step 3: Categorize operations into value-added, delays, and losses
        operations_df = self.categorize_operations(operations_df)

        # Step 4: Calculate total time for each category
        time_breakdown = self.calculate_time_breakdown(operations_df)

        # Step 5: Compute OEE metrics
        oee_metrics = self.compute_oee_metrics(time_breakdown, end_time - start_time)

        return {
            "oee_metrics": oee_metrics,
            "time_breakdown": time_breakdown,
            "processed_operations": operations_df
        }

    def handle_missing_timestamps(self, operations_df, default_end_time):
        """
        Handles missing `timestamp_end` values by using the default end time or marking as ongoing.
        """
        operations_df['timestamp_start'] = pd.to_datetime(operations_df['timestamp_start'])
        operations_df['timestamp_end'] = pd.to_datetime(operations_df['timestamp_end'], errors='coerce')

        # Replace missing end timestamps with the default end time (e.g., current time)
        ongoing_mask = operations_df['timestamp_end'].isna()
        operations_df.loc[ongoing_mask, 'timestamp_end'] = default_end_time

        # Add a flag to indicate ongoing operations
        operations_df['is_ongoing'] = ongoing_mask

        return operations_df

    def filter_time_range(self, operations_df, start_time, end_time):
        """
        Filters operations to include only those overlapping with the time range.
        """
        return operations_df[
            (operations_df['timestamp_end'] > start_time) & (operations_df['timestamp_start'] < end_time)
        ]

    def categorize_operations(self, operations_df):
        """
        Categorize operations based on value-added time limits and loss types.
        """
        def categorize(row):
            operation = row['operation']
            duration = (row['timestamp_end'] - row['timestamp_start']).total_seconds() / 60  # in minutes

            if operation in self.config['value_added_times']:
                limit = self.config['value_added_times'][operation]
                return "value-added" if duration <= limit else self.config['loss_mappings'].get(operation, "unclassified")
            return self.config['loss_mappings'].get(operation, "unclassified")

        operations_df['category'] = operations_df.apply(categorize, axis=1)
        return operations_df

    def calculate_time_breakdown(self, operations_df):
        """
        Summarizes total time spent in each category.
        """
        operations_df['duration'] = (operations_df['timestamp_end'] - operations_df['timestamp_start']).dt.total_seconds()
        time_breakdown = operations_df.groupby('category')['duration'].sum().to_dict()
        return time_breakdown

    def compute_oee_metrics(self, time_breakdown, total_time):
        """
        Computes OEE metrics: Availability, Performance, Quality.
        """
        availability = time_breakdown.get("value-added", 0) / total_time
        performance = time_breakdown.get("value-added", 0) / (time_breakdown.get("value-added", 0) + time_breakdown.get("delay", 0))
        quality = 1.0  # Placeholder, to be extended if quality data is available
        return {
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "oee": availability * performance * quality
        }