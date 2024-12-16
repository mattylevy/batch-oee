import pandas as pd
from datetime import datetime, timedelta
from oee_calculator import OEECalculator  # Replace with your module name

# Step 1: Create a sample dataset
data = [
    {"operation": "Mixing", "timestamp_start": "2024-12-15 08:00:00", "timestamp_end": "2024-12-15 08:30:00", "loss_category": "unplanned_stop"},
    {"operation": "Heating", "timestamp_start": "2024-12-15 08:30:00", "timestamp_end": "2024-12-15 09:00:00", "loss_category": "speed_loss"},
    {"operation": "Cooling", "timestamp_start": "2024-12-15 09:00:00", "timestamp_end": "2024-12-15 09:30:00", "loss_category": "value_added"},
]

operations_df = pd.DataFrame(data)

# Step 2: Define the value-added times (sample YAML/JSON would load these)
value_added_times = {"Mixing": 900, "Heating": 1800, "Cooling": 1200}  # seconds

# Step 3: Initialize the OEECalculator
calculator = OEECalculator()
calculator.value_added_times = value_added_times  # Alternatively, load from config file

# Step 4: Specify time range for OEE calculation
start_time = datetime(2024, 12, 15, 8, 0, 0)
end_time = datetime(2024, 12, 15, 10, 0, 0)

# Step 5: Perform OEE calculation
oee_metrics = calculator.calculate_oee(operations_df, start_time, end_time)

# Step 6: Display results
print("OEE Metrics:")
for metric, value in oee_metrics.items():
    print(f"{metric.capitalize()}: {value}")

# Optional: Visualize results (requires matplotlib)
try:
    import matplotlib.pyplot as plt

    categories = ["Availability Losses", "Performance Losses", "Quality Losses", "Value-Added Time"]
    values = [oee_metrics["availability_losses"], oee_metrics["performance_losses"], oee_metrics["quality_losses"], oee_metrics["value_added_time"]]

    plt.figure(figsize=(8, 5))
    plt.bar(categories, values, color=['red', 'orange', 'green', 'blue'])
    plt.title("OEE Metrics Breakdown")
    plt.ylabel("Time (seconds)")
    plt.show()
except ImportError:
    print("Install matplotlib for visualizations: pip install matplotlib")