# batch-oee
Modular toolkit for analysis on batch mfg processes 

# OEE Calculator for Batch Manufacturing (ISA-88 Compatible)

This repository contains a Python module designed for calculating OEE (Overall Equipment Effectiveness) in batch manufacturing environments, using the ISA-88 batch recipe structure. The module supports value-added time calculations, loss categorization, and flexible configurations through YAML files.

---

## Features

1. **ISA-88 Compatibility**:
   - Supports Procedures, Unit Procedures, and Operations hierarchy.
   - Handles timestamped event data from systems like DeltaV Batch, Rockwell FactoryTalk, and Aveva InBatch.

2. **Loss Categorization**:
   - Classifies operations into the following categories:
     - Value-Added
     - Unplanned Stop
     - Planned Stop
     - Speed Loss
     - Small Stop
     - Rework/Scrap
     - Startup Loss

3. **Dynamic Configurations**:
   - Value-added time thresholds and loss mappings can be configured via a YAML file.

4. **Handles Missing Data**:
   - Automatically accounts for missing end timestamps and ongoing operations.

5. **Time Range Filtering**:
   - Analyze operations within specific time ranges (e.g., last 30 minutes or a calendar day).

6. **OEE Calculation**:
   - Computes Availability, Performance, and Quality metrics.

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/oee-calculator.git
   cd oee-calculator

	2.	Install dependencies:

pip install -r requirements.txt

Usage

1. Prepare Input Data

Create a CSV or DataFrame with the following structure:

timestamp_start	timestamp_end	operation	unit_id	batch_id
2024-12-14 08:00:00	2024-12-14 08:30:00	heating	Unit1	Batch123
2024-12-14 08:30:00	2024-12-14 09:00:00	cooling	Unit1	Batch123
2024-12-14 09:00:00	NaT	mixing	Unit1	Batch123

	•	Replace NaT for missing end timestamps.

2. Prepare a Configuration File

Define a YAML file (config.yaml) with value-added times and loss mappings:

value_added_times:
  heating: 30  # Value-added for 30 minutes
  cooling: 20  # Value-added for 20 minutes
  mixing: 15   # Value-added for 15 minutes

loss_mappings:
  heating: "speed loss"
  cooling: "planned stop"
  idle: "unplanned stop"
  mixing: "value-added"
  rework: "rework/scrap"
  startup: "startup loss"

3. Run the Calculator

Example script to calculate OEE:

from oee_calculator import OEECalculator
import pandas as pd
from datetime import datetime

# Load operation data
data = {
    "timestamp_start": ["2024-12-14 08:00:00", "2024-12-14 08:30:00", "2024-12-14 09:00:00"],
    "timestamp_end": ["2024-12-14 08:30:00", "2024-12-14 09:00:00", None],
    "operation": ["heating", "cooling", "mixing"],
    "unit_id": ["Unit1", "Unit1", "Unit1"],
    "batch_id": ["Batch123", "Batch123", "Batch123"],
}
operations_df = pd.DataFrame(data)

# Parse timestamps
operations_df["timestamp_start"] = pd.to_datetime(operations_df["timestamp_start"])
operations_df["timestamp_end"] = pd.to_datetime(operations_df["timestamp_end"])

# Initialize OEECalculator
calculator = OEECalculator("config.yaml")

# Define time range
start_time = datetime(2024, 12, 14, 8, 0)
end_time = datetime(2024, 12, 14, 10, 0)

# Calculate OEE
result = calculator.calculate_oee(operations_df, start_time, end_time)

# Print results
print("OEE Metrics:", result["oee_metrics"])
print("Time Breakdown:", result["time_breakdown"])

Outputs

1. OEE Metrics

Key OEE metrics:
	•	Availability: Time equipment is available for production.
	•	Performance: Actual vs. optimal production speed.
	•	Quality: Proportion of good output.

2. Time Breakdown

Total duration in each category:
	•	Value-Added
	•	Loss Types (e.g., Speed Loss, Planned Stop, etc.)

3. Processed Data

A DataFrame with categorized operations and durations.

Contribution

We welcome contributions! To get started:
	1.	Fork the repository.
	2.	Create a feature branch: git checkout -b feature-name.
	3.	Submit a pull request.

License

This project is licensed under the MIT License. See the LICENSE file for details.

Roadmap
	•	Add visualization tools (e.g., Gantt charts).
	•	Support direct integration with common batch servers (e.g., DeltaV, FactoryTalk).
	•	Extend OEE calculations to include Quality metrics.

Contact

For questions or support, reach out via GitHub issues

---

You can save this file as `README.txt` in the root of your repository.