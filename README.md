# Batch Processing Analysis

Python implementation of the batch processing analysis technique presented in the paper "Data-Driven Analysis of Batch Processing Inefficiencies in Business Processes", by Katsiaryna Lashkevich, Fredrik Milani, David Chapela-Campa and Marlon Dumas.

Given an event log as input (Pandas DataFrame), this technique:

1. Identify and classify the batches in the event log using [BAMA](https://github.com/nielsmartin/bama).
2. Analize the different waiting times (batch created, batch ready, other...) of each batch and gives a report of the impact of each waiting time type in the cycle time efficiency.
3. Extract, using [RIPPER](https://github.com/imoscovitz/wittgenstein), the rules that lead to an activation of a batch.

## Requirements

- **Python v3.9.5+**
- **PIP v21.1.2+**
- Dependencies: [This package](https://github.com/AutomatedProcessImprovement/start-time-estimator) and those listed in `requirements.txt`
- External: **R v4.1.2+** (check R dependencies in `external/batch-detection/batch_detection.R`)

## Basic Usage

Check `batch_processing_analysis/main.py` for a simple execution, and `batch_processing_analysis/batch_config.py` for configuration parameters.
