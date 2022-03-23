# Batch Processing Analysis

Python implementation of the batch processing analysis technique presented in the paper "Data-Driven Analysis of Batch Processing Inefficiencies in Business Processes", by Katsiaryna Lashkevich, Fredrik Milani, David Chapela-Campa and Marlon Dumas.

Given an event log as input (Pandas DataFrame), this technique:

1. Identify and classify the batches in the event log using [BAMA](https://github.com/nielsmartin/bama).
2. Analize the different waiting times (batch created, batch ready, other...) of each batch and gives a report of the impact of each waiting time type in the cycle time efficiency.
3. Extract, using [RIPPER](https://github.com/imoscovitz/wittgenstein), the rules that lead to an activation of a batch.

## Requirements

- **Python v3.9.5+**
- **PIP v21.1.2+**
- Python dependencies: 
  - [This package](https://github.com/AutomatedProcessImprovement/start-time-estimator).
  - The packages listed in `requirements.txt`.
  - For the *wittenstein* package dependency, better install [this fork](https://github.com/david-chapela/wittgenstein) until its push request is accepted in the original repository.
- External dependencies: **R v4.1.2+** and the R packages listed in the header of `external/batch-detection/batch_detection.R`.

## Basic Usage

Check [main file](https://github.com/AutomatedProcessImprovement/batch-processing-analysis/blob/main/src/preprocessing/main.py) for an example of a simple execution, and the [config file](https://github.com/AutomatedProcessImprovement/batch-processing-analysis/blob/main/src/batch_processing_analysis/config.py) for an explanation of the configuration parameters.
