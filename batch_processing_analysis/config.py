import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


def get_project_dir() -> Path:
    return Path(os.path.dirname(__file__)).parent


@dataclass
class EventLogIDs:
    case: str = 'case_id'
    activity: str = 'Activity'
    start_timestamp: str = 'start_time'
    end_timestamp: str = 'end_time'
    enabled_time: str = 'enabled_time'
    available_time: str = 'available_time'
    resource: str = 'Resource'
    lifecycle: str = 'Lifecycle'


DEFAULT_CSV_IDS = EventLogIDs(case='case_id',
                              activity='Activity',
                              start_timestamp='start_time',
                              end_timestamp='end_time',
                              enabled_time='enabled_time',
                              available_time='available_time',
                              resource='Resource',
                              lifecycle='Lifecycle')


@dataclass
class Configuration:
    """Class storing the configuration parameters for the start time estimation.

    Attributes:
        log_ids                     Identifiers for each key element (e.g. executed activity or resource).
        non_estimated_time          Time to use as value when the enabled time cannot be calculated.
    """
    log_ids: EventLogIDs = DEFAULT_CSV_IDS
    non_estimated_time: pd.Timestamp = pd.NaT

    PATH_PROJECT = get_project_dir()
    PATH_EXTERNAL_TOOLS = PATH_PROJECT.joinpath("external")
    PATH_BATCH_DETECTION_FOLDER = PATH_EXTERNAL_TOOLS.joinpath("batch-detection")
    PATH_BATCH_DETECTION_SCRIPT = PATH_BATCH_DETECTION_FOLDER.joinpath("batch_detection.R")
    PATH_R_EXECUTABLE = "C:/Program Files/R/R-4.1.2/bin/Rscript.exe"
