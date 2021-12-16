import os
from dataclasses import dataclass
from pathlib import Path


def get_project_dir() -> Path:
    return Path(os.path.dirname(__file__)).parent


@dataclass
class EventLogIDs:
    """
    Batches nomenclature:
     - Batch: a group of activities that are executed in a batch (e.g. A, B and C), independently of the batch type, they can even be
     executed following a type in some cases and following another type in others.
     - Batch instance: an instance of a batch grouping more than one execution of the batch (i.e. A, B and C executed in a batch in three
     traces).
     - Batch case: the batch activity instances of only one trace (e.g. A, B and C executed in a batch in trace 01).

     A 'batch' contains many 'batch instances' in an event log (all the executions of the batch activities following a batch behavior), and
     each batch instance contains at least two 'batch cases' (it is executed at least in two traces).
    """
    case: str = 'case_id'  # ID of the case instance of the process (trace)
    activity: str = 'Activity'  # Name of the executed activity in this activity instance
    start_time: str = 'start_time'  # Timestamp in which this activity instance started
    end_time: str = 'end_time'  # Timestamp in which this activity instance ended
    resource: str = 'Resource'  # ID of the resource that executed this activity instance
    enabled_time: str = 'enabled_time'  # Enable time of this activity instance
    batch_number: str = 'batch_number'
    batch_type: str = 'batch_type'
    batch_subprocess_number: str = 'batch_subprocess_number'
    batch_subprocess_type: str = 'batch_subprocess_type'
    batch_total_wt: str = 'batch_total_wt'
    batch_creation_wt: str = 'batch_creation_wt'
    batch_ready_wt: str = 'batch_ready_wt'
    batch_other_wt: str = 'batch_other_wt'


@dataclass
class Configuration:
    """Class storing the configuration parameters for the start time estimation.

    Attributes:
        log_ids                     Identifiers for each key element (e.g. executed activity or resource).
    """
    log_ids: EventLogIDs = EventLogIDs()

    PATH_PROJECT = get_project_dir()
    PATH_EXTERNAL_TOOLS = PATH_PROJECT.joinpath("external")
    PATH_BATCH_DETECTION_FOLDER = PATH_EXTERNAL_TOOLS.joinpath("batch-detection")
    PATH_BATCH_DETECTION_SCRIPT = PATH_BATCH_DETECTION_FOLDER.joinpath("batch_detection.R")
    PATH_R_EXECUTABLE = "C:/Program Files/R/R-4.1.2/bin/Rscript.exe"
