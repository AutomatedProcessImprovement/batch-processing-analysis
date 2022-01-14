import enum
import os
from dataclasses import dataclass
from pathlib import Path


def get_project_dir() -> Path:
    return Path(os.path.dirname(__file__)).parent


class ActivationRulesMode(enum.Enum):
    PER_ACTIVITY = 0
    PER_BATCH = 1
    PER_BATCH_TYPE = 2


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
    batch_id: str = 'batch_instance_id'  # ID of the batch instance this activity instance belongs to, if any
    batch_type: str = 'batch_instance_type'  # Type of the batch instance this activity instance belongs to, if any
    batch_pt: str = 'batch_pt'  # Batch case processing time: time in which there is an activity of the batch case being processed
    batch_wt: str = 'batch_wt'  # Batch case waiting time: time in which there are no activities of the batch case being processed
    batch_total_wt: str = 'batch_total_wt'  # Batch case waiting time: time since the batch case enablement until its start
    batch_creation_wt: str = 'batch_creation_wt'  # Batch case creation wt: time since batch case enablement until batch instance creation
    batch_ready_wt: str = 'batch_ready_wt'  # Batch instance ready waiting time: time since the batch instance is created until its start
    batch_other_wt: str = 'batch_other_wt'  # Batch case other waiting time: time since the batch instance start until the batch case start


@dataclass
class BatchType:
    parallel: str = "Parallel"
    task_sequential: str = "Sequential task-based"
    task_concurrent: str = "Concurrent task-based"
    case_sequential: str = "Sequential case-based"
    case_concurrent: str = "Concurrent case-based"


@dataclass
class Configuration:
    """Class storing the configuration parameters for the start time estimation.

    Attributes:
        log_ids                             Identifiers for each key element (e.g. executed activity or resource).
        num_batch_ready_negative_events     Number of non-activating events to generate in the batch-ready interval to extract the batch
                                            activation rules.
        num_batch_ready_negative_events     Max number of non-activating events to generate in the batch cases enablement instants
                                            to extract the batch activation rules.
        activation_rules_type               Type of grouping to find the activation rules. For example, 'PER_BATCH' groups all batches of
                                            the same activities, independently of the batch type, and discover the rules for them.
        max_rules                           Maximum number of activation rules to extract from a batch.
        min_rule_support                    Minimum individual support for the discovered activation rules.
        min_batch_instance_size             Minimum size to analyze a batch instance, being the size its number of batch cases.
        batch_discovery_subsequence_type    Method to extract the subsequences in the batch discovery: "all" for considering all
                                            subsequences, "freq" to use only frequent subsequences.
    """
    log_ids: EventLogIDs = EventLogIDs()
    num_batch_ready_negative_events: int = 1
    num_batch_enabled_negative_events: int = 1
    activation_rules_type: ActivationRulesMode = ActivationRulesMode.PER_BATCH
    max_rules: int = 3
    min_rule_support: float = 0.1
    min_batch_instance_size: int = 2
    batch_discovery_subsequence_type: str = "all"

    PATH_PROJECT = get_project_dir()
    PATH_EXTERNAL_TOOLS = PATH_PROJECT.joinpath("external")
    PATH_BATCH_DETECTION_FOLDER = PATH_EXTERNAL_TOOLS.joinpath("batch-detection")
    PATH_BATCH_DETECTION_SCRIPT = PATH_BATCH_DETECTION_FOLDER.joinpath("batch_detection.R")
    PATH_R_EXECUTABLE = "C:/Program Files/R/R-4.1.2/bin/Rscript.exe"
