import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytz
from config import EventLogIDs, DEFAULT_CSV_IDS, ConcurrencyOracleType, HeuristicsThresholds


def get_project_dir() -> Path:
    return Path(os.path.dirname(__file__)).parent


@dataclass
class Configuration:
    """Class storing the configuration parameters for the start time estimation.

    Attributes:
        log_ids                     Identifiers for each key element (e.g. executed activity or resource).
        concurrency_oracle_type     Concurrency oracle to use (e.g. heuristics miner's concurrency oracle).
        non_estimated_time          Time to use as value when the enabled time cannot be estimated.
        heuristics_thresholds       Thresholds for the heuristics concurrency oracle (only used is this oracle is selected as
                                    [concurrency_oracle_type].
    """
    log_ids: EventLogIDs = DEFAULT_CSV_IDS
    concurrency_oracle_type: ConcurrencyOracleType = ConcurrencyOracleType.ALPHA
    non_estimated_time: pd.Timestamp = pd.Timestamp.min.tz_localize(tz=pytz.UTC) + timedelta(seconds=1)
    heuristics_thresholds: HeuristicsThresholds = HeuristicsThresholds()

    script_path = "{}\\external\\batch-detection\\batch_detection.R".format(get_project_dir())
    r_executable = "C:/Program Files/R/R-4.1.2/bin/Rscript.exe"
