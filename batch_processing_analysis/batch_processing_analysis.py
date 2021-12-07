import pandas as pd
from concurrency_oracle import NoConcurrencyOracle, AlphaConcurrencyOracle, HeuristicsConcurrencyOracle
from config import ConcurrencyOracleType

from batch_processing_discovery import discover_batches_martins21
from config import Configuration


class BatchProcessingAnalysis:
    def __init__(self, event_log: pd.DataFrame, config: Configuration):
        # Set event log
        self.event_log = event_log
        # Set configuration
        self.config = config
        # Set concurrency oracle
        if self.config.concurrency_oracle_type == ConcurrencyOracleType.NONE:
            self.concurrency_oracle = NoConcurrencyOracle(self.event_log, self.config)
        elif self.config.concurrency_oracle_type == ConcurrencyOracleType.ALPHA:
            self.concurrency_oracle = AlphaConcurrencyOracle(self.event_log, self.config)
        elif self.config.concurrency_oracle_type == ConcurrencyOracleType.HEURISTICS:
            self.concurrency_oracle = HeuristicsConcurrencyOracle(self.event_log, self.config)
        else:
            raise ValueError("No concurrency oracle defined!")

    def analyze_batches(self):
        # Discover batches
        batched_event_log = discover_batches_martins21(self.event_log, self.config)
        # Discover enablement times per activity
        for (key, trace) in batched_event_log.groupby([self.config.log_ids.case]):
            for index, event in trace.iterrows():
                batched_event_log.loc[index, self.config.log_ids.enabled_time] = self.concurrency_oracle.enabled_since(trace, event)
        # Calculate batching waiting times

        # Discover activation rules
        pass
