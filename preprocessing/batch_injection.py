import math

import numpy as np
import pandas as pd
from concurrency_oracle import HeuristicsConcurrencyOracle
from config import Configuration as StartTimeConfiguration
from config import EventLogIDs as StartTimeEventLogIDs

from batch_config import EventLogIDs


def inject_batches(event_log: pd.DataFrame, activity: str, batch_size: int, resource_prefix: str, log_ids: EventLogIDs) -> pd.DataFrame:
    batched_event_log = event_log.copy()
    # Get all executions of the activity to batch
    activity_instances = batched_event_log[
        batched_event_log[log_ids.activity] == activity
        ].sort_values(by=[log_ids.start_time, log_ids.end_time, log_ids.case])
    # Group by batches based on the desired size
    num_batches = math.ceil(len(activity_instances) / batch_size)
    batch_instances = np.array_split(activity_instances, num_batches)
    # For each batch instance
    for batch_index, batch_instance in enumerate(batch_instances):
        new_start = None
        latest_end = None
        latest_duration = None
        # For each activity instance in the batch instance
        for activity_index, activity_instance in batch_instance.iterrows():
            # Calculate the time to displace the execution of the activity in order to force the batch
            current_duration = (activity_instance[log_ids.end_time] - activity_instance[log_ids.start_time])
            if not new_start:
                # First iteration of the batch instance
                new_start = batch_instance[log_ids.start_time].max()
            else:
                # Following iterations
                new_start = latest_end - (min(latest_duration, current_duration) / 4)
            difference = new_start - activity_instance[log_ids.start_time]
            latest_end = activity_instance[log_ids.end_time] + difference
            latest_duration = current_duration
            # Displace all its successors
            batched_event_log.loc[
                (batched_event_log[log_ids.case] == activity_instance[log_ids.case]) &
                (batched_event_log[log_ids.start_time] > activity_instance[log_ids.start_time]),
                [log_ids.start_time, log_ids.end_time]
            ] += difference
            # Displace the activity
            batched_event_log.loc[
                activity_index,
                [log_ids.start_time, log_ids.end_time]
            ] += difference
            # Assign the resource to the activity, and the batch ID for debugging purposes
            batched_event_log.loc[
                activity_index,
                [log_ids.resource, log_ids.batch_id]
            ] = ["{}-{}".format(resource_prefix, batch_index), "{}-{}".format(activity, batch_index)]
    # Return
    return batched_event_log


def _calculate_enabled_timed(event_log: pd.DataFrame, log_ids: EventLogIDs):
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, start_time_config)
    for (batch_key, trace) in event_log.groupby([log_ids.case]):
        trace_start_time = trace[log_ids.start_time].min()
        indexes = []
        enabled_times = []
        for index, event in trace.iterrows():
            indexes += [index]
            enabled_time = concurrency_oracle.enabled_since(trace, event)
            if enabled_time != concurrency_oracle.non_estimated_time:
                enabled_times += [enabled_time]
            else:
                enabled_times += [trace_start_time]
        event_log.loc[indexes, log_ids.enabled_time] = enabled_times


if __name__ == '__main__':
    preprocessed_log_path = "C:/Users/David Chapela/PycharmProjects/start-time-estimator/event_logs/Loan_Application.csv.gz"
    log_ids = EventLogIDs()
    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True)
    # Calc enabled times
    start_time_config = StartTimeConfiguration(
        log_ids=StartTimeEventLogIDs(
            case=log_ids.case,
            activity=log_ids.activity,
            enabled_time=log_ids.enabled_time,
            start_time=log_ids.start_time,
            end_time=log_ids.end_time,
            resource=log_ids.resource,
        ),
        consider_parallelism=True
    )
    _calculate_enabled_timed(event_log, log_ids)
    # Inject batching
    event_log[log_ids.batch_id] = np.NaN
    batched_event_log = inject_batches(event_log, " Assess loan risk", 15, "Fake Loan Officer", log_ids)
    # batched_event_log = inject_batches(
    #    batched_event_log,
    #    " Cancel application",
    #    12,
    #    ["Fake Clerk 1", "Fake Clerk 2", "Fake Clerk 3", "Fake Clerk 4", "Fake Clerk 5"],
    #    log_ids
    # )
    # batched_event_log = inject_batches(
    #    batched_event_log,
    #    " Approve application",
    #    11,
    #    ["Fake Tom 1", "Fake Tom 2", "Fake Tom 3", "Fake Tom 4", "Fake Tom 5"],
    #    log_ids
    # )
    _calculate_enabled_timed(batched_event_log, log_ids)
    batched_event_log.to_csv(preprocessed_log_path.replace(".csv.gz", "_batched.csv"), encoding='utf-8', index=False)
