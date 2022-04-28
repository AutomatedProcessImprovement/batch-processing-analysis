"""
Implementation of the artificial injection of batches to a simulated event log of a Loan Application process. This
code has been used to artificially inject batches in order to perform an experimentation with synthetic data and
prove that the technique is able to discover batches and calculated the related waiting times and activation rules.
"""

import datetime
import enum
import math
import random

import numpy as np
import pandas as pd
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle
from estimate_start_times.config import Configuration as StartTimeConfiguration
from estimate_start_times.config import EventLogIDs as StartTimeEventLogIDs
from pandas import Timedelta

from batch_processing_analysis.config import EventLogIDs


class _BatchType(enum.Enum):
    PARALLEL = 0
    SEQUENTIAL = 1
    CONCURRENT = 3


def inject_batches(
        event_log: pd.DataFrame,
        activity: str,
        batch_size: int,
        resource_prefix: str,
        log_ids: EventLogIDs,
        day_of_week: int = None,
        hour_of_day: int = None,
        wt_ready: bool = True,
        wt_other: bool = True,
        batch_types=None
) -> pd.DataFrame:
    # Set default argument if not provided
    if batch_types is None:
        batch_types = [_BatchType.CONCURRENT]
    # Copy event log to inject batches
    batched_event_log = event_log.copy()
    # Get all executions of the activity to batch
    activity_instances = batched_event_log[
        batched_event_log[log_ids.activity] == activity
        ].sort_values(by=[log_ids.start_time, log_ids.end_time, log_ids.case])
    # Group by batches based on the desired size
    num_batches = math.floor(len(activity_instances) / batch_size)
    batch_instances = np.array_split(activity_instances, num_batches)
    # For each batch instance
    count_batches = [0] * len(batch_types)
    for batch_index, batch_instance in enumerate(batch_instances):
        latest_start = None
        latest_end = None
        batch_type = random.choice(batch_types)
        # If batch type is None or the groups has a different length, skip this group
        if (batch_type is not None) and (len(batch_instance) == batch_size):
            count_batches[batch_types.index(batch_type)] += 1
            # For each activity instance in the batch instance
            for activity_index, activity_instance in batch_instance.iterrows():
                # Calculate the time to displace the execution of the activity in order to force the batch
                new_start, new_end, difference = _get_new_start_end_difference(
                    activity_instance=activity_instance,
                    batch_instance=batch_instance,
                    latest_start=latest_start,
                    latest_end=latest_end,
                    log_ids=log_ids,
                    day_of_week=day_of_week,
                    hour_of_day=hour_of_day,
                    wt_ready=wt_ready,
                    wt_other=wt_other,
                    batch_type=batch_type
                )
                # Displace all activity instances starting and ending after the current one
                batched_event_log.loc[
                    (batched_event_log[log_ids.case] == activity_instance[log_ids.case]) &
                    (batched_event_log[log_ids.start_time] >= activity_instance[log_ids.start_time]),
                    [log_ids.start_time, log_ids.end_time]
                ] += difference
                # Displace the end of all activity instances starting before the current one, but ending after it
                batched_event_log.loc[
                    (batched_event_log[log_ids.case] == activity_instance[log_ids.case]) &
                    (batched_event_log[log_ids.start_time] < activity_instance[log_ids.start_time]) &
                    (batched_event_log[log_ids.end_time] > activity_instance[log_ids.start_time]),
                    log_ids.end_time
                ] += difference
                # Displace the activity, assign the resource, and assign the batch ID for debugging purposes
                batched_event_log.loc[
                    activity_index,
                    [log_ids.start_time, log_ids.end_time, log_ids.resource, log_ids.batch_id]
                ] = [new_start, new_end, "{}-{}".format(resource_prefix, batch_index), "{}-{}".format(activity, batch_index)]
                # Update new time instants
                latest_start = new_start
                latest_end = new_end
    # Return
    print(count_batches)
    return batched_event_log


def _get_new_start_end_difference(
        activity_instance: pd.DataFrame,
        batch_instance: pd.DataFrame,
        latest_start: pd.Timestamp,
        latest_end: pd.DataFrame,
        log_ids: EventLogIDs,
        day_of_week: int = None,
        hour_of_day: int = None,
        wt_ready: bool = True,
        wt_other: bool = True,
        batch_type: _BatchType = _BatchType.CONCURRENT
) -> (pd.Timestamp, pd.Timestamp, datetime.timedelta):
    # Get duration of current activity instance
    current_duration = (activity_instance[log_ids.end_time] - activity_instance[log_ids.start_time])
    # Calculate the new start time
    if not latest_start:
        # First iteration of the batch instance
        max_enabled = batch_instance[log_ids.enabled_time].max()
        max_start = batch_instance[log_ids.start_time].max()
        if wt_ready:
            # WT_ready wanted
            if (day_of_week is not None) or (hour_of_day is not None):
                # Set as new start the latest start time
                new_start = max_start
                if hour_of_day is not None:
                    # Specific hour wanted
                    if new_start.hour != hour_of_day or new_start <= max_enabled:
                        # Displace until the next specific hour is reached
                        new_start += Timedelta(hours=(hour_of_day - new_start.hour) % 24)
                if day_of_week is not None:
                    # Specific day wanted
                    if new_start.day_of_week != day_of_week or new_start <= max_enabled:
                        # Displace until the next specific day_of_week
                        new_start += Timedelta(days=(day_of_week - new_start.day_of_week) % 7)
            elif max_start > max_enabled:
                # Not a specific day/hour and there is already some WT in the last started one
                new_start = max_start
            else:
                # Not a specific day/hour and there is no WT_ready, so force it to a quarter of the current activity duration
                new_start = max_enabled + (current_duration / 4)
        else:
            # Not WT_ready wanted, so start as soon as the batch instance is enabled
            new_start = max_enabled
    else:
        # Following iterations
        latest_duration = (latest_end - latest_start)
        if batch_type == _BatchType.SEQUENTIAL:
            new_start = latest_end
        elif batch_type == _BatchType.CONCURRENT and wt_other:
            new_start = latest_end - (min(latest_duration, current_duration) / 4)
        else:  # PARALLEL or NO WT_other
            new_start = latest_start
    # Calculate the difference between the new start and the actual start
    difference = new_start - activity_instance[log_ids.start_time]
    # Calculate the new end time
    if batch_type == _BatchType.SEQUENTIAL or batch_type == _BatchType.CONCURRENT:
        new_end = activity_instance[log_ids.end_time] + difference
    else:  # PARALLEL
        if not latest_end:
            # First iteration of the batch instance
            agg_duration = (batch_instance[log_ids.end_time] - batch_instance[log_ids.start_time]).sum()
            new_end = new_start + agg_duration
        else:
            # Following iterations
            new_end = latest_end
        # Update difference if the end changed
        difference += new_end - (activity_instance[log_ids.end_time] + difference)
    return new_start, new_end, difference


def _calculate_enabled_timed(event_log: pd.DataFrame, log_ids: EventLogIDs):
    start_time_config = StartTimeConfiguration(
        log_ids=StartTimeEventLogIDs(
            case=log_ids.case,
            activity=log_ids.activity,
            enabled_time=log_ids.enabled_time,
            start_time=log_ids.start_time,
            end_time=log_ids.end_time,
            resource=log_ids.resource,
        ),
        consider_start_times=True
    )
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


def _main_batch_injection(preprocessed_log_path: str, output_log_path: str):
    log_ids = EventLogIDs()
    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True)
    # Calc enabled times
    _calculate_enabled_timed(event_log, log_ids)
    # Inject batching
    event_log[log_ids.batch_id] = np.NaN
    batched_event_log = inject_batches(
        event_log=event_log,
        activity=" Assess loan risk",
        batch_size=14,
        resource_prefix="Fake Loan Officer",
        log_ids=log_ids,
        day_of_week=0,
        wt_ready=True,
        wt_other=True,
        batch_types=[_BatchType.PARALLEL, _BatchType.SEQUENTIAL, _BatchType.CONCURRENT, None]
    )
    _calculate_enabled_timed(batched_event_log, log_ids)
    batched_event_log = inject_batches(
        event_log=batched_event_log,
        activity=" Cancel application",
        batch_size=10,
        resource_prefix="Fake Clerk",
        log_ids=log_ids,
        day_of_week=4,
        hour_of_day=10,
        wt_ready=True,
        wt_other=False,
        batch_types=[_BatchType.PARALLEL, _BatchType.CONCURRENT, None]
    )
    _calculate_enabled_timed(batched_event_log, log_ids)
    batched_event_log = inject_batches(
        event_log=batched_event_log,
        activity=" Approve application",
        batch_size=12,
        resource_prefix="Fake Tom",
        log_ids=log_ids,
        wt_ready=False,
        wt_other=True,
        batch_types=[_BatchType.SEQUENTIAL, _BatchType.CONCURRENT, None]
    )
    _calculate_enabled_timed(batched_event_log, log_ids)
    batched_event_log.to_csv(output_log_path, encoding='utf-8', index=False, compression='gzip')


if __name__ == '__main__':
    input_path = "path_to_original_log"
    output_path = "path_to_batched_log"
    _main_batch_injection(input_path, output_path)
