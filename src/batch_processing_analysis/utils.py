from datetime import timedelta

import pandas as pd
from pandas import Timestamp

from .config import EventLogIDs


def get_batch_activities(batch_events: pd.DataFrame, log_ids: EventLogIDs) -> tuple:
    """
    Get the set of activities of the batch.

    :param batch_events: activity instances of this batch.
    :param log_ids: dict with the attribute IDs.

    :return: tuple with the activities forming the batch.
    """
    return tuple(
        sorted(
            batch_events[log_ids.activity].unique()
        )
    )


def get_batch_instance_start_time(batch_instance: pd.DataFrame, log_ids: EventLogIDs) -> Timestamp:
    """
    Get the start time of a batch instance, being this the start time of the first processed activity in this batch instance.

    :param batch_instance: activity instances of this batch instance.
    :param log_ids: dict with the attribute IDs.

    :return: the start time of this batch instance.
    """
    return batch_instance[log_ids.start_time].min()


def get_batch_instance_end_time(batch_instance: pd.DataFrame, log_ids: EventLogIDs) -> Timestamp:
    """
    Get the end time of a batch instance, being this the end time of the last processed activity in this batch instance.

    :param batch_instance: activity instances of this batch instance.
    :param log_ids: dict with the attribute IDs.

    :return: the end time of this batch instance.
    """
    return batch_instance[log_ids.end_time].max()


def get_batch_instance_enabled_time(batch_instance: pd.DataFrame, log_ids: EventLogIDs) -> Timestamp:
    """
    Get the enabled time of the batch instance, being this the last of the enable times of its batch cases.

    :param batch_instance: activity instances of this batch instance.
    :param log_ids: dict with the attribute IDs.

    :return: the enabled time of this batch instance.
    """
    enabled_first_processed = []
    for (key, batch_case) in batch_instance.groupby([log_ids.case]):
        enabled_first_processed += [get_batch_case_enabled_time(batch_case, log_ids)]
    return max(enabled_first_processed)


def get_batch_case_start_time(batch_case: pd.DataFrame, log_ids: EventLogIDs) -> Timestamp:
    """
    Get the start time of a batch case, being this the start time of the first processed activity in this batch case.

    :param batch_case: activity instances of this batch case.
    :param log_ids: dict with the attribute IDs.

    :return: the start time of this batch case.
    """
    return batch_case[log_ids.start_time].min()


def get_batch_case_end_time(batch_case: pd.DataFrame, log_ids: EventLogIDs) -> Timestamp:
    """
    Get the end time of a batch case, being this the end time of the last processed activity in this batch case.

    :param batch_case: activity instances of this batch case.
    :param log_ids: dict with the attribute IDs.

    :return: the end time of this batch case.
    """
    return batch_case[log_ids.end_time].max()


def get_batch_case_enabled_time(batch_case: pd.DataFrame, log_ids: EventLogIDs) -> Timestamp:
    """
    Get the enabled time of a batch case, being this the enabled time of the first processed activity instance in the batch case.

    :param batch_case: activity instances of a batch case.
    :param log_ids: dict with the attribute IDs.

    :return: the enabled time of this batch case.
    """
    first_start_time = get_batch_case_start_time(batch_case, log_ids)
    return batch_case.loc[
        batch_case[log_ids.start_time] == first_start_time,
        log_ids.enabled_time
    ].min()


def get_naive_batch_case_processing_waiting_times(batch_case: pd.DataFrame, log_ids: EventLogIDs) -> (timedelta, timedelta):
    """
    Get the processing and waiting times of a batch case with a naive algorithm. Given the activity instances of a batch case, the
    processing time is calculated as the time from the start of the first activity to the end of the last one, including intra-batch waiting
    times; the waiting time is calculated as the time waited due to the batch processing, i.e., the time since the first enablement to the
    first start.

    :param batch_case: activity instances of the batch case.
    :param log_ids: dict with the attribute IDs.

    :return: a tuple with the processing and waiting times, respectively, of the batch case.
    """
    batch_case_enabled_time = get_batch_case_enabled_time(batch_case, log_ids)
    batch_case_start_time = get_batch_case_start_time(batch_case, log_ids)
    batch_case_end_time = get_batch_case_end_time(batch_case, log_ids)
    return (batch_case_end_time - batch_case_start_time), (batch_case_start_time - batch_case_enabled_time)


def get_batch_case_processing_waiting_times(batch_case: pd.DataFrame, log_ids: EventLogIDs) -> (timedelta, timedelta):
    """
    Get the processing and waiting times of a batch case. Given the activity instances of a batch case, the processing time is the time
    interval from the first enablement until the last end in which an activity was being processed; the waiting time is the time interval in
    which there were enabled activities but none being processed.

    :param batch_case: activity instances of the batch case.
    :param log_ids: dict with the attribute IDs.

    :return: a tuple with the processing and waiting times, respectively, of the batch case.
    """
    # Merge enabled, start and end events
    enabled_times = batch_case[log_ids.enabled_time].to_frame().rename(columns={log_ids.enabled_time: 'time'})
    enabled_times['lifecycle'] = '1-enabled'
    start_times = batch_case[log_ids.start_time].to_frame().rename(columns={log_ids.start_time: 'time'})
    start_times['lifecycle'] = '2-start'
    end_times = batch_case[log_ids.end_time].to_frame().rename(columns={log_ids.end_time: 'time'})
    end_times['lifecycle'] = '3-end'
    times = enabled_times.append(start_times).append(end_times).sort_values(['time', 'lifecycle'])
    # Loop times measuring processing and waiting intervals
    enabled = 0
    processing = 0
    waiting_time = timedelta(0)
    processing_time = timedelta(0)
    for time, lifecycle in times.itertuples(index=False):
        if lifecycle == '1-enabled':
            # New activity enabled
            if enabled == 0 and processing == 0:
                # Currently not processing and first enabled, so:
                start_wt = time  # waiting time starts.
            enabled += 1
        elif lifecycle == '2-start':
            # Activity starts its processing
            if processing == 0:
                # Currently not processing, just waiting with enabled activity/ies, so:
                waiting_time += time - start_wt  # waiting time ends, record it, and
                start_pt = time  # processing time starts.
            enabled -= 1
            processing += 1
        elif lifecycle == '3-end':
            # Activity ends its processing
            if processing == 1:
                # The only one being processed, so:
                processing_time += time - start_pt  # processing time ends, record it, and
                if enabled > 0:
                    # There are enabled activities, so:
                    start_wt = time  # waiting time starts.
            processing -= 1
    # Return processing and waiting times
    return processing_time, waiting_time


def get_workload(event_log: pd.DataFrame, resource: str, instant: Timestamp, log_ids: EventLogIDs) -> int:
    """
    Get the number of cases in which [resource] is working, or assigned, at the instant [instant].

    :param event_log: the event log to search on.
    :param resource: the resource to calculate the workload.
    :param instant: the instant in which to calculate the workload.
    :param log_ids: dict with the attribute IDs.

    :return: the number of cases in which the resource is assigned to, at least, an activity.
    """
    return len(
        event_log[
            (event_log[log_ids.resource] == resource) &
            (event_log[log_ids.enabled_time] <= instant) &
            (instant <= event_log[log_ids.end_time])
            ][log_ids.case].unique()
    )


def get_batch_activities_number_executions(event_log: pd.DataFrame, batch: pd.DataFrame, log_ids: EventLogIDs) -> int:
    """
    Get the number of times the activities of a batch are executed in the log.

    :param event_log: event log to search the number of executions.
    :param batch: activity instances of the batch to get the activities.
    :param log_ids: dict with the attribute IDs.

    :return: the number of times the sequence of activities in the batch were executed in the event log.
    """
    activities = list(
        batch[batch[log_ids.case] == batch[log_ids.case].iloc[0]].sort_values([log_ids.start_time, log_ids.end_time])[log_ids.activity]
    )
    # Get number of occurrences
    if len(activities) == 1:
        # Single activity, just count
        activity = activities[0]
        num_executions = len(event_log[event_log[log_ids.activity] == activity])
    else:
        # Subprocess, search occurrences of sequence
        num_executions = 0
        for (key, case) in event_log.groupby([log_ids.case]):
            num_executions += sum(
                [
                    list(activity_sequence.values) == activities
                    for activity_sequence
                    in case.sort_values([log_ids.start_time, log_ids.end_time])[log_ids.activity].rolling(len(activities))
                ]
            )
    # Return num occurrences
    return num_executions
