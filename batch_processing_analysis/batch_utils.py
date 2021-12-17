import pandas as pd

from batch_config import EventLogIDs


def get_batch_instance_start_time(batch_instance: pd.DataFrame, log_ids: EventLogIDs):
    """
    Get the start time of a batch instance, being this the start time of the first processed activity in this batch instance.

    :param batch_instance: activity instances of this batch instance.
    :param log_ids: dict with the attribute IDs.

    :return: the start time of this batch instance.
    """
    return batch_instance[log_ids.start_time].min()


def get_batch_instance_enabled_time(batch_instance: pd.DataFrame, log_ids: EventLogIDs):
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


def get_batch_case_start_time(batch_case: pd.DataFrame, log_ids: EventLogIDs):
    """
    Get the start time of a batch case, being this the start time of the first processed activity in this batch case.

    :param batch_case: activity instances of this batch case.
    :param log_ids: dict with the attribute IDs.

    :return: the start time of this batch case.
    """
    return batch_case[log_ids.start_time].min()


def get_batch_case_enabled_time(batch_case: pd.DataFrame, log_ids: EventLogIDs):
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
