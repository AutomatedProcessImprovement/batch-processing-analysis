import datetime
import os

import pandas as pd
from estimate_start_times.concurrency_oracle import HeuristicsConcurrencyOracle
from estimate_start_times.config import Configuration

from config import EventLogIDs
from preprocessing.batch_injection import _calculate_enabled_timed, inject_batches, _BatchType


def _batch_injection(log: str):
    _inject_batch_handoff(
        input_path="handoff-logs/{}.CSV".format(log),
        no_wt_log_path="handoff-logs/LoanApp_infRes_24-7_noTimers.CSV",
        output_path="handoff-logs/{}_batch.CSV".format(log),
        activity="Assess loan risk"
    )


def _inject_batch_handoff(input_path: str, no_wt_log_path: str, output_path: str, activity: str):
    # Read event log with no WTs
    log_ids = EventLogIDs()
    no_wt_event_log = pd.read_csv(no_wt_log_path)
    no_wt_event_log[log_ids.start_time] = pd.to_datetime(no_wt_event_log[log_ids.start_time], utc=True)
    no_wt_event_log[log_ids.end_time] = pd.to_datetime(no_wt_event_log[log_ids.end_time], utc=True)
    no_wt_event_log[log_ids.resource] = no_wt_event_log[log_ids.resource].astype(str)
    # Take traces containing target activity
    batched_sublog = _get_traces_containing(0.4, no_wt_event_log, activity, log_ids)
    # Compute enabled times
    _calculate_enabled_timed(batched_sublog, log_ids)
    # Replace all resources with new one
    batched_sublog[log_ids.resource] = "Fake_Resource"
    # Use existent code to add batch
    batched_sublog = inject_batches(
        event_log=batched_sublog,
        activity=activity,
        batch_size=12,
        resource_prefix="Batch_Resource",
        log_ids=log_ids,
        wt_ready=False,
        wt_other=False,  # if there are WT_other is going to be resource contention
        batch_types=[_BatchType.PARALLEL]
    )
    # Read log to add batches
    event_log = pd.read_csv(input_path)
    event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True)
    event_log[log_ids.resource] = event_log[log_ids.resource].astype(str)
    # Align traces to the end of the log
    offset = event_log[log_ids.end_time].max() - batched_sublog[log_ids.start_time].min() + datetime.timedelta(hours=1)
    batched_sublog[log_ids.enabled_time] = batched_sublog[log_ids.enabled_time] + offset
    batched_sublog[log_ids.start_time] = batched_sublog[log_ids.start_time] + offset
    batched_sublog[log_ids.end_time] = batched_sublog[log_ids.end_time] + offset
    # Update case ID
    max_case_id = event_log[log_ids.case].max() + 1
    batched_sublog[log_ids.case] = batched_sublog[log_ids.case] + max_case_id
    # Inject batches
    batched_event_log = event_log.append(batched_sublog)
    # Remove enabled time calculation
    batched_event_log.drop([log_ids.enabled_time], axis=1, inplace=True)
    # Write log to output
    batched_event_log.to_csv(output_path, encoding="utf-8", index=False)


def _get_traces_containing(percentage: float, event_log: pd.DataFrame, activity: str, log_ids: EventLogIDs):
    case_ids = []
    # Search the IDs of those cases containing the activity
    for case_id, events in event_log.groupby(log_ids.case):
        if activity in events[log_ids.activity].values:
            case_ids += [case_id]
    # Return events of % traces containing the activity
    case_ids = case_ids[0:round(len(case_ids) * percentage)]
    return event_log[event_log[log_ids.case].isin(case_ids)].copy()


def _log_filtering(log: str):
    # Read event log
    log_ids = EventLogIDs()
    log_path = "handoff-logs/{}.CSV".format(log)
    event_log = pd.read_csv(log_path)
    # Remove events and trip activity names
    event_log[log_ids.activity] = event_log[log_ids.activity].apply(lambda x: x.strip().replace("\xa0", "").replace("  ", " "))
    event_log = event_log[~event_log[log_ids.activity].isin([
        "Loan application approved",
        "Loan application canceled",
        "Loan application rejected",
        "Loan application received",
        "EVENT 33 CATCH TIMER",
        "EVENT 34 CATCH TIMER",
        "EVENT 35 CATCH TIMER",
        "EVENT 36 CATCH TIMER",
        "EVENT 37 CATCH TIMER"
    ])]
    # Remove existent log
    os.remove(log_path)
    # Write filtered log
    event_log.to_csv(log_path, encoding="utf-8", index=False)


def _displace_calendar_unavailability(log: str):
    # Read event log
    log_ids = EventLogIDs()
    log_path = "handoff-logs/{}.CSV".format(log)
    event_log = pd.read_csv(log_path)
    event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True)
    event_log[log_ids.resource] = event_log[log_ids.resource].astype(str)
    # Identify events starting/ending where Loan Officer is not working and displace them until the resource is working
    event_log.loc[
        (event_log[log_ids.resource].str.contains("Loan Officer")) & (event_log[log_ids.start_time].dt.day_of_week > 2),
        log_ids.start_time
    ] = event_log[
        (event_log[log_ids.resource].str.contains("Loan Officer")) & (event_log[log_ids.start_time].dt.day_of_week > 2)
        ][log_ids.start_time].apply(lambda x: x + datetime.timedelta(
        days=(6 - x.day_of_week),  # Move forward to next sunday
        hours=(8 - x.hour) % 24,  # Move forward to next 8 AM (current hour is always >9AM)
        minutes=(59 - x.minute),  # Move forward to next 59
        seconds=(59 - x.second),  # Move forward to next 00
        microseconds=(1000000 - x.microsecond)  # Move forward to next completed second
    )
                                    )
    # Identify events starting/ending where Senior Officer is not working and displace them until the resource is working
    event_log.loc[
        (event_log[log_ids.resource].str.contains("Senior Officer")) & (event_log[log_ids.start_time].dt.day_of_week < 3),
        log_ids.start_time
    ] = event_log[
        (event_log[log_ids.resource].str.contains("Senior Officer")) & (event_log[log_ids.start_time].dt.day_of_week < 3)
        ][log_ids.start_time].apply(lambda x: x + datetime.timedelta(
        days=(2 - x.day_of_week),  # Move forward to next wednesday
        hours=(8 - x.hour) % 24,  # Move forward to next 8 AM (current hour is always >9AM)
        minutes=(59 - x.minute),  # Move forward to next 59
        seconds=(59 - x.second),  # Move forward to next 00
        microseconds=(1000000 - x.microsecond)  # Move forward to next completed second
    )
                                    )
    # Remove existent log
    os.remove(log_path)
    # Write filtered log
    event_log.to_csv(log_path, encoding="utf-8", index=False)


def _prioritization_injection(log: str):
    _inject_prioritized_handoff(
        log_path="handoff-logs/{}.CSV".format(log),
        base_log_path="handoff-logs/LoanApp_only_ONE_contention.csv",
        output_path="handoff-logs/{}_prior.CSV".format(log),
        activity="Assess loan risk"
    )


def _inject_prioritized_handoff(log_path: str, base_log_path: str, output_path: str, activity: str):
    # Read base event log
    log_ids = EventLogIDs()
    base_log = pd.read_csv(base_log_path)
    base_log[log_ids.start_time] = pd.to_datetime(base_log[log_ids.start_time], utc=True)
    base_log[log_ids.end_time] = pd.to_datetime(base_log[log_ids.end_time], utc=True)
    config = Configuration()
    concurrency_oracle = HeuristicsConcurrencyOracle(base_log, config)
    concurrency_oracle.add_enabled_times(base_log)
    # Get activities that can be prioritized
    to_prioritize = []
    for index, event in base_log[
        (base_log[log_ids.activity] == activity) & (base_log[log_ids.enabled_time] == base_log[log_ids.start_time])
    ].iterrows():
        potential_prioritization = base_log[
            (base_log[log_ids.resource] == event[log_ids.resource]) &
            (base_log[log_ids.enabled_time] > event[log_ids.enabled_time]) &
            (base_log[log_ids.enabled_time] < event[log_ids.end_time]) &
            (base_log[log_ids.start_time] == event[log_ids.end_time])
            ]
        if len(potential_prioritization) > 0:
            to_prioritize += [(event, potential_prioritization.iloc[0])]
    # Read event log
    log_ids = EventLogIDs()
    event_log = pd.read_csv(log_path)
    event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True)
    # Copy traces and force prioritization
    for i in range(len(to_prioritize)):
        event_1, event_2 = to_prioritize[i]  # event_1 is the one executed before event_2
        # Clone the two traces corresponding to the events
        trace_1 = base_log[base_log[log_ids.case] == event_1[log_ids.case]].drop([log_ids.enabled_time], axis=1).copy()
        trace_2 = base_log[base_log[log_ids.case] == event_2[log_ids.case]].drop([log_ids.enabled_time], axis=1).copy()
        # Set new resources and case ID
        max_case_id = event_log[log_ids.case].max() + 1
        trace_1[log_ids.case] = max_case_id
        trace_1[log_ids.resource] = trace_1[log_ids.resource].astype(str) + "_prior_{}".format(i)
        trace_2[log_ids.case] = max_case_id + 1
        trace_2[log_ids.resource] = trace_2[log_ids.resource].astype(str) + "_prior_{}".format(i)
        # Move earlier the start of the second activity instance to be executed first
        trace_2.loc[
            (trace_2[log_ids.start_time] == event_2[log_ids.start_time]) &
            (trace_2[log_ids.end_time] == event_2[log_ids.end_time]) &
            (trace_2[log_ids.activity] == event_2[log_ids.activity]),
            log_ids.start_time
        ] = event_2[log_ids.enabled_time]
        # Displace to later the first activity to be executed after the prioritized one
        difference = event_2[log_ids.end_time] - event_1[log_ids.start_time]
        trace_1.loc[
            trace_1[log_ids.start_time] >= event_1[log_ids.start_time],
            log_ids.start_time
        ] += difference
        trace_1.loc[
            trace_1[log_ids.end_time] > event_1[log_ids.start_time],
            log_ids.end_time
        ] += difference
        trace_1.loc[  # Displace enabled time (end time of enabling activity) of this event to 1 ms before the other event enabled time
            trace_1[log_ids.end_time] == event_1[log_ids.enabled_time],
            log_ids.end_time
        ] += (event_2[log_ids.enabled_time] - event_1[log_ids.enabled_time]) - datetime.timedelta(seconds=1)
        # Add traces to the event log
        event_log = pd.concat([event_log, trace_1, trace_2], ignore_index=True)
    # Write log to output
    event_log.to_csv(output_path, encoding="utf-8", index=False)


def _check_priorit(log: str):
    # Read base event log
    log_ids = EventLogIDs()
    event_log = pd.read_csv("handoff-logs/{}_prior.CSV".format(log))
    event_log[log_ids.start_time] = pd.to_datetime(event_log[log_ids.start_time], utc=True)
    event_log[log_ids.end_time] = pd.to_datetime(event_log[log_ids.end_time], utc=True)
    config = Configuration()
    concurrency_oracle = HeuristicsConcurrencyOracle(event_log, config)
    print(concurrency_oracle.concurrency)
    concurrency_oracle.add_enabled_times(event_log)
    # Check WTs
    for index, event in event_log[event_log[log_ids.start_time] > event_log[log_ids.enabled_time]].iterrows():
        other = event_log[
            (event_log[log_ids.resource] == event[log_ids.resource]) &
            (event_log[log_ids.end_time] == event[log_ids.start_time])
            ].iloc[0]
        if (other[log_ids.start_time] - event[log_ids.enabled_time]) != datetime.timedelta(seconds=1):
            print("POTATO")
        if event[log_ids.start_time] > event[log_ids.enabled_time]:
            print(event[log_ids.activity])


if __name__ == '__main__':
    datasets = [
        "LoanApp_fewRes_9-5_noTimers",
        "LoanApp_fewRes_9-5_timers",
        "LoanApp_fewRes_24-7_noTimers",
        "LoanApp_fewRes_24-7_timers",
        "LoanApp_infRes_9-5_noTimers",
        "LoanApp_infRes_9-5_timers",
        "LoanApp_infRes_24-7_noTimers",
        "LoanApp_infRes_24-7_timers"
    ]
    for dataset in datasets:
        # _log_filtering(dataset)
        # _displace_calendar_unavailability(dataset)
        # _prioritization_injection(dataset)
        # _batch_injection(dataset)
        _check_priorit(dataset)
