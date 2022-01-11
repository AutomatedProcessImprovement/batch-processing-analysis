from datetime import timedelta

import pandas as pd
from numpy import mean

from batch_activation_rules import ActivationRulesDiscoverer
from batch_config import Configuration, BatchType
from batch_processing_analysis import BatchProcessingAnalysis
from batch_processing_report import summarize_batch_waiting_times


def main():
    preprocessed_log_path = "C:/Users/David Chapela/PycharmProjects/start-time-estimator/event_logs/ConsultaDataMining201618.csv.gz"
    config = Configuration()
    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[config.log_ids.start_time] = pd.to_datetime(event_log[config.log_ids.start_time], utc=True)
    event_log[config.log_ids.end_time] = pd.to_datetime(event_log[config.log_ids.end_time], utc=True)
    # Run main analysis
    batch_event_log = BatchProcessingAnalysis(event_log, config).analyze_batches()
    batch_report = summarize_batch_waiting_times(batch_event_log, config.log_ids)
    for batch_activities in batch_report:
        print("\n\nBatch formed by activities: {}".format(batch_activities))
        print("\tNumber of occurrences: {}".format(batch_report[batch_activities]['total_occurrences']))
        print("\tNum executed in batch: {}%".format(batch_report[batch_activities]['batched_total_occurrences']))
        print("\tFrequency executed in batch: {:.2f}%".format(round(batch_report[batch_activities]['batched_freq_occurrence'] * 100), 2))
        for batch_type in [BatchType.parallel,
                           BatchType.task_sequential,
                           BatchType.task_concurrent,
                           BatchType.case_sequential,
                           BatchType.case_concurrent]:
            batch_stats = batch_report[batch_activities][batch_type]
            if batch_stats['num_instances'] > 0:
                print("\t- Batch type: {}".format(batch_type))
                print("\t\tNum batch instances: {}".format(batch_stats['num_instances']))
                print("\t\tNum batch cases: {}".format(batch_stats['num_cases']))
                print("\t\tFrequency: {:.2f}%".format(round(batch_stats['freq_occurrence'] * 100), 2))
                print("\t\tAverage overall processing time: {} sec".format(mean(batch_stats['processing_time'])))
                print("\t\tAverage overall waiting time: {} sec".format(mean(batch_stats['waiting_time'])))
                print("\t\tCTE: {}".format(cte(batch_stats['processing_time'], batch_stats['waiting_time'])))
                print("\t\tAverage total wt: {} sec".format(mean(batch_stats['total_wt'])))
                print("\t\tAverage creation wt: {} sec".format(mean(batch_stats['creation_wt'])))
                print("\t\tAverage ready wt: {} sec".format(mean(batch_stats['ready_wt'])))
                print("\t\tAverage other wt: {} sec".format(mean(batch_stats['other_wt'])))
    # Discover activation rules
    rules = ActivationRulesDiscoverer(batch_event_log, config).get_activation_rules()
    for key in rules:
        ruleset_str = str(
            [str(rule) for rule in rules[key].ruleset_.rules]
        ).replace(" ", "").replace(",", " V\n\t").replace("'", "").replace("^", " ^ ")
        print("\n\nBatch: {}:\n\t{}".format(key, ruleset_str))


def cte(processing_times: list, waiting_times: list):
    if sum(processing_times, timedelta(0)) > timedelta(0):
        value = sum(processing_times, timedelta(0)) / (sum(processing_times, timedelta(0)) + sum(waiting_times, timedelta(0)))
    else:
        value = 0
    return value


if __name__ == '__main__':
    main()
