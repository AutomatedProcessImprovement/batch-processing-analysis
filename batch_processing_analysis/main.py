import pandas as pd

from batch_activation_rules import ActivationRulesDiscoverer
from batch_config import Configuration
from batch_processing_analysis import BatchProcessingAnalysis
from batch_processing_report import summarize_batch_waiting_times, print_batch_waiting_times_report


def main():
    preprocessed_log_path = "C:/Users/David Chapela/PycharmProjects/start-time-estimator/event_logs/Production.csv.gz"
    config = Configuration()
    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[config.log_ids.start_time] = pd.to_datetime(event_log[config.log_ids.start_time], utc=True)
    event_log[config.log_ids.end_time] = pd.to_datetime(event_log[config.log_ids.end_time], utc=True)
    # Run main analysis
    batch_event_log = BatchProcessingAnalysis(event_log, config).analyze_batches()
    batch_report = summarize_batch_waiting_times(batch_event_log, config.log_ids)
    print_batch_waiting_times_report(batch_report)
    # Discover activation rules
    rules = ActivationRulesDiscoverer(batch_event_log, config).get_activation_rules(config.activation_rules_type)
    for key in rules:
        if len(rules[key]) > 0:
            ruleset_str = str(
                [str(rule) for rule in rules[key]['model'].ruleset_.rules]
            ).replace(" ", "").replace(",", " V\n\t").replace("'", "").replace("^", " ^ ")
            print("\n\nBatch: {}:\n\t# Observations: {}\n\tConfidence: {:.2f}\n\tSupport: {:.2f}\n\t{}".format(
                key,
                rules[key]['num_obs'],
                round(rules[key]['confidence'], 2),
                round(rules[key]['support'], 2),
                ruleset_str
            ))
        else:
            print("\n\nBatch: {}: No rules could match the specified criterion (support >= {}).".format(key, config.min_rule_support))


if __name__ == '__main__':
    main()
