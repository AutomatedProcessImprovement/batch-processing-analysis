import pandas as pd

from batch_config import Configuration
from batch_processing_analysis import analyze_batches


def main():
    preprocessed_log_path = "C:/Users/David Chapela/PycharmProjects/start-time-estimator/event_logs/ConsultaDataMining201618.csv.gz"
    config = Configuration()
    # Read and preprocess event log
    event_log = pd.read_csv(preprocessed_log_path)
    event_log[config.log_ids.start_timestamp] = pd.to_datetime(event_log[config.log_ids.start_timestamp], utc=True)
    event_log[config.log_ids.end_timestamp] = pd.to_datetime(event_log[config.log_ids.end_timestamp], utc=True)
    # Run main analysis
    analyze_batches(event_log, config)
    print()


if __name__ == '__main__':
    main()
