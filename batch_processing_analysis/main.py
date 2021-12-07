import subprocess

from configu import get_project_dir


def main():
    preprocessed_log_path = "C:/Users/David Chapela/PycharmProjects/start-time-estimator/event_logs/ConsultaDataMining201618.csv.gz"
    batched_log_path = "C:/Users/David Chapela/PycharmProjects/batch-processing-analysis/external/batch-detection/output.csv"
    script_path = '{}\\external\\batch-detection\\batch_detection.R'.format(get_project_dir())
    subprocess.call(
        [
            'C:/Program Files/R/R-4.1.2/bin/Rscript.exe',
            script_path,
            preprocessed_log_path,
            batched_log_path,
            "yyyy-mm-dd hh:mm:ss"
        ],
        shell=True
    )


if __name__ == '__main__':
    main()
