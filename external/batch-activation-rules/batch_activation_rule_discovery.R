

# Dependencies
# install.packages('reshape')
# install.packages('C50')
# install.packages('RWeka')

library(readr)


# Load functions
source("./batch_activation_rule_retrieval.r")

# Read CSV
event_log <- read_csv("./event_log.csv.gz")
names(event_log) <- c("case_id", "activity", "arrival", "start", "complete", "resource", "batch_number")
event_log[["resource"]][is.na(event_log[["resource"]])] <- "NOT_SET"

# Load features table
feature_table <- create_feature_table(event_log)

# Learn decision rules
decision_rules <- learn_decision_rules(feature_table)
