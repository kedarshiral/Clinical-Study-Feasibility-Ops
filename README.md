# Clinical-Study-Feasibility-Ops
This is a project where I built a solution for our client to get them best suitable sites and investigators to conduct there clinical trials.

# CTFO Platform Service Application
CTFO is a platform service application under the “Zaidyn by ZS” umbrella. This solution helps clients track and manage their clinical trial data. The backend system extracts data from four major data sources, ingests it into the environment, and applies essential transformations such as standardization, data quality management (DQM) checks, and SCD implementation. The data then undergoes a deduplication and mastering process. Once this is complete, the cleaned and mastered data is sent to the reporting layer, where key performance indicators (KPIs) are calculated to help clients identify the most suitable sites for conducting their clinical trials.

# Contributions:
- Optimized scalable data pipelines, improving processing speed by 35%, enabling faster trial site selection and reducing time-to-market.
- Extracted multi-source data from Citeline API, AACT (ct.gov), and CTMS CSV files, ensuring seamless integration via custom Python adapters and PySpark.
- Designed and implemented Data Quality Metrics (DQM) to enforce primary key validation, duplicate checks, and data type consistency, ensuring 99% data accuracy for analytics.
- Developed a Universal Structured Layer (USL) to model data by site, study, and investigator, enhancing decision-making for clinical trials.
- Utilized Dedupe.io to eliminate redundancy and assign unique IDs, reducing duplicate records by 40%.
