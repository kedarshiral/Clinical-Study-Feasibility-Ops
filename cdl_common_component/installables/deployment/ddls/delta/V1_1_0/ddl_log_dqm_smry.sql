create table log_dqm_smry(
application_id         varchar(20)  ,
dataset_id             int(11)      ,
batch_id               bigint       ,
file_id                int(11)      ,
qc_id                  int(11)      ,
column_name            varchar(500) ,
qc_type                varchar(50)  ,
qc_param               varchar(1000),
criticality            varchar(20)  ,
error_count            int(11)      ,
qc_message             varchar(100) ,
qc_failure_percentage  varchar(20)  ,
qc_status              varchar(20)  ,
qc_start_time          timestamp    ,
qc_end_time            timestamp    ,
qc_create_by           varchar(20)  ,
qc_create_date         timestamp
);
