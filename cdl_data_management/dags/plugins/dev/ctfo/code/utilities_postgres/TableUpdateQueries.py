update_status_query = "UPDATE $$schema_name$$.log_model_run_status SET $$params$$"\
",processing_job_id='$$processing_job_id$$' WHERE model_run_id = %(model_run_id)s "\
" AND theme_id= %(theme_id)s AND  scenario_id= %(scenario_id)s AND created_by=%(created_by)s"


update_validation_status_query= "UPDATE $$schema_name$$.log_dqm_smry  SET  null_validation = '$$null_validation$$', data_type_validation = '$$data_type_validation$$', validation_rules = '$$validation_rules$$', s3_err_location = '$$s3_err_location$$', status = '$$status$$', created_timestamp = '$$created_timestamp$$', updated_timestamp = '$$updated_timestamp$$', delete_flag = '$$delete_flag$$', s3_smry_location = '$$s3_smry_location$$' WHERE user_id='$$user_id$$' AND validation_id='$$validation_id$$'"

check_id_exists_query= "select count(1) from   $$schema_name$$.log_dqm_smry WHERE validation_id = '$$validation_id$$'"

insert_validation_status_query= "INSERT INTO $$schema_name$$.log_dqm_smry  values ('$$validation_id$$', '$$null_validation$$', '$$data_type_validation$$','$$validation_rules$$','$$user_id$$','$$status$$','$$created_timestamp$$','$$updated_timestamp$$','$$s3_err_location$$', '$$s3_smry_location$$', '$$delete_flag$$')"
