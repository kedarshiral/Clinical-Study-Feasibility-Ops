backup_select_query = "select * from $$schema$$.f_rpt_user_activity_scenario_details where created_timestamp between '$$start_time$$' and '$$end_time$$'"
# backup_select_query = "select * from $$schema$$.f_rpt_user_activity_scenario_details where last_updated_timestamp between '2022-02-07 00:00:00' and '2022-03-21 23:59:59'"
backup_delete_query = "DELETE FROM $$schema$$.f_rpt_user_activity_scenario_details where created_timestamp between '$$start_time$$' and '$$end_time$$'"
