ALTER TABLE ctl_process_dependency_details MODIFY column dependency_value bigint(40);
ALTER TABLE ctl_process_dependency_details MODIFY column override_dependency_value bigint(40);
ALTER TABLE ctl_process_dependency_details MODIFY column write_dependency_value bigint(40);