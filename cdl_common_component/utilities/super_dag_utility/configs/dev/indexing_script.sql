-- Drop all INDEXES
DROP INDEX ctfo_internal_app_fa_dev.index_filter_country;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_trial_id_1;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_site_id;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_inv_id;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_trial_site_id;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_trial_site_inv_id;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_trial_inv_id;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_country_usa;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_country_code;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_region;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_trial_id_2;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_ta;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_dis;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_pat_seg;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_nct;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_phase;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_protocol;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_title;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_ta_dis_phase;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_ta_onc;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_trial_id_3;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_sponsor;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_drug;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_trial_status;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_drug_status;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_cas;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_gender;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_age;
DROP INDEX ctfo_internal_app_fa_dev.index_filter_mesh;
DROP INDEX ctfo_internal_app_fa_dev.index_site_study_trial_id;
DROP INDEX ctfo_internal_app_fa_dev.index_site_study_site_id;
DROP INDEX ctfo_internal_app_fa_dev.index_site_study_trial_site_id;
DROP INDEX ctfo_internal_app_fa_dev.index_site_study_trial_status;
DROP INDEX ctfo_internal_app_fa_dev.index_inv_site_study_trial_id;
DROP INDEX ctfo_internal_app_fa_dev.index_inv_site_study_site_id;
DROP INDEX ctfo_internal_app_fa_dev.index_inv_site_study_inv_id;
DROP INDEX ctfo_internal_app_fa_dev.index_inv_site_study_trial_site_id;
DROP INDEX ctfo_internal_app_fa_dev.index_inv_site_study_trial_inv_id;
DROP INDEX ctfo_internal_app_fa_dev.index_inv_site_study_trial_site_inv_id;
DROP INDEX ctfo_internal_app_fa_dev.mview_site_id_1;
DROP INDEX ctfo_internal_app_fa_dev.mview_inv_id_1;

-- ctfo_internal_app_fa_dev.f_rpt_filters_site_inv
CREATE INDEX index_filter_country ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (country );
CREATE INDEX index_filter_trial_id_1 ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (ctfo_trial_id);
CREATE INDEX index_filter_site_id ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (ctfo_site_id );
CREATE INDEX index_filter_inv_id ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (ctfo_investigator_id);
CREATE INDEX index_filter_trial_site_id ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (trial_site_id);
CREATE INDEX index_filter_trial_site_inv_id ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (trial_site_inv_id);
CREATE INDEX index_filter_country_code ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (country_code );
CREATE INDEX index_filter_region ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (region );
CREATE INDEX index_filter_trial_inv_id ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv USING btree (ctfo_investigator_id, ctfo_trial_id);
CREATE INDEX index_filter_country_usa ON ctfo_internal_app_fa_dev.f_rpt_filters_site_inv (country) where country = 'United States';

-- ctfo_internal_app_fa_dev.f_rpt_filters_setup_all
CREATE INDEX index_filter_ta ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (therapeutic_area);
CREATE INDEX index_filter_dis ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (disease);
CREATE INDEX index_filter_phase ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (trial_phase);
CREATE INDEX index_filter_ta_dis_phase ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (therapeutic_area, disease, trial_phase);
CREATE INDEX index_filter_trial_id_2 ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (ctfo_trial_id);
CREATE INDEX index_filter_nct ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (nct_id);
CREATE INDEX index_filter_protocol ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (protocol_ids);
CREATE INDEX index_filter_title ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (study_name);
CREATE INDEX index_filter_pat_seg ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all USING btree (patient_segment);
CREATE INDEX index_filter_ta_onc ON ctfo_internal_app_fa_dev.f_rpt_filters_setup_all (therapeutic_area) where therapeutic_area = 'Oncology';

-- ctfo_internal_app_fa_dev.f_rpt_filters_trial
CREATE INDEX index_filter_trial_id_3 ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (ctfo_trial_id);
CREATE INDEX index_filter_sponsor ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (sponsor);
CREATE INDEX index_filter_drug ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (drug_names);
CREATE INDEX index_filter_trial_status ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (trial_status);
CREATE INDEX index_filter_drug_status ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (drug_status);
CREATE INDEX index_filter_cas ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (cas_num);
CREATE INDEX index_filter_gender ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (gender);
CREATE INDEX index_filter_age ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (minimum_age, maximum_age);
CREATE INDEX index_filter_mesh ON ctfo_internal_app_fa_dev.f_rpt_filters_trial USING btree (mesh_term);

-- ctfo_internal_app_fa_dev.f_rpt_site_study_details
CREATE INDEX index_site_study_trial_id ON ctfo_internal_app_fa_dev.f_rpt_site_study_details USING btree (ctfo_trial_id);
CREATE INDEX index_site_study_site_id ON ctfo_internal_app_fa_dev.f_rpt_site_study_details USING btree (ctfo_site_id);
CREATE INDEX index_site_study_trial_site_id ON ctfo_internal_app_fa_dev.f_rpt_site_study_details USING btree (trial_site_id);
CREATE INDEX index_site_study_trial_status ON ctfo_internal_app_fa_dev.f_rpt_site_study_details USING btree (trial_status);

-- ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details
CREATE INDEX index_inv_site_study_trial_id ON ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details USING btree (ctfo_trial_id);
CREATE INDEX index_inv_site_study_site_id ON ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details USING btree (ctfo_site_id);
CREATE INDEX index_inv_site_study_inv_id ON ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details USING btree (ctfo_investigator_id);
CREATE INDEX index_inv_site_study_trial_site_id ON ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details USING btree (trial_site_id);
CREATE INDEX index_inv_site_study_trial_inv_id ON ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details USING btree (ctfo_investigator_id, ctfo_trial_id);
CREATE INDEX index_inv_site_study_trial_site_inv_id ON ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details USING btree (trial_site_inv_id);

-- vacuum analyze tables
vacuum analyze ctfo_internal_app_fa_dev.f_rpt_filters_site_inv;
vacuum analyze ctfo_internal_app_fa_dev.f_rpt_filters_setup_all;
vacuum analyze ctfo_internal_app_fa_dev.f_rpt_filters_trial;
vacuum analyze ctfo_internal_app_fa_dev.f_rpt_site_study_details;
vacuum analyze ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details;

-- refresh materialized views
DROP MATERIALIZED VIEW ctfo_internal_app_fa_dev.inv_completed_trials_vw;
CREATE MATERIALIZED VIEW ctfo_internal_app_fa_dev.inv_completed_trials_vw
AS
SELECT ctfo_investigator_id,
count(DISTINCT CASE
WHEN trial_status = 'Closed' OR trial_status = 'Completed' OR trial_status = 'Terminated' THEN ctfo_trial_id
ELSE NULL
END) AS completed_trials
FROM ctfo_internal_app_fa_dev.f_rpt_investigator_site_study_details
GROUP BY ctfo_investigator_id
WITH DATA;

DROP MATERIALIZED VIEW ctfo_internal_app_fa_dev.site_workload_vw;
CREATE MATERIALIZED VIEW ctfo_internal_app_fa_dev.site_workload_vw
AS
SELECT ctfo_site_id,
count(DISTINCT CASE
WHEN trial_status = 'Active (Recruiting)' OR trial_status = 'Active (Not Recruiting)' THEN ctfo_trial_id
ELSE NULL
END) AS current_workload
FROM ctfo_internal_app_fa_dev.f_rpt_site_study_details
GROUP BY ctfo_site_id
WITH DATA;

DROP MATERIALIZED VIEW ctfo_internal_app_fa_dev.country_startup_time;
CREATE MATERIALIZED VIEW ctfo_internal_app_fa_dev.country_startup_time
AS
SELECT site_country AS country,
region,
region_code,
country_code,
median(average_startup_time) AS average_startup_time
FROM ctfo_internal_app_fa_dev.f_rpt_site_study_details
GROUP BY site_country, region, region_code, country_code
WITH DATA;


-- create indexes on materialized view
create index mview_site_id_1 on ctfo_internal_app_fa_dev.site_workload_vw using btree (ctfo_site_id);
create index mview_inv_id_1 on ctfo_internal_app_fa_dev.inv_completed_trials_vw using btree (ctfo_investigator_id);



