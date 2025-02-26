select
       organizationTrialId as organization_trial_id,
       organizationId as organization_id,
       trialId as trial_id,
       trialStatusId as trial_status_id,
       trialStatus as trial_status,
       trialStartDate as trial_start_date,
       trialPhase as trial_phase,
       copyrightNotice as copyright_notice
 from $$table$$