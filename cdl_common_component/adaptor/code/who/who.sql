select
	TRIALID,
	LAST_REFRESHED_ON,
	PUBLIC_TITLE,
	SCIENTIFIC_TITLE,
	ACRONYM,
	PRIMARY_SPONSOR,
	DATE_REGISTRATION,
	PROSPECTIVE_REGISTRATION,
	EXPORT_DATE,
	SOURCE_REGISTER,
	WEB_ADDRESS,
	RECRUITMENT_STATUS,
	OTHER_RECORDS,
	INCLUSION_AGEMIN,
	INCLUSION_AGEMAX,
	INCLUSION_GENDER,
	DATE_ENROLLEMENT,
	TARGET_SIZE,
	STUDY_TYPE,
	STUDY_DESIGN,
	PHASE,
	COUNTRIES,
	CONTACT_FIRSTNAME,
	CONTACT_LASTNAME,
	CONTACT_ADDRESS,
	CONTACT_EMAIL,
	CONTACT_TEL,
	CONTACT_AFFILIATION,
	INCLUSION_CRITERIA,
	EXCLUSION_CRITERIA,
	CONDITION,
	INTERVENTION,
	PRIMARY_OUTCOME,
	SECONDARY_OUTCOME,
	SECONDARY_ID,
	SOURCE_SUPPORT,
	SECONDARY_SPONSOR,
	DISEASE_AREA_NAME
from
(
select
	Trialid as TRIALID,
	date_of_export as LAST_REFRESHED_ON,
	Public_Title as PUBLIC_TITLE,
	Scientific_Title as SCIENTIFIC_TITLE,
	'' as ACRONYM,
	Primary_Sponsor as PRIMARY_SPONSOR,
	Date_Registration as DATE_REGISTRATION,
	'' as PROSPECTIVE_REGISTRATION,
	'' as EXPORT_DATE,
	'' as SOURCE_REGISTER,
	url as WEB_ADDRESS,
	Recruitment_Status as RECRUITMENT_STATUS,
	'' as OTHER_RECORDS,
	Agemin as INCLUSION_AGEMIN,
	Agemax as INCLUSION_AGEMAX,
	Gender as INCLUSION_GENDER,
	Date_enrollement as DATE_ENROLLEMENT,
	Target_Size as TARGET_SIZE,
	Study_Type as STUDY_TYPE,
	Study_Design as STUDY_DESIGN,
	Phase as PHASE,
	Countries as COUNTRIES,
	Public_Contact_Firstname as CONTACT_FIRSTNAME,
	Public_Contact_Lastname as CONTACT_LASTNAME,
	Public_Contact_Address as CONTACT_ADDRESS,
	Public_Contact_Email as CONTACT_EMAIL,
	Public_Contact_Tel as CONTACT_TEL,
	Public_Contact_Affiliation as CONTACT_AFFILIATION,
	Inclusion_Criteria as INCLUSION_CRITERIA,
	Exclusion_Criteria as EXCLUSION_CRITERIA,
	Conditions as CONDITION,
	Interventions as INTERVENTION,
	Primary_Outcome as PRIMARY_OUTCOME,
	Secondary_Outcomes as SECONDARY_OUTCOME,
	SecondaryIDs as SECONDARY_ID,
	Source_Support as SOURCE_SUPPORT,
	Secondary_sponsors as SECONDARY_SPONSOR,
	'$$disease_area$$' as DISEASE_AREA_NAME,
	rank() over(partition by Trialid order by TO_DATE(CAST(UNIX_TIMESTAMP(date_of_export, 'dd MMMM yyyy') AS TIMESTAMP)) desc ) as date_of_export_rank
from
$$table$$
) temp_table
where date_of_export_rank = 1
