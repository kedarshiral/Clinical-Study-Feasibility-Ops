SELECT 
	`$type`                                                                           AS type,
	copyrightNotice                                                                   AS copyright_notice,
	recordUrl 																		  AS record_url,
	organizationId 																	  AS organization_id,
	organizationName 																  AS organization_name,
	concat_ws('|', childOrganizationIds)											  AS child_organization_ids,
	concat_ws('|', siblingOrganizationIds)											  AS sibling_organization_ids,
	updatedDate 																	  AS updated_date
from $$table$$