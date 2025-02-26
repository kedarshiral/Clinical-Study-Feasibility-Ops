
#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'

################################# Module Information #####################################################
#  Module Name         : Metric Template
#  Purpose             : Calculate screen failure rate
#  Pre-requisites      : MCE source table
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : startup_time values
#  Last changed on     : 12-02-2021
#  Last changed by     : Shivam Pandey, Prashant  Karan
#  Reason for change   : Initial Code Development
##########################################################################################################

# Dataframe to calculate the metric

# Reading dic from RDS table column template_parameters
template_params =$$parameters$$

# Reading values from template_params dictionary
aggregation_columns = template_params["aggregation_on"]
numerator = template_params["numerator"] # write a-b in config
denominator = template_params["denominator"] # write b in config
#column_name = template_params["column_name"]

query = """
select 
    """ +aggregation_columns+ """, 
    (""" + numerator + """)/(""" + denominator + """)
	
    as $$column_name$$ 
from $$source_table$$
"""

aggregated_standard_$$column_name$$_df = spark.sql(query)





