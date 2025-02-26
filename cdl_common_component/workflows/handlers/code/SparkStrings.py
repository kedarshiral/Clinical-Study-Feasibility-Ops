#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ZS Associates'
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_cc/browse ) & all static servers.

"""
Doc_Type            : Metric Calculation Engine Spark Strings
Tech Description    : This code contains spark string
"""

TEMPLATE = {

    # extra spark string which are needed to execute metric template
    "spark_string": {

        # import libraries
        "import_str": """\nimport copy
                         \nfrom pyspark.sql.functions import *
                         \nfrom pyspark.sql.window import Window
                         \nfrom pyspark.sql.types import *
                         \nfrom pyspark.sql import SparkSession
                         \nspark = SparkSession.builder.enableHiveSupport().getOrCreate()
                      """,

        # if table is given as input type then convert it into dataframe
        "register_df": """\ninput_df = spark.sql("SELECT * from $$source_table$$")
                       """,
        # if table is given as target type then convert it into dataframe
        "register_target_df": """\ntarget_df = spark.sql("SELECT * from $$source_table$$")
                       """,

        # spark code to filter input data
        "filter_condition": """\n$$column_name$$ = when($$optional_filters$$, $$column_name$$).otherwise(None)
                            """,

        "aggregated_filter_condition" : """\naggregated_standard_$$column_name$$_df = aggregated_standard_$$column_name$$_df.filter(aggregated_standard_$$column_name$$_df.$$optional_filters$$)""",

        # spark code to create new columns for each metric
        "generate_withColumn_str": ".withColumn('$$column_name$$', $$column_name$$.cast('$$metric_data_type$$'))",

        # spark string to create new columns for metrics which are in-active
        "generate_withColumn_for_inactive_metric_str": ".withColumn('$$column_name$$', lit(None))",

        # spark string for inactive custom metric
        "inactive_custom_metric": """\ninput_custom_df = spark.table('$$target_table$$')
                                     \n$$metric_type$$_$$column_name$$_df = input_custom_df.withColumn('$$column_name$$', lit(None))
                              """,

        # spark code to be used while creating a new dataframe which contains all source columns as well as new metric
        # columns
        "create_new_df_str": "\n$$metric_type$$_df = input_df",

        # get columns for standard & custom metric dataframes
        "get_columns_string": """\n$$dataframe_name$$_columns = $$dataframe_name$$.columns""",

        # spark code to add equal columns in each dataframe
        "select_all_columns_in_dataframe_string": """\ncolumn_lookup_list = $$metric_column_list$$
                                                 \ntarget_table = spark.table("$$target_table$$")
                                                 \ntarget_columns = target_table.columns
                                                 \nselect_columns = copy.deepcopy(target_columns)
                                                 \ncolumns_for_group_by = copy.deepcopy(target_columns)
                                                 \nfor column in column_lookup_list:
                                                 \n    if column not in select_columns:
                                                 \n        select_columns.append(column)
                                                 \n    else:
                                                 \n        columns_for_group_by.remove(column)

                                              """,

        # spark code to add equal columns in each dataframe
        "select_all_columns_in_tempdataframe_string": """\ncolumn_lookup_list = $$metric_column_list$$
                                                 \ntarget_table = spark.table("$$target_table$$")
                                                 \ntarget_columns = target_table.columns
                                                 \nselect_columns = copy.deepcopy(target_columns)
                                                 \ncolumns_for_group_by = copy.deepcopy(target_columns)

                                              """,

        "add_extra_columns": """\nfor column in column_lookup_list:
                                \n  if column not in {df_columns}:
                                \n    {dataframe} = {dataframe}.withColumn(column, lit(None))
                             """,

        # get base columns from target table
        "string_to_get_base_column_of_target_table": """\ncolumn_lookup = $$metric_column_list$$
                                                    \nselect_columns = spark.table("$$target_table$$").columns
                                                    \n
                                                    \nfor column in column_lookup:
                                                    \n    if column in select_columns:
                                                    \n        select_columns.remove(column)
                                                 """,

        # spark code to register dataframe as temp table
        "register_tempdf_as_table_str": """\njoinDF.registerTempTable('aggregated_tempdataframe_temp_table')
                                """,

        # spark code to register dataframe as temp table
        "register_df_as_table_str": """\nunionDF.registerTempTable('aggregated_dataframe_temp_table')
                                """,

        "register_aggregated_df_as_table_str":
            """\naggregated_dataframe.registerTempTable('aggregated_dataframe_table')
                                """,

        "aggregated_dataframe_str": """\nquery_string = "SELECT "
                \nparameters = ""
                \nmin_query = ""
                \nfrom_query = " from aggregated_dataframe_temp_table"
                \nfor each_item in columns_for_group_by:
                \n  parameters = parameters + each_item + ", "
                \nfor each_item in column_lookup_list:
                \n  min_query += " min(" + each_item + ")" + " as " + each_item + ","
                \nparameters = parameters.rstrip(" ")
                \nmin_query = min_query.rstrip(", ")
                \nquery_string = query_string + parameters + min_query + from_query + " group by " + parameters
                \nquery_string = query_string.rstrip(",")
                \naggregated_dataframe = spark.sql(query_string)
        """,


        "temp_aggregated_dataframe_str": """\nquery_string = "SELECT "
                \nfrom_query = "* from aggregated_tempdataframe_temp_table"
                \nquery_string = query_string + from_query
                \naggregated_dataframe = spark.sql(query_string)
        """,

        # spark function to extract column list for any given dataframe
        "select_columns_str": """ \ndef get_columns_list(columns):
                              \n  df = spark.table('$$target_table$$')
                              \n  column_list = df.columns
                              \n  col_str = ''
                              \n  for col in column_list:
                              \n    col_str = col_str+','+col
                              \n  col_str = col_str.strip(',')
                              \n  return str(col_str)
                            """,
        "alter_table_string": """\ntarget_table_df =spark.table("$$target_table$$")
                             \ncolumn_list = target_table_df.columns
                             \nadd_column_string = ""
                             \ncolumn_lookup=$$metric_column_list$$
                             \nschema_dict = $$alter_table_schema$$
                             \nfor column_name in column_lookup:
                             \n  if column_name not in column_list:
                             \n    add_column_string += column_name + "  " + schema_dict[column_name]+","
                             \nadd_column_string = add_column_string.strip(",")
                             \nif add_column_string != "":
                             \n  alter_string = "alter table $$target_table$$ add columns({column_list})".format(column_list=add_column_string)
                             \n  spark.sql(alter_string)
                          """,

        # spark code to insert calculated metric data into final output table
        "insert_into_target_table_str": """\ntarget_table_df = spark.table('$$target_table$$')
                                       \ncolumn_list = target_table_df.columns
                                       \nselect_column_string = ""
                                       \nfor column_name in column_list:
                                       \n  select_column_string += column_name + ","
                                       \nselect_column_string = select_column_string.strip(",")
                                       \nquery = "insert overwrite table $$target_table$$ select "
                                       \nfor i in target_table_df.columns :
                                       \n   query = query + i + ","

                                       \nquery=query.rstrip(",")
                                       \nquery = query + " from aggregated_dataframe_table"

                                       \nspark.sql(query)
                                    """,

        # spark command string to insert into metric target table
        "final_reporting_data_str": """
                                    \ncolumns = target_table_df.columns
                                    \nfinal_df = joined_df.select(columns)
                                    \nfinal_df.registerTempTable('final_temp_table')
                                    \ntarget_table_df = spark.table('$$target_table$$')
                                    \ncolumn_list = target_table_df.columns
                                    \nselect_column_string = ""
                                    \nfor column_name in column_list:
                                    \n  select_column_string += column_name + ","
                                    \nselect_column_string = select_column_string.strip(",")
                                    \nquery = '''insert into table $$target_table$$ select '''+select_column_string+'''  from final_temp_table'''
                                    \nspark.sql(query)
                                """
    }
}
