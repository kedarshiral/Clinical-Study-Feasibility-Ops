################################# Module Information ######################################
#  Module Name         : Dedupe Utility
#  Purpose             : This contains generic functions for maintaining Golden IDs across
#                        runs, incorporating KM valdidated output.
#  Pre-requisites      : This script should be present in the job executor folder.
#  Last changed on     : 03-07-2023
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

import os


def maintainGoldenID(cur_data_df, d_enty, process_id, prev_data_flg, env):
    """
    Purpose: This function maps the golden IDs of last successful run with the current run on the basis of hash_uid.
    Input  : Intermediate HDFS table consisting columns of hash_uid and golden ID evaluated for the latest run.
    Output : HDFS table with hash_uids mapped with the correct golden IDs based on correctness of previous and current run.
    """

    print("cur_data_df: {}, d_enty: {}, process_id: {}, prev_data_flg: {}".format(cur_data_df, d_enty, process_id,
                                                                                  prev_data_flg))

    # Defining cursor
    import CommonConstants as CommonConstants
    from MySQLConnectionManager import MySQLConnectionManager
    from ConfigUtility import JsonConfigUtility
    import MySQLdb

    configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
    mysql_connection = MySQLConnectionManager().get_my_sql_connection()
    cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local").appName('sprk-job').enableHiveSupport().getOrCreate())
    spark.sql("""show databases""").show()
    spark.sql("""show tables""").show()


    # get latest successful data_dt and cycle_id
    print("Executing query to get latest successful cycle_id")
    cursor.execute(
        "select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = {process_id} order by cycle_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, process_id=process_id))
    fetch_enable_flag_result = cursor.fetchone()
    latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
    latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])

    # read data for latest data_date and cycle_id
    # data is getting as investigator under mastering therfore this change
    if (d_enty == "inv"):
        d_enty_path = d_enty
        d_enty_path = d_enty_path.replace("inv", "investigator")
        # path = "s3://aws-a0220-use1-00-$$s3_env-s3b-shrd-cus-cdl01/clinical-data-lake/applications/commons/temp/mastering/{}/temp_xref_{}/pt_data_dt={}/pt_cycle_id={}/".format(
        #     d_enty_path, d_enty, latest_stable_data_date, latest_stable_cycle_id).replace('$$s3_env', env)
        path = "{}/applications/commons/temp/mastering/{}/temp_xref_{}/pt_data_dt={}/pt_cycle_id={}/".format(
            bucket_path, d_enty_path, d_enty, latest_stable_data_date, latest_stable_cycle_id).replace('$$s3_env',
                                                                                                       env)

        previous_snapshot_data = spark.read.parquet(path)
        previous_snapshot_data.registerTempTable("previous_snapshot_data")

        previous_data = spark.sql("""
                            select
                                hash_uid,
                                """ + d_enty + """_id as golden_id
                            from previous_snapshot_data
                            group by 1,2
                            """)
        previous_data.registerTempTable("previous_data")

        current_data = spark.sql("""
                            select
                                hash_uid,
                                 """ + d_enty + """_id as golden_id
                            from """ + cur_data_df + """
                            group by 1,2
                            """)

        current_data.registerTempTable("current_data")

        # Mapping the golden id of previous run on basis of hash uid
        old_to_new_map_1 = spark.sql("""
                            select
                                a.*,
                                concat('old-',b.golden_id) as old_golden_id,
                                concat('new-',a.golden_id) as new_golden_id
                           from current_data a
                            left join (select hash_uid, golden_id from previous_data group by 1,2) b
                            on a.hash_uid=b.hash_uid""")
        old_to_new_map_1.registerTempTable("old_to_new_map_1")

        old_to_new_map_1.write.mode("overwrite").saveAsTable("old_to_new_map_1_{}".format(d_enty))
        if prev_data_flg == 'N':
            print("New Run is correct")
            # Case - Old: Clustered New: Clustered + Extra
            old_to_new_map_2 = spark.sql("""
                                select /*+ broadcast(b) */
                                    a.hash_uid,
                                    split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] as old_golden_id,
                                    a.new_golden_id
                                from old_to_new_map_1 a
                                left outer join
                                (select old_golden_id, new_golden_id from old_to_new_map_1 where old_golden_id is not null group by 1,2 ) b
                                on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                                group by 1,2,3
                                """)

            old_to_new_map_2.registerTempTable("old_to_new_map_2")

            # Case - Old: Declustered New: Clustered
            old_new_golden_id_min = spark.sql("""
                                select
                                    min(old_golden_id) as old_golden_id,
                                    new_golden_id
                                from old_to_new_map_2
                                group by 2
                                """)

            old_new_golden_id_min.registerTempTable("old_new_golden_id_min")

            old_to_new_map_3 = spark.sql("""
                                select /*+ broadcast(b) */
                                    a.hash_uid,
                                    b.old_golden_id,
                                    a.new_golden_id
                                from old_to_new_map_2 a
                                left outer join old_new_golden_id_min b
                                on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                                group by 1,2,3
                                """)

            old_to_new_map_3.registerTempTable("old_to_new_map_3")

            # Logic to nullify the old golden id for declustered records identified in new run
            old_new_golden_id_count = spark.sql("""
                                select /*+ broadcast(b) */
                                    a.old_golden_id,
                                    a.new_golden_id,
                                    count(*) as rec_count
                                from old_to_new_map_3 a
                                inner join
                                (select old_golden_id from (select old_golden_id, count(distinct new_golden_id) from old_to_new_map_3 group by 1 having count(distinct new_golden_id)> 1)) b
                                on a.old_golden_id = b.old_golden_id
                                group by 1,2
                                """)

            old_new_golden_id_count.registerTempTable("old_new_golden_id_count")

            nullify_declustered_rec = spark.sql("""
                                select
                                    new_golden_id,
                                    case when rec_count = 1 and a.new_golden_id <> b.min_golden_id then NULL else b.old_golden_id end as  old_golden_id
                                from old_new_golden_id_count a
                                left outer join
                                (select old_golden_id, min(new_golden_id) as min_golden_id from old_new_golden_id_count group by 1) b
                                on a.old_golden_id = b.old_golden_id
                                """)

            nullify_declustered_rec.registerTempTable("nullify_declustered_rec")

            old_to_new_map_4 = spark.sql("""
                                select /*+ broadcast(b) */
                                    a.hash_uid,
                                   case when b.new_golden_id is null then a.old_golden_id else b.old_golden_id end as old_golden_id,
                                    a.new_golden_id
                                from old_to_new_map_3 a
                                left outer join nullify_declustered_rec b
                                on a.new_golden_id = b.new_golden_id
                                """)

            old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))
        else:
            print("Old Run is correct")

            temp1 = spark.sql("""select old_golden_id, 'Y' as flag from
                                (select old_golden_id from old_to_new_map_1 group by 1 having count(distinct new_golden_id)>1)
                                """)
            temp1.registerTempTable("temp1")
            # temp1.write.mode("overwrite").saveAsTable("temp1_{}".format(d_enty))

            temp = spark.sql("""select old_golden_id, new_golden_id from
                                (select old_golden_id, new_golden_id, row_number() over (partition by old_golden_id order by cnt desc) as rnk from
                                (select old_golden_id, new_golden_id, count(distinct hash_uid) as cnt from old_to_new_map_1 where old_golden_id is not null group by 1,2) c ) d 
                                where rnk=1
                                """)
            temp.registerTempTable("temp")
            # temp.write.mode("overwrite").saveAsTable("temp_{}".format(d_enty))

            old_to_new_map_4 = spark.sql("""
                                select /*+ broadcast(b) */
                                    a.hash_uid,
                                    case when temp1.flag = 'Y' then split(b.old_golden_id,'-')[1]
                                    else split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] end as old_golden_id,
                                    a.new_golden_id
                                from old_to_new_map_1 a
                                left outer join
                                (select old_golden_id, new_golden_id from temp group by 1,2) b
                                on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                                left outer join temp1 on temp1.old_golden_id=a.old_golden_id
                                group by 1,2,3
                                """)
            old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))

            new_golden_id_for_null_rec = spark.sql("""
                select
                    a.new_golden_id,
                    concat("ctfo_","{}_", row_number() over(order by null) + coalesce(temp.max_id,0)) as final_golden_id
                from
                (select new_golden_id from old_to_new_map_4_{} where old_golden_id is null group by 1) a cross join
                (select max(cast(split(new_golden_id,"{}_")[1] as bigint)) as max_id from old_to_new_map_4_{}) temp""".format(
                d_enty, d_enty, d_enty, d_enty))

            new_golden_id_for_null_rec.registerTempTable("new_golden_id_for_null_rec")

            temp_golden_id_xref = spark.sql("""
                        select
                            hash_uid,
                            new_golden_id as cur_run_id,
                            old_golden_id as """ + "ctfo_" + d_enty + """_id
                        from old_to_new_map_4_""" + d_enty + """ where old_golden_id is not null
                        union
                        select
                            a.hash_uid,
                            a.new_golden_id as cur_run_id,
                            b.final_golden_id as """ + "ctfo_" + d_enty + """_id
                        from old_to_new_map_4_""" + d_enty + """ a left outer join new_golden_id_for_null_rec b on a.new_golden_id = b.new_golden_id
                        where a.old_golden_id is null""")

            temp_golden_id_xref.write.mode("overwrite").saveAsTable("maintained_golden_id_xref_" + d_enty)
            final_table = "maintained_golden_id_xref_" + d_enty
            print("Final Table created: ".format(final_table))
            return final_table

    if (d_enty == "trial"):
        path = "{}/applications/commons/temp/mastering/{}/temp_xref_{}/pt_data_dt={}/pt_cycle_id={}/".format(
            bucket_path, d_enty, d_enty, latest_stable_data_date, latest_stable_cycle_id)
        previous_snapshot_data = spark.read.parquet(path)

        previous_snapshot_data.registerTempTable("previous_snapshot_data")

        previous_data = spark.sql("""
            select
                hash_uid,
                """ + d_enty + """_id as golden_id,nct_id
            from previous_snapshot_data
            group by 1,2,3
            """)
        previous_data.registerTempTable("previous_data")

        current_data = spark.sql("""
        select
            hash_uid,
             """ + d_enty + """_id as golden_id,nct_id
        from """ + cur_data_df + """
        group by 1,2,3
        """)

        current_data.registerTempTable("current_data")

        # Mapping the golden id of previous run on basis of hash uid
        old_to_new_map_1 = spark.sql("""
            SELECT
                a.*,
                COALESCE(concat('old-', b1.golden_id), concat('old-', b2.golden_id)) AS old_golden_id,
                concat('new-', a.golden_id) AS new_golden_id
            FROM current_data a
            LEFT JOIN (SELECT hash_uid, golden_id, nct_id FROM previous_data GROUP BY hash_uid, golden_id, nct_id) b1
                ON  a.nct_id = b1.nct_id
            LEFT JOIN (SELECT hash_uid, golden_id FROM previous_data GROUP BY hash_uid, golden_id) b2
                ON a.hash_uid = b2.hash_uid AND a.nct_id IS NULL
        """)
        old_to_new_map_1.registerTempTable("old_to_new_map_1")

        old_to_new_map_1.write.mode("overwrite").saveAsTable("old_to_new_map_1_{}".format(d_enty))
        if prev_data_flg == 'N':
            print("New Run is correct")
            # Case - Old: Clustered New: Clustered + Extra
            old_to_new_map_2 = spark.sql("""
            select /*+ broadcast(b) */
                a.hash_uid,
                split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] as old_golden_id,
                a.new_golden_id
            from old_to_new_map_1 a
            left outer join
            (select old_golden_id, new_golden_id from old_to_new_map_1 where old_golden_id is not null group by 1,2 ) b
            on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
            group by 1,2,3
            """)

            old_to_new_map_2.registerTempTable("old_to_new_map_2")

            # Case - Old: Declustered New: Clustered
            old_new_golden_id_min = spark.sql("""
            select
                min(old_golden_id) as old_golden_id,
                new_golden_id
            from old_to_new_map_2
            group by 2
            """)

            old_new_golden_id_min.registerTempTable("old_new_golden_id_min")

            old_to_new_map_3 = spark.sql("""
            select /*+ broadcast(b) */
                a.hash_uid,
                b.old_golden_id,
                a.new_golden_id
            from old_to_new_map_2 a
            left outer join old_new_golden_id_min b
            on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
            group by 1,2,3
            """)

            old_to_new_map_3.registerTempTable("old_to_new_map_3")

            # Logic to nullify the old golden id for declustered records identified in new run
            old_new_golden_id_count = spark.sql("""
            select /*+ broadcast(b) */
                a.old_golden_id,
                a.new_golden_id,
                count(*) as rec_count
            from old_to_new_map_3 a
            inner join
            (select old_golden_id from (select old_golden_id, count(distinct new_golden_id) from old_to_new_map_3 group by 1 having count(distinct new_golden_id)> 1)) b
            on a.old_golden_id = b.old_golden_id
            group by 1,2
            """)

            old_new_golden_id_count.registerTempTable("old_new_golden_id_count")

            nullify_declustered_rec = spark.sql("""
            select
                new_golden_id,
                case when rec_count = 1 and a.new_golden_id <> b.min_golden_id then NULL else b.old_golden_id end as  old_golden_id
            from old_new_golden_id_count a
            left outer join
            (select old_golden_id, min(new_golden_id) as min_golden_id from old_new_golden_id_count group by 1) b
            on a.old_golden_id = b.old_golden_id
            """)

            nullify_declustered_rec.registerTempTable("nullify_declustered_rec")

            old_to_new_map_4 = spark.sql("""
            select /*+ broadcast(b) */
                a.hash_uid,
               case when b.new_golden_id is null then a.old_golden_id else b.old_golden_id end as old_golden_id,
                a.new_golden_id
            from old_to_new_map_3 a
            left outer join nullify_declustered_rec b
            on a.new_golden_id = b.new_golden_id
            """)

            old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))
        else:
            print("Old Run is correct")

            temp1 = spark.sql("""select old_golden_id, 'Y' as flag from
            (select old_golden_id from old_to_new_map_1 group by 1 having count(distinct new_golden_id)>1)
            """)
            temp1.registerTempTable("temp1")
            # temp1.write.mode("overwrite").saveAsTable("temp1_{}".format(d_enty))

            temp = spark.sql("""select old_golden_id, new_golden_id from
            (select old_golden_id, new_golden_id, row_number() over (partition by old_golden_id order by cnt desc) as rnk from
            (select old_golden_id, new_golden_id, count(distinct hash_uid) as cnt from old_to_new_map_1 where old_golden_id is not null group by 1,2) c ) d 
            where rnk=1
            """)
            temp.registerTempTable("temp")
            # temp.write.mode("overwrite").saveAsTable("temp_{}".format(d_enty))

            old_to_new_map_4 = spark.sql("""
            select /*+ broadcast(b) */
                a.hash_uid,
                case when temp1.flag = 'Y' then split(b.old_golden_id,'-')[1]
                else split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] end as old_golden_id,
                a.new_golden_id
            from old_to_new_map_1 a
            left outer join
            (select old_golden_id, new_golden_id from temp group by 1,2) b
            on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
            left outer join temp1 on temp1.old_golden_id=a.old_golden_id
            group by 1,2,3
            """)

            old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4")

            temp_new = spark.sql("""
                SELECT new_golden_id, 'Y' AS flag FROM (
                    SELECT new_golden_id FROM old_to_new_map_4 
                    GROUP BY new_golden_id 
                    HAVING COUNT(DISTINCT old_golden_id) > 1
                )
            """)
            temp_new.registerTempTable("temp_new")

            temp_old = spark.sql("""
                SELECT new_golden_id, old_golden_id FROM (
                    SELECT new_golden_id, old_golden_id, 
                        ROW_NUMBER() OVER (PARTITION BY new_golden_id ORDER BY cnt DESC) AS rnk 
                    FROM (
                        SELECT new_golden_id, old_golden_id, COUNT(DISTINCT hash_uid) AS cnt 
                        FROM old_to_new_map_4 
                        WHERE new_golden_id IS NOT NULL 
                        GROUP BY new_golden_id, old_golden_id
                    ) c 
                ) d 
                WHERE rnk = 1
            """)
            temp_old.write.mode("overwrite").saveAsTable("temp_old")

            old_to_new_map_final = spark.sql("""
                 SELECT DISTINCT
                     a.hash_uid,
                     split(a.new_golden_id,'-')[1] as new_golden_id,
                     CASE
                         WHEN temp_new.flag = 'Y' THEN b.old_golden_id
                         ELSE COALESCE(a.old_golden_id, b.old_golden_id)
                     END AS old_golden_id
                 FROM old_to_new_map_4 a
                 LEFT JOIN temp_old b
                     ON a.new_golden_id = b.new_golden_id
                 LEFT JOIN temp_new
                     ON temp_new.new_golden_id = a.new_golden_id
             """)

            old_to_new_map_final.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))
            new_golden_id_for_null_rec = spark.sql("""
                select
                    a.new_golden_id,
                    concat("ctfo_","{}_", row_number() over(order by null) + coalesce(temp.max_id,0)) as final_golden_id
                from
                (select new_golden_id from old_to_new_map_4_{} where old_golden_id is null group by 1) a cross join
                (select max(cast(split(new_golden_id,"{}_")[1] as bigint)) as max_id from old_to_new_map_4_{}) temp""".format(
                d_enty, d_enty, d_enty, d_enty))

            new_golden_id_for_null_rec.registerTempTable("new_golden_id_for_null_rec")

            temp_golden_id_xref = spark.sql("""
                        select
                            hash_uid,
                            new_golden_id as cur_run_id,
                            old_golden_id as """ + "ctfo_" + d_enty + """_id
                        from old_to_new_map_4_""" + d_enty + """ where old_golden_id is not null
                        union
                        select
                            a.hash_uid,
                            a.new_golden_id as cur_run_id,
                            b.final_golden_id as """ + "ctfo_" + d_enty + """_id
                        from old_to_new_map_4_""" + d_enty + """ a left outer join new_golden_id_for_null_rec b on a.new_golden_id = b.new_golden_id
                        where a.old_golden_id is null""")

            temp_golden_id_xref.write.mode("overwrite").saveAsTable("maintained_golden_id_xref_" + d_enty)
            final_table = "maintained_golden_id_xref_" + d_enty
            print("Final Table created: ".format(final_table))
            return final_table


    if (d_enty == "site"):
        path = "{}/applications/commons/temp/mastering/{}/temp_xref_{}/pt_data_dt={}/pt_cycle_id={}/".format(
            bucket_path, d_enty, d_enty, latest_stable_data_date, latest_stable_cycle_id)
        previous_snapshot_data = spark.read.parquet(path)

        previous_snapshot_data.registerTempTable("previous_snapshot_data")

        previous_data = spark.sql("""
                select
                    hash_uid,
                    """ + d_enty + """_id as golden_id
                from previous_snapshot_data
                group by 1,2
                """)
        previous_data.registerTempTable("previous_data")

        current_data = spark.sql("""
                select
                    hash_uid,
                     """ + d_enty + """_id as golden_id
                from """ + cur_data_df + """
                group by 1,2
                """)

        current_data.registerTempTable("current_data")

        # Mapping the golden id of previous run on basis of hash uid
        old_to_new_map_1 = spark.sql("""
                select
                    a.*,
                    concat('old-',b.golden_id) as old_golden_id,
                    concat('new-',a.golden_id) as new_golden_id
               from current_data a
                left join (select hash_uid, golden_id from previous_data group by 1,2) b
                on a.hash_uid=b.hash_uid""")
        old_to_new_map_1.registerTempTable("old_to_new_map_1")

        old_to_new_map_1.write.mode("overwrite").saveAsTable("old_to_new_map_1_{}".format(d_enty))
        if prev_data_flg == 'N':
            print("New Run is correct")
            # Case - Old: Clustered New: Clustered + Extra
            old_to_new_map_2 = spark.sql("""
                    select /*+ broadcast(b) */
                        a.hash_uid,
                        split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] as old_golden_id,
                        a.new_golden_id
                    from old_to_new_map_1 a
                    left outer join
                    (select old_golden_id, new_golden_id from old_to_new_map_1 where old_golden_id is not null group by 1,2 ) b
                    on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                    group by 1,2,3
                    """)

            old_to_new_map_2.registerTempTable("old_to_new_map_2")

            # Case - Old: Declustered New: Clustered
            old_new_golden_id_min = spark.sql("""
                    select
                        min(old_golden_id) as old_golden_id,
                        new_golden_id
                    from old_to_new_map_2
                    group by 2
                    """)

            old_new_golden_id_min.registerTempTable("old_new_golden_id_min")

            old_to_new_map_3 = spark.sql("""
                    select /*+ broadcast(b) */
                        a.hash_uid,
                        b.old_golden_id,
                        a.new_golden_id
                    from old_to_new_map_2 a
                    left outer join old_new_golden_id_min b
                    on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                    group by 1,2,3
                    """)

            old_to_new_map_3.registerTempTable("old_to_new_map_3")

            # Logic to nullify the old golden id for declustered records identified in new run
            old_new_golden_id_count = spark.sql("""
                    select /*+ broadcast(b) */
                        a.old_golden_id,
                        a.new_golden_id,
                        count(*) as rec_count
                    from old_to_new_map_3 a
                    inner join
                    (select old_golden_id from (select old_golden_id, count(distinct new_golden_id) from old_to_new_map_3 group by 1 having count(distinct new_golden_id)> 1)) b
                    on a.old_golden_id = b.old_golden_id
                    group by 1,2
                    """)

            old_new_golden_id_count.registerTempTable("old_new_golden_id_count")

            nullify_declustered_rec = spark.sql("""
                    select
                        new_golden_id,
                        case when rec_count = 1 and a.new_golden_id <> b.min_golden_id then NULL else b.old_golden_id end as  old_golden_id
                    from old_new_golden_id_count a
                    left outer join
                    (select old_golden_id, min(new_golden_id) as min_golden_id from old_new_golden_id_count group by 1) b
                    on a.old_golden_id = b.old_golden_id
                    """)

            nullify_declustered_rec.registerTempTable("nullify_declustered_rec")

            old_to_new_map_4 = spark.sql("""
                    select /*+ broadcast(b) */
                        a.hash_uid,
                       case when b.new_golden_id is null then a.old_golden_id else b.old_golden_id end as old_golden_id,
                        a.new_golden_id
                    from old_to_new_map_3 a
                    left outer join nullify_declustered_rec b
                    on a.new_golden_id = b.new_golden_id
                    """)

            old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))
        else:
            print("Old Run is correct")

            temp1 = spark.sql("""select old_golden_id, 'Y' as flag from
                    (select old_golden_id from old_to_new_map_1 group by 1 having count(distinct new_golden_id)>1)
                    """)
            temp1.registerTempTable("temp1")
            # temp1.write.mode("overwrite").saveAsTable("temp1_{}".format(d_enty))

            temp = spark.sql("""select old_golden_id, new_golden_id from
                    (select old_golden_id, new_golden_id, row_number() over (partition by old_golden_id order by cnt desc) as rnk from
                    (select old_golden_id, new_golden_id, count(distinct hash_uid) as cnt from old_to_new_map_1 where old_golden_id is not null group by 1,2) c ) d 
                    where rnk=1
                    """)
            temp.registerTempTable("temp")
            # temp.write.mode("overwrite").saveAsTable("temp_{}".format(d_enty))

            old_to_new_map_4 = spark.sql("""
                    select /*+ broadcast(b) */
                        a.hash_uid,
                        case when temp1.flag = 'Y' then split(b.old_golden_id,'-')[1]
                        else split(coalesce(a.old_golden_id,b.old_golden_id),'-')[1] end as old_golden_id,
                        a.new_golden_id
                    from old_to_new_map_1 a
                    left outer join
                    (select old_golden_id, new_golden_id from temp group by 1,2) b
                    on split(a.new_golden_id,'-')[1] = split(b.new_golden_id,'-')[1]
                    left outer join temp1 on temp1.old_golden_id=a.old_golden_id
                    group by 1,2,3
                    """)
            old_to_new_map_4.write.mode("overwrite").saveAsTable("old_to_new_map_4_{}".format(d_enty))

            test = spark.sql("""select * from old_to_new_map_4_{} where old_golden_id is null """.format(d_enty))
            test.registerTempTable('test')
            # test2 = spark.sql("""select a.hash_uid, b.old_golden_id, a.new_golden_id from test a left join final_km_ids b
            #  on lower(trim(a.hash_uid)) = lower(trim(b.hash_uid))""")
            # test2.registerTempTable('test2')
            #
            # new_golden_id_for_null_rec = spark.sql("""
            # select
            #     a.new_golden_id,
            #     concat("ctfo_","{}_", row_number() over(order by null) + coalesce(temp.max_id,0)) as final_golden_id
            # from
            # (select new_golden_id from test2 where old_golden_id is null group by 1) a cross join
            # (select max(cast(split(new_golden_id,"{}_")[1] as bigint)) as max_id from old_to_new_map_4_{}) temp""".format(
            #     d_enty, d_enty, d_enty, d_enty))
            #
            # new_golden_id_for_null_rec.registerTempTable("new_golden_id_for_null_rec")

            test2_and_new_golden_id_df = spark.sql(f"""
                WITH test2 AS (
                    SELECT 
                        a.hash_uid, 
                        b.site_id as old_golden_id, 
                        a.new_golden_id 
                    FROM test a 
                    LEFT JOIN final_km_ids b 
                    ON LOWER(TRIM(a.hash_uid)) = LOWER(TRIM(b.hash_uid))
                ),
                max_id_temp AS (
                    SELECT 
                        MAX(CAST(SPLIT(new_golden_id, "{d_enty}_")[1] AS BIGINT)) AS max_id 
                    FROM old_to_new_map_4_{d_enty}
                ),
                new_golden_id_for_null_rec AS (
                    SELECT 
                        a.new_golden_id,
                        CONCAT("ctfo_", "{d_enty}_", ROW_NUMBER() OVER(ORDER BY NULL) + COALESCE(temp.max_id, 0)) AS final_golden_id
                    FROM 
                        (SELECT new_golden_id FROM test2 WHERE old_golden_id IS NULL GROUP BY new_golden_id) a 
                    CROSS JOIN 
                        max_id_temp temp
                )
                SELECT 
                    t2.hash_uid,
                    t2.new_golden_id,
                    COALESCE(t2.old_golden_id, ngr.final_golden_id) AS final_golden_id
                FROM test2 t2
                LEFT JOIN new_golden_id_for_null_rec ngr 
                ON t2.new_golden_id = ngr.new_golden_id
            """)

            test2_and_new_golden_id_df.write.mode("overwrite").saveAsTable('test2_and_new_golden_id_df')




            temp_golden_id_xref = spark.sql("""
                    select
                        hash_uid,
                        new_golden_id as cur_run_id,
                        old_golden_id as """ + "ctfo_" + d_enty + """_id
                    from old_to_new_map_4_""" + d_enty + """ where old_golden_id is not null
                    union
                    select
                        a.hash_uid,
                        a.new_golden_id as cur_run_id,
                        b.final_golden_id as """ + "ctfo_" + d_enty + """_id
                    from old_to_new_map_4_""" + d_enty + """ a left outer join test2_and_new_golden_id_df b on a.new_golden_id = b.new_golden_id
                    where a.old_golden_id is null""")

            temp_golden_id_xref.write.mode("overwrite").saveAsTable("maintained_golden_id_xref_" + d_enty)
            final_table = "maintained_golden_id_xref_" + d_enty
            print("Final Table created: ".format(final_table))
            return final_table


def maintainUniqueID(cur_data_df, d_enty, process_id, env):
    """
    Purpose: This function maps the golden IDs of last successful run with the current run on the basis of hash_uid.
    Input  : Intermediate HDFS table consisting columns of hash_uid and golden ID evaluated for the latest run.
    Output : HDFS table with hash_uids mapped with the correct golden IDs based on correctness of previous and current run.
    """

    print("cur_data_df: {}, d_enty: {}, process_id: {}".format(cur_data_df, d_enty, process_id))

    # Defining cursor
    from PySparkUtility import PySparkUtility
    from CommonUtils import CommonUtils
    import CommonConstants as CommonConstants
    from MySQLConnectionManager import MySQLConnectionManager
    from ConfigUtility import JsonConfigUtility
    import MySQLdb

    configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
    audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
    mysql_connection = MySQLConnectionManager().get_my_sql_connection()
    cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local").appName('sprk-job').enableHiveSupport().getOrCreate())
    spark.sql("""show databases""").show()
    spark.sql("""show tables""").show()
    try:
        # get latest successful data_dt and cycle_id
        print("Executing query to get latest successful cycle_id")
        cursor.execute(
            "select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = {process_id} order by cycle_start_time desc limit 1 ".format(
                orchestration_db_name=audit_db, process_id=process_id))
        fetch_enable_flag_result = cursor.fetchone()
        latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
        latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
        # read data for latest data_date and cycle_id
        previous_snapshot_data = spark.read.parquet(
            "{}/applications/commons/temp/dimensions/temp_d_{}_hash_map/"
            "/pt_data_dt={}/pt_cycle_id={}/".format(bucket_path, d_enty, latest_stable_data_date,
                                                    latest_stable_cycle_id).replace(
                '$$s3_env', env))

        previous_snapshot_data.registerTempTable("previous_snapshot_data")

        previous_data = spark.sql("""
        select
            hash_uid,
            {d_enty}_id as golden_id
        from previous_snapshot_data
        group by 1,2
        """.format(d_enty=d_enty))

        previous_data.registerTempTable("previous_data")

        current_data = spark.sql("""
        select
            hash_uid,
            {d_enty}_id as golden_id
        from {cur_data_df}
        group by 1,2
        """.format(d_enty=d_enty, cur_data_df=cur_data_df))

        current_data.registerTempTable("current_data")

        # Mapping the golden id of previous run on basis of hash uid
        old_to_new_map = spark.sql("""
        select
            a.*,
            b.golden_id as old_golden_id,
            a.golden_id as new_golden_id
        from current_data a
        left join (select hash_uid, golden_id from previous_data group by 1,2) b
        on a.hash_uid=b.hash_uid
        """)
        old_to_new_map.registerTempTable("old_to_new_map")

        # Generating new golden IDs for records having no golden ID assigned in previous run
        new_golden_id_for_null_rec = spark.sql("""
        select
            a.new_golden_id,
            concat("{d_enty}_", row_number() over(order by null) + coalesce(temp.max_id,0)) as final_golden_id
        from
        (select new_golden_id from old_to_new_map where old_golden_id is null group by 1) a cross join
        (select max(cast(split(old_golden_id,"{d_enty}_")[1] as bigint)) as max_id from old_to_new_map) temp""".format(
            d_enty=d_enty))

        new_golden_id_for_null_rec.registerTempTable("new_golden_id_for_null_rec")

        maintain_uniqueID_map = spark.sql("""
                select
                    c.hash_uid,
                    c.new_golden_id as cur_run_id,
                    c.old_golden_id as {d_enty}_id
                from old_to_new_map c where old_golden_id is not null
                union
                select
                    a.hash_uid,
                    a.new_golden_id as cur_run_id,
                    b.final_golden_id as {d_enty}_id
                from old_to_new_map a left outer join new_golden_id_for_null_rec b on a.new_golden_id = b.new_golden_id
                where a.old_golden_id is null""".format(d_enty=d_enty))
        maintain_uniqueID_map = maintain_uniqueID_map.withColumnRenamed(d_enty + "_id", "ctfo_" + d_enty + "_id")
        maintain_uniqueID_map.write.mode("overwrite").saveAsTable("maintained_id_d_" + d_enty)

        final_table = "maintained_id_d_" + d_enty
        print("Final Table created: ".format(final_table))
    finally:
        print("Function for maintaining Golden ID executed")
    return final_table


def KMValidate(data_df, d_enty, data_date, cycle_id, env):
    """
    Purpose: This function maps the KM cluster ID and KM score with the data on basis of hash_uid.
    Input  : Intermediate HDFS table consisting columns of cluster ID and score.
    Output : HDFS table with hash_uids mapped with the correct golden IDs based on KM validated output.
    """
    from pyspark.sql import SparkSession
    from ConfigUtility import JsonConfigUtility
    import CommonConstants as CommonConstants

    configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
    bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
    spark = (SparkSession.builder.master("local").appName('sprk-job').enableHiveSupport().getOrCreate())
    km_output_available = 'N'
    path = "{}/applications/commons/temp/mastering/{}/table_name/pt_data_dt={}/pt_cycle_id={}".format(
        bucket_path, d_enty, data_date, cycle_id)
    try:
        dedupe_km_input = spark.read.format('csv').option("header", "true").option("delimiter", ",").load(
            "{}/applications/commons/temp/km_validation/km_inputs/{}/".format(
                bucket_path, d_enty))

        print("\n\nKM data is available, starting KM validation step\n\n")
        km_output_available = 'Y'
        dedupe_km_input.write.mode('overwrite').saveAsTable('dedupe_km_input')
        # Modifying cluster ID and score based on KM input
        temp_all_dedupe_results_km_opt_union = spark.sql("""
        select
            a.hash_uid,
            coalesce(b.new_cluster_id, a.cluster_id) as final_cluster_id,
            cast(coalesce(b.new_score, a.score) as double) as final_score,
            coalesce(b.comment, '') as final_KM_comment
        from
        """ + data_df + """ a
        left outer join (select distinct hash_uid, new_cluster_id as new_cluster_id, new_score as new_score,
comment as comment from  (select *, row_number() over (partition by hash_uid order by new_cluster_id desc, new_score desc, comment desc)
as rnk from dedupe_km_input) where rnk =1 ) b
        on a.hash_uid = b.hash_uid
        """)
        # save on HDFS
        temp_all_dedupe_results_km_opt_union.write.mode("overwrite").saveAsTable(
            "temp_all_{}_dedupe_results_km_opt_union".format(d_enty))
        write_path = path.replace('table_name', 'temp_all_dedupe_results_km_opt_union_with_msd')
        temp_all_dedupe_results_km_opt_union.repartition(100).write.mode('overwrite').parquet(write_path)

    except Exception as e:
        if km_output_available == 'N':
            print("KM output is not available")
            temp_all_dedupe_results_km_opt_union = spark.sql("""
            select
                hash_uid,
                cluster_id as final_cluster_id,
                score as final_score,
                '' final_KM_comment
            from
            """ + data_df + """
            """)
            # save on HDFS
            temp_all_dedupe_results_km_opt_union.write.mode("overwrite").saveAsTable(
                "temp_all_{}_dedupe_results_km_opt_union".format(d_enty))
        else:
            print(
                "\n\nKM data was present, but a runtime exception occurred during KM validation. ERROR - {}\n\n".format(
                    str(e)))
            raise
    finally:
        temp_all_dedupe_results_km_opt_union_final = spark.sql("""
        select
            a.*,
            b.final_cluster_id,
            b.final_score,
            b.final_KM_comment
        from
        """ + data_df + """ a
        left outer join 
        temp_all_{d_enty}_dedupe_results_km_opt_union b
        on a.hash_uid = b.hash_uid
        """.format(d_enty=d_enty))
        temp_all_dedupe_results_km_opt_union_final.write.mode("overwrite").saveAsTable(
            "temp_all_{}_dedupe_results_km_opt_union_final".format(d_enty))
        # Push to S3
        write_path = path.replace("table_name", "temp_all_{}_dedupe_results_km_opt_union_final".format(d_enty))
        temp_all_dedupe_results_km_opt_union_final.repartition(100).write.mode("overwrite").parquet(write_path)
        # move input file to archieve
        command = "aws s3 mv {}/applications/commons/temp/km_validation/km_inputs/{}/ {}/applications/commons/temp/km_validation/km_inputs/archive/{}/{}/{}/ --recursive".format(
            bucket_path, d_enty, bucket_path, d_enty, data_date, cycle_id)
        os.system(command)
        print("KM Validation completed")
    return "temp_all_{}_dedupe_results_km_opt_union_final".format(d_enty)
