
################################# Module Information ######################################
#  Module Name         : Investigator Dedupe Training Generation
#  Purpose             : This will generate training file and train modeule to get results for
# clustering
#  Pre-requisites      : data preparation table = temp_all_inv_data_prep
#  Last changed on     : 22-03-2023
#  Last changed by     : Kashish
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

import json
import os
import multiprocessing as mp
import multiprocessing.pool
import datetime
import dedupe
import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from pyhive import hive

timeStart = datetime.datetime.now()
import CommonConstants as CommonConstants

# temp1_kd= spark.sql(""" select * from temp_all_inv_data_prep order by name limit 10000 """)
# temp1_kd.dropDuplicates()
# temp1_kd.write.mode('overwrite').saveAsTable('temp1_kd')

#study_select = """select
#uid,
#concat(coalesce(lower(trim(split(name,' ')[0])),''),' ',
#coalesce(lower(trim(reverse(split(reverse(name),' ')[0]))),'')) as name, lower(email) as email,
#phone,therapeutic_area
#from temp_all_inv_data_prep
#where coalesce(lower(name),'na') not in ('na','','null')
#and name !=''"""
cur = hive.connect(host='localhost', port=10000).cursor()

cur.execute('drop table if exists temp_all_inv_match_score')
cur.execute('create table temp_all_inv_match_score (cluster_id string, uid bigint,score double) stored as parquet')

settings_file = CommonConstants.EMR_CODE_PATH+'/configs/job_executor/' \
                'all_inv_dedupe_setting'
training_file = CommonConstants.EMR_CODE_PATH+'/configs/job_executor/' \
                'all_inv_dedupe_training.json'


def cnvrt_to_dict(cur):
    names = [d[0] for d in cur.description]
    names_list = []
    for each in names:
        names_list.append(str(each))
    list_dict = []
    for row in cur.fetchall():
        list_dict.append(dict((zip(names_list, row))))
    for each in names:
        for each_dict in list_dict:
            if not each_dict[each]:
                each_dict[each] = None
            if isinstance(each_dict[each], int):
                pass
            else:
                each_dict[each] = each_dict[each]
    return list_dict



# def func_fuzzy_match_ta(field_1, field_2):
#     score = fuzz.WRatio(field_1, field_2)
#     return score


def func_fuzzy_match_name(field_1, field_2):
    score = fuzz.WRatio(field_1, field_2)
    return score
    # if score >= 85:
    #     return 1
    # else:
    #     return 0

#therapeutic_area_list_query = """select distinct therapeutic_area from temp_all_inv_data_prep"""
#cur = hive.connect(host='localhost', port=10000).cursor()
#cur.execute(therapeutic_area_list_query)
#therapeutic_area_list = cur.fetchall()
#print('therapeutic_area = ' + str(therapeutic_area_list))
########################################################################################

def execute_dedupe():
    #therapeutic_area = therapeutic_area.strip().lower()
    #print(therapeutic_area)
    therapeutic_area_data = """select uid, name, email, phone  from 
    temp_all_inv_data_prep where name is not null"""

    cur_start_time = datetime.datetime.now()
    cur = hive.connect(host='localhost', port=10000).cursor()
    # print('Established connection for {}'.format(therapeutic_area))
    cur_end_time = datetime.datetime.now()
    # print(
    #     '*****Time taken to establish Hive Connection for therapeutic_area: {} Start time: {}, '
    #     'End time: {}'.format(str(therapeutic_area), str(cur_start_time), str(cur_end_time)))
    print('Preparing sample data')
    try:
        data_d = {}
        cur.execute(therapeutic_area_data)
        list_of_dict = cnvrt_to_dict(cur)
        # itr = 1
        for each_dict in list_of_dict:
            if each_dict.get('phone'):
                each_dict['phone'] = tuple(sorted([phone.strip() for phone in
                                                   each_dict['phone'].split('|') if phone != 'none']))
            if each_dict.get('email'):
                each_dict['email'] = tuple(sorted([email.strip() for email in
                                                   each_dict['email'].split('|') if email != 'none']))
            data_d[int(each_dict['uid'])] = each_dict
            # print(data_d)
    except Exception as e:
        print('Error in converting result set to dictionary!')
        print(str(e))

    try:
        if os.path.exists(settings_file):
            print('reading from', settings_file)
            with open(settings_file, 'rb') as f:
                deduper = dedupe.StaticDedupe(f)
        else:
            # ## Training
            # Define the fields dedupe will pay attention to
            fields = fields = [
                {'field': 'name', 'variable name': 'name', 'type': 'Custom',
                 'comparator': func_fuzzy_match_name},
                {'field': 'email', 'variable name': 'email', 'type': 'Set', 'has missing': True},
                {'field': 'phone', 'variable name': 'phone', 'type': 'Set', 'has missing': True},

                {'type': 'Interaction', 'interaction variables': ['name', 'phone']},
                {'type': 'Interaction', 'interaction variables': ['name', 'email']}
            ]

            # Create a new deduper object and pass our data model to it.
            deduper = dedupe.Dedupe(fields)
            # To train dedupe, we feed it a sample of records.
            deduper.sample(data_d, 10000)

            # If we have training data saved from a previous run of dedupe,
            # look for it and load it in.
            # __Note:__ if you want to train from scratch, delete the training_file
            if os.path.exists(training_file):
                print('reading labeled examples from ', training_file)
                with open(training_file, 'rb') as f:
                    deduper.readTraining(f)

            # ## Active learning
            # Dedupe will find the next pair of records
            # it is least certain about and ask you to label them as duplicates
            # or not.
            # use 'y', 'n' and 'u' keys to flag duplicates
            # press 'f' when you are finished
            print('starting active labeling...')

            dedupe.consoleLabel(deduper)

            # Using the examples we just labeled, train the deduper and learn
            # blocking predicates
            deduper.train()  # default - use this, below is modified
            # deduper.train(index_predicates = False)

            # When finished, save our training to disk
            with open(training_file, 'w') as tf:
                deduper.writeTraining(tf)

            # Save our weights and predicates to disk.  If the settings file
            # exists, we will skip all the training and learning next time we run
            # this file.
            with open(settings_file, 'wb') as sf:
                deduper.writeSettings(sf)

            # deduper.cleanupTraining()

        # Find the threshold that will maximize a weighted average of our
        # precision and recall.  When we set the recall weight to 2, we are
        # saying we care twice as much about recall as we do precision.
        #
        # If we had more data, we would not pass in all the blocked data into
        # this function but a representative sample.
        print('Now calculating threshold!')
        cur_start_time = datetime.datetime.now()
        threshold = deduper.threshold(data_d, recall_weight=0.7)
        cur_end_time = datetime.datetime.now()
        print(threshold)
        print(
            '*****TIME Deplication Threshold Calculator Query Start time: {}, '
            'End time: {}'.format(
                str(
                    cur_start_time),
                str(
                    cur_end_time)))

        # ## Clustering

        # `match` will return sets of record IDs that dedupe
        # believes are all referring to the same entity.
        cur_start_time = datetime.datetime.now()
        print('clustering...')
        duplicates = deduper.match(data_d, threshold)
        cur_end_time = datetime.datetime.now()
        print('*****TIME De-duplication Match Start time: {}, End time: {}'.
              format(str(cur_start_time), str(cur_end_time)))

        # print duplicates
        query = 'insert into temp_all_inv_match_score values '
        print('# duplicate sets', len(duplicates))

        print('\n\nNow duplicates')

        itr = 1
        cluster_id = 0
        for each_match in duplicates:
            # print each_match
            list_of_uids = list(each_match[0])
            list_of_scores = list(each_match[1])
            cluster_id = cluster_id + 1
            for uid, score in zip(list_of_uids, list_of_scores):
                list_of_values = []
                if itr != 1:
                    query = query + ','
                list_of_values.append(str(cluster_id))
                list_of_values.append(uid)
                list_of_values.append(score)
                query = query + str(tuple(list_of_values))
                itr = itr + 1
        split_query = query.split()
        if split_query[-1] == 'values':
            query = ''
        cur.execute(query)
        #output_list.append(query)

    except Exception as e:
        print('Skipping for therapeutic_area due to some error = ')
        print(str(e))




#def _execute_parallel_mysql_insert_queries():
#    output_list = multiprocessing.Manager().list()
#    process_list = []
#    print('Starting - {}'.format(str(datetime.datetime.now())))
#    count = 0
#    final_therapeutic_area_list = []
#    for therapeutic_area in therapeutic_area_list:
#        final_therapeutic_area_list.append(str(therapeutic_area[0]))
#    counter = 0
#    count_therapeutic_area = 1
#    new_therapeutic_area_list = []
#    for therapeutic_area in final_therapeutic_area_list:
#        if therapeutic_area != None:
#            new_therapeutic_area_list.append(therapeutic_area)
#            if therapeutic_area == 'Oncology':
#                process_object = multiprocessing.Process(target=execute_dedupe,
#                                                         args=(str(therapeutic_area), output_list))
#                process_list.append(process_object)
#                for process_item in process_list:
#                    print('{}: Starting processes '.format(str(datetime.datetime.now())) +
#                          ' for therapeutic_area ' + str(therapeutic_area))
#                    process_item.start()
#
#                for process_item in process_list:
#                    print('{}: Joining processes'.format(str(datetime.datetime.now())) +
#                          ' for therapeutic_area ' + str(therapeutic_area))
#                    process_item.join()
#
#                for query_output in output_list:
#                    if query_output != '':
#                        cur.execute(query_output)
#
#                new_therapeutic_area_list.remove('Oncology')
#                process_list = []
#                output_list = multiprocessing.Manager().list()
#                count_therapeutic_area = count_therapeutic_area - 1
#
#            elif count_therapeutic_area % 5 == 0:
#                counter = counter + 1
#                print('Inside if and count of therapeutic_area = ' + str(count_therapeutic_area))
#                print('therapeutic_area list = ' + str(new_therapeutic_area_list))
#                for therapeutic_area in new_therapeutic_area_list:
#                    process_object = multiprocessing.Process(target=execute_dedupe,
#                                                             args=(str(therapeutic_area), output_list))
#                    process_list.append(process_object)
#
#                for process_item in process_list:
#                    print('{}: Starting processes '.format(str(datetime.datetime.now())) +
#                          ' for therapeutic_area ' + str(therapeutic_area))
#                    process_item.start()
#
#                for process_item in process_list:
#                    print('{}: Joining processes'.format(str(datetime.datetime.now())) +
#                          ' for therapeutic_area ' + str(therapeutic_area))
#                    process_item.join()
#
#                print(str(output_list))
#
#                for query_output in output_list:
#                    if query_output != '':
#                        cur.execute(query_output)
#
#                new_therapeutic_area_list = []
#                process_list = []
#                output_list = multiprocessing.Manager().list()
#
#            count_therapeutic_area = count_therapeutic_area + 1
#        else:
#            pass
#
#    print('therapeutic_area list = ' + str(new_therapeutic_area_list))
#    if counter < len(therapeutic_area_list):
#        for therapeutic_area in new_therapeutic_area_list:
#            process_object = multiprocessing.Process(target=execute_dedupe, args=(str(therapeutic_area),
#                                                                                  output_list))
#            process_list.append(process_object)
#
#        for process_item in process_list:
#            print('{}: Starting processes'.format(str(datetime.datetime.now())))
#            process_item.start()
#
#        for process_item in process_list:
#            print('{}: Joining processes'.format(str(datetime.datetime.now())))
#            process_item.join()
#
#        print(str(output_list))
#
#        for query_output in output_list:
#            if query_output != '':
#                cur.execute(query_output)
#
#    print('Ending - {}'.format(str(datetime.datetime.now())))
#
#
#_execute_parallel_mysql_insert_queries()
execute_dedupe()

timeEnd = datetime.datetime.now()

'''
spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_match_score PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_inv_match_score
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_match_score')
'''

