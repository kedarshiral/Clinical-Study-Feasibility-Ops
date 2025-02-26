

################################# Module Information ######################################
#  Module Name         : ALL Site Dedupe Process
#  Purpose             : This will execute dedupe among all data sources
#  Pre-requisites      : Source table required: temp_all_site_data_prep
#  Last changed on     : 19-01-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Performs Manual dedupe and stores data on HDFS across data sources
############################################################################################

import os
import dedupe
import json
import datetime
import multiprocessing as mp
import multiprocessing.pool
import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from pyhive import hive

timeStart = datetime.datetime.now()
cur = hive.connect(host='localhost', port=10000).cursor()

COUNTRY_KEY = 'country'
settings_file = '/app/clinical_design_center/data_management/sanofi_ctfo/configs/job_executor/' \
                'all_site_dedupe_setting'
training_file = '/app/clinical_design_center/data_management/sanofi_ctfo/configs/job_executor/' \
                'all_site_dedupe_training.json'

cur.execute('drop table if exists temp_all_site_match_score')
cur.execute('create table temp_all_site_match_score (cluster_id string, uid bigint, score double) '
            'stored as parquet')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/site/table_name/pt_data_dt=$$data_dt/'


def func_fuzzy_match_name(field_1, field_2):
    score = fuzz.token_set_ratio(field_1, field_2)
    return score


def func_fuzzy_match_address(field_1, field_2):
    score = fuzz.token_set_ratio(field_1, field_2)
    if score >= 85:
        return score
    else:
        return 0


def cnvrt_to_dict(cur):
    names = [d[0] for d in cur.description]
    # print names
    names_list = []
    for each in names:
        names_list.append(str(each))
    list_dict = []
    for row in cur.fetchall():
        list_dict.append(dict((zip(names_list, row))))

    for each in names:
        # print(list_dict)
        for each_dict in list_dict:
            if not each_dict[each]:
                each_dict[each] = None
            if isinstance(each_dict[each], int):
                pass
            else:
                each_dict[each] = each_dict[each]
    return list_dict

country_list_query = """select distinct country from temp_all_site_data_prep"""
cur = hive.connect(host='localhost', port=10000).cursor()
cur.execute(country_list_query)
country_list = cur.fetchall()
print('country_list = ' + str(country_list))

def execute_dedupe(country, output_list):
    country = country.strip().lower()
    print(country)
    country_data = """select uid, name, city, state, country, zip, address  from
    temp_all_site_data_prep where name is not null and lower(trim(country)) = '""" + country +\
                   """'"""

    cur_start_time = datetime.datetime.now()
    cur = hive.connect(host='localhost', port=10000).cursor()
    print('Established connection for {}'.format(country))
    cur_end_time = datetime.datetime.now()
    print(
        '*****Time taken to establish Hive Connection for Country: {} Start time: {}, '
        'End time: {}'.format(str(country), str(cur_start_time), str(cur_end_time)))
    print('Preparing sample data')
    try:
        data_d = {}
        cur.execute(country_data)
        list_of_dict = cnvrt_to_dict(cur)
        # itr = 1
        for each_dict in list_of_dict:
            # print each_dict
            # data_d[itr] = each_dict
            # itr = itr + 1
            data_d[int(each_dict['uid'])] = each_dict
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
                {'field': 'city', 'variable name': 'city', 'type': 'ShortString',
                 'has missing': True},
                {'field': 'zip', 'variable name': 'zip', 'type': 'Exact', 'has missing': True},
                {'field': 'address', 'variable name': 'address', 'type': 'Custom',
                 'comparator': func_fuzzy_match_address}
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
            '*****TIME Deplication Threshold Calculator Query for Country: {} Start time: {}, '
            'End time: {}'.format(
                str(country),
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
        print('*****TIME De-duplication Match for Country: {} Start time: {}, End time: {}'.
              format(str(country), str(cur_start_time), str(cur_end_time)))

        # print duplicates
        query = 'insert into temp_all_site_match_score values '
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
                list_of_values.append(country + '_' + str(cluster_id))
                list_of_values.append(uid)
                list_of_values.append(score)
                query = query + str(tuple(list_of_values))
                itr = itr + 1
        split_query = query.split()
        if split_query[-1] == 'values':
            query = ''
        output_list.append(query)

    except Exception as e:
        print('Skipping for country due to some error = ' + str(country))
        print(str(e))

def _execute_parallel_mysql_insert_queries():
    output_list = multiprocessing.Manager().list()
    process_list = []
    print('Starting - {}'.format(str(datetime.datetime.now())))
    count = 0
    final_country_list = []
    for country in country_list:
        final_country_list.append(str(country[0]))
    counter = 0
    count_country = 1
    new_country_list = []
    for country in final_country_list:
        if country != None:
            new_country_list.append(country)
            if country == 'united states':
                process_object = multiprocessing.Process(target=execute_dedupe,
                                                         args=(str(country), output_list))
                process_list.append(process_object)
                for process_item in process_list:
                    print('{}: Starting processes '.format(str(datetime.datetime.now())) +
                          ' for country ' + str(country))
                    process_item.start()

                for process_item in process_list:
                    print('{}: Joining processes'.format(str(datetime.datetime.now())) +
                          ' for country ' + str(country))
                    process_item.join()

                for query_output in output_list:
                    if query_output != '':
                        cur.execute(query_output)

                new_country_list.remove('united states')
                process_list = []
                output_list = multiprocessing.Manager().list()
                count_country = count_country - 1

            elif count_country % 5 == 0:
                counter = counter + 1
                print('Inside if and count of country = ' + str(count_country))
                print('country list = ' + str(new_country_list))
                for country in new_country_list:
                    process_object = multiprocessing.Process(target=execute_dedupe,
                                                             args=(str(country), output_list))
                    process_list.append(process_object)

                for process_item in process_list:
                    print('{}: Starting processes '.format(str(datetime.datetime.now())) +
                          ' for country ' + str(country))
                    process_item.start()

                for process_item in process_list:
                    print('{}: Joining processes'.format(str(datetime.datetime.now())) +
                          ' for country ' + str(country))
                    process_item.join()

                print(str(output_list))

                for query_output in output_list:
                    if query_output != '':
                        cur.execute(query_output)

                new_country_list = []
                process_list = []
                output_list = multiprocessing.Manager().list()

            count_country = count_country + 1
        else:
            pass

    print('country list = ' + str(new_country_list))
    if counter < len(country_list):
        for country in new_country_list:
            process_object = multiprocessing.Process(target=execute_dedupe, args=(str(country),
                                                                                  output_list))
            process_list.append(process_object)

        for process_item in process_list:
            print('{}: Starting processes'.format(str(datetime.datetime.now())))
            process_item.start()

        for process_item in process_list:
            print('{}: Joining processes'.format(str(datetime.datetime.now())))
            process_item.join()

        print(str(output_list))

        for query_output in output_list:
            if query_output != '':
                cur.execute(query_output)

    print('Ending - {}'.format(str(datetime.datetime.now())))


_execute_parallel_mysql_insert_queries()

timeEnd = datetime.datetime.now()





