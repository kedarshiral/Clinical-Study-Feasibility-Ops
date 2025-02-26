


################################# Module Information ######################################
#  Module Name         : Trial Dedupe Process
#  Purpose             : This will execute cross dedupe for Trial data
#  Pre-requisites      : Source table required: temp_all_trial_data_prep
#  Last changed on     : 22-03-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Performs Manual dedupe and stores data on HDFS
############################################################################################

import os
import datetime
import json
import multiprocessing
import multiprocessing.pool
import dedupe
import CommonConstants as CommonConstants

from pyhive import hive


PHASE_KEY = 'phase'
timeStart = datetime.datetime.now()


settings_file = CommonConstants.EMR_CODE_PATH+'/configs/job_executor/' \
                'all_trial_dedupe_setting'
training_file = CommonConstants.EMR_CODE_PATH+'/configs/job_executor/' \
                'all_trial_dedupe_training.json'

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


phase_list_query = """select distinct trial_phase from temp_all_trial_data_prep order by 1"""
cur = hive.connect(host='localhost', port=10000).cursor()
cur.execute(phase_list_query)
phase_list = cur.fetchall()
print(phase_list)


def execute_dedupe(input_json):
    """
    Purpose :   This method will execute dedupe for the phase passed as input
    """
    print("Starting execution for JSON - {}".format(json.dumps(input_json)))
    phase = input_json[PHASE_KEY]
    print(phase)
    print("Fetching data from source tables")
    # TODO remove limit in select query before final push to bitbucket
    phase_data = """select uid, lower(trial_title) as title, lower(trial_phase) as phase,
    lower(trial_drug) as trial_drug
    ,lower(trial_indication)  as trial_indication from temp_all_trial_data_prep where
    trim(lower(trial_phase)) in
     ( '""" + phase + """')"""
    print('Opening connection for {}'.format(phase))
    cur_start_time4 = datetime.datetime.now()
    cur = hive.connect(host='localhost', port=10000).cursor()
    print('Established connection for {}'.format(phase))
    cur_end_time4 = datetime.datetime.now()
    print('*****TIME Hive Connection established Phase: {} Start time: {}, End time: {}'.
          format(str(phase), str(cur_start_time4), str(cur_end_time4)))
    print('Preparing sample data')
    try:
        data_d = {}
        print('Executing query - {}'.format(phase_data))
        cur.execute(phase_data)
        print('Executed successfully, query - {}'.format(phase_data))
        list_of_dict = cnvrt_to_dict(cur)
        # itr = 1
        # added strip and split for study indication
        for each_dict in list_of_dict:
            data_d[int(each_dict['uid'])] = each_dict
            if each_dict.get('trial_indication'):
                each_dict['trial_indication'] = tuple(sorted([trial_indication.strip() for trial_indication in each_dict['trial_indication'].split(';') if trial_indication != 'none']))
            if each_dict.get('trial_drug'):
                each_dict['trial_drug'] = tuple(sorted([trial_drug.strip() for trial_drug in each_dict['trial_drug'].split(';') if trial_drug != 'none']))
        print('Sample data prepared')
    except Exception as e:
        print('Error in converting result set to dictionary!')
        print(str(e))

    # If a settings file already exists, we'll just load that and skip training
    try:
        print('Checking if settings file exists')
        if os.path.exists(settings_file):
            print('reading from', settings_file)
            with open(settings_file, 'rb') as f:
                deduper = dedupe.StaticDedupe(f)
        else:
            # ## Training

            # Define the fields dedupe will pay attention to
            fields = fields = [
                {'field': 'title', 'type': 'String'},
                {'field': 'trial_drug', 'type': 'Set', 'has missing': True},
                {'field': 'trial_indication', 'type': 'Set', 'has missing': True}
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
        print("Now calculating threshold!")
        cur_start_time2 = datetime.datetime.now()
        threshold = deduper.threshold(data_d, recall_weight=0.7)
        cur_end_time2 = datetime.datetime.now()

        print(threshold)
        print(
            '*****TIME Deplication Threshold Calculator Query for Phase: {} Start time: {},'
            ' End time: {}'.format(str(phase), str(cur_start_time2), str(cur_end_time2)))

        # ## Clustering

        # `match` will return sets of record IDs that dedupe
        # believes are all referring to the same entity.
        cur_start_time3 = datetime.datetime.now()
        print('clustering...')
        duplicates = deduper.match(data_d, threshold)
        cur_end_time3 = datetime.datetime.now()
        print('*****TIME De-duplication Match for Phase: {} Start time: {}, End time: {}'.
              format(str(phase), str(cur_start_time3), str(cur_end_time3)))
        # print "duplicates: "
        # print duplicates

        print('# duplicate sets', len(duplicates))
        phase_name = phase
        phase_value = phase_name.replace(' ', '_')
        phase_value = phase_value.replace('/', '_')
        print(phase_value)

        cur.execute("drop table if exists temp_all_trial_match_score_" + phase_value)
        cur.execute(
            "create table temp_all_trial_match_score_" + phase_value +
            " (cluster_id bigint, uid bigint, score double) stored as parquet")
        cur_start_time1 = datetime.datetime.now()
        query = "insert into temp_all_trial_match_score_" + phase_value + " values "
        cur_end_time1 = datetime.datetime.now()
        print(
            '*****TIME Insert Trial Match Score results Query for Phase: {} Start time: {}, '
            'End time: {}'.format(str(phase), str(cur_start_time1), str(cur_end_time1)))

        print('\n\nNow duplicates')

        itr = 1
        cluster_id = 0
        for each_match in duplicates:
            # print each_match
            list_of_uids = list(each_match[0])
            list_of_scores = list(each_match[1])
            cluster_id = cluster_id + 1
            # print list_of_uids
            # print list_of_scores
            for uid, score in zip(list_of_uids, list_of_scores):
                list_of_values = []
                if itr != 1:
                    query = query + ","
                list_of_values.append(cluster_id)
                list_of_values.append(uid)
                list_of_values.append(score)
                query = query + str(tuple(list_of_values))
                itr = itr + 1

        cur.execute(query)
        cur_start_time = datetime.datetime.now()
        cur_end_time = datetime.datetime.now()
        print('*****TIME Insert Dedup results Query for Phase: {} Start time: {}, End time: {}'
              .format(str(phase), str(cur_start_time), str(cur_end_time)))
        print('Execution completed for {}'.format(phase))
    except Exception as e:
        print('Skipping for phase due to some error = ' + str(phase))
        print(str(e))

#############Added code for multiprocessing
import multiprocessing

class TestNoDaemonProcess(multiprocessing.Process):
    # To make 'daemon' attribute always return False.
    # We are overriding Multiprocessing Process class with some more properties.
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class TestPool(multiprocessing.pool.Pool):
    Process = TestNoDaemonProcess

thread_list = []
for phase_item in phase_list:
    print(phase_item[0])
    if phase_item[0].lower() == 'other':
        input_json = {"phase": phase_item[0].lower()}
        p1 = multiprocessing.Process(target=execute_dedupe, args=(input_json,))
        p1.start()
    else:
        input_json = {"phase": phase_item[0].lower()}
        print('Creating threads for {}'.format(json.dumps(input_json)))
        thread_list.append(input_json)
print(thread_list)
p1.join()

pool = TestPool(processes=4)
print('Starting Multiprocessing for executing for all Phases.')
pool.map(execute_dedupe, thread_list)

pool.close()
pool.join()

# create final table for scores
cur.execute("create table if not exists temp_all_trial_match_score"
            " (cluster_id string, uid bigint, score double) "
            "stored as parquet ")

for phase_item in phase_list:
    phase_value = phase_item[0].lower()
    phase_value = phase_value.replace(' ', '_')
    phase_value = phase_value.replace('/', '_')
    print("Inserting values for: {}".format(phase_value))
    # insert into scores table
    try:
        score_insert_query = "insert into temp_all_trial_match_score select concat('" \
                            + phase_value + "_',cast(cluster_id as string))" \
                                            " as cluster_id, uid, score from " \
                                            "temp_all_trial_match_score_" + phase_value
        print("Executing query: {}".format(score_insert_query))
        cur.execute(score_insert_query)
    except Exception as e:
        print("Skipping for phase due to some error = " + str(phase_value))
        print(str(e))


timeEnd = datetime.datetime.now()
print("Start time: {}, End time: {}".format(str(timeStart), str(timeEnd)))

'''
spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_match_score PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_trial_match_score
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_match_score')
'''

