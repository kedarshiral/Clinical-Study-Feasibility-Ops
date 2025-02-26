


################################# Module Information ######################################
#  Module Name         : Investigator Dedupe Training Generation
#  Purpose             : This will generate training file and train modeule to get results for
# clustering
#  Pre-requisites      : data preparation table = temp_all_inv_data_prep
#  Last changed on     : 22-03-2021
#  Last changed by     : Himanshi
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


study_select = """select uid, concat(coalesce(lower(trim(split(name,' ')[0])),''),' ',
 coalesce(lower(trim(reverse(split(reverse(name),' ')[0]))),'')) as name, lower(email) as email
 ,phone from temp_all_inv_data_prep where coalesce(lower(name),'na') not in ('na','','null')
 and name !=''"""
cur = hive.connect(host='localhost', port=10000).cursor()


settings_file = '/app/clinical_design_center/data_management/sanofi_ctfo/configs/job_executor/' \
                'all_inv_dedupe_setting'
training_file = '/app/clinical_design_center/data_management/sanofi_ctfo/configs/job_executor/' \
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


def func_fuzzy_match_name(field_1, field_2):
    score = fuzz.WRatio(field_1, field_2)
    return score
    # if score >= 85:
    #     return 1
    # else:
    #     return 0

try:
    data_d = {}
    cur.execute(study_select)
    list_of_dict = cnvrt_to_dict(cur)
    for each_dict in list_of_dict:
        if each_dict.get('phone'):
            each_dict['phone'] = tuple(sorted([phone.strip() for phone in
                                               each_dict['phone'].split('|') if phone != 'none']))
        if each_dict.get('email'):
            each_dict['email'] = tuple(sorted([email.strip() for email in
                                               each_dict['email'].split('|') if email != 'none']))
        data_d[int(each_dict['uid'])] = each_dict
    print(data_d)
except Exception as e:
    print("Error in converting result set to dictionary!")
    print(str(e))

# If a settings file already exists, we'll just load that and skip training
if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)
else:
    fields = [
        {'field': 'name', 'variable name': 'name', 'type': 'Custom',
         'comparator': func_fuzzy_match_name},
        {'field': 'email', 'variable name': 'email', 'type': 'Set', 'has missing': True},
        {'field': 'phone', 'variable name': 'phone', 'type': 'Set', 'has missing': True},
        {'type': 'Interaction', 'interaction variables': ['name', 'phone']},
        {'type': 'Interaction', 'interaction variables': ['name', 'email']}
    ]
    deduper = dedupe.Dedupe(fields)
    deduper.sample(data_d, 10000)
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file, 'rb') as f:
            deduper.readTraining(f)
    print('starting active labeling...')
    dedupe.consoleLabel(deduper)
    deduper.train()
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)
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
threshold = deduper.threshold(data_d, recall_weight=0.7)
print(threshold)
# ## Clustering

# `match` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print("clustering...")
duplicates = deduper.match(data_d, threshold)

# print "duplicates: "
# print duplicates

print("# duplicate sets", len(duplicates))

cur.execute('drop table if exists temp_all_inv_match_score')
cur.execute('create table temp_all_inv_match_score (cluster_id string, uid bigint, '
            'score double) stored as parquet')

query = "insert into temp_all_inv_match_score values "

print("\n\nNow duplicates")

itr = 1
cluster_id = 0
for each_match in duplicates:
    list_of_uids = list(each_match[0])
    list_of_scores = list(each_match[1])
    cluster_id = cluster_id + 1
    for uid, score in zip(list_of_uids, list_of_scores):
        list_of_values = []
        if itr != 1:
            query = query + ','
        list_of_values.append(cluster_id)
        list_of_values.append(uid)
        list_of_values.append(score)
        query = query + str(tuple(list_of_values))
        itr = itr + 1

cur.execute(query)







