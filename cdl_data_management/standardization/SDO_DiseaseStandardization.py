import re
import time
import pandas as pd
import numpy as np
from sparse_dot_topn import awesome_cossim_topn
import warnings
import logging
import sys
import json
import os

from datetime import date, datetime

from ftfy import fix_text
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

logging.info("Downloading stopwords..")
try:
    nltk.download('stopwords')
    nltk.download('punkt')
except Exception as e:
    print(e)

# Getting stopwords for english language
stop_words = set(stopwords.words('english'))


class DiseaseStandardization:

    def removeStopwords(self, disease):
        if len(disease) >= 30:
            word_tokens = word_tokenize(disease)
            filtered_sentence = [w for w in word_tokens if not w in stop_words]

            filtered_sentence = []

            for w in word_tokens:
                if w not in stop_words:
                    filtered_sentence.append(w)

            return (' '.join(filtered_sentence))

        return disease

    def combine_primary_syn(self, df):

        """
        Combining base disease standard file primary name and synonyms into a single column for mapping
        """

        logging.info(
            "Number of rows before combining data: {}\nNumber of columns before combining data: {}".format(df.shape[0],
                                                                                                           df.shape[1]))

        ctln_df2_one = pd.concat([df['diseaseprimaryname_processed'], df['diseasenamesynonym_processed']], axis=0)

        ctln_df2_one_df = pd.DataFrame(ctln_df2_one, columns=['processed'])

        logging.info("Number of rows after combining data: {}\nNumber of columns after combining data: {}".format(
            ctln_df2_one_df.shape[0], ctln_df2_one_df.shape[1]))

        return ctln_df2_one_df

    def ngrams(self, string, n=3):

        """
        returns n-grams of input disease after some preprocessing on it

        Parameters:
        string : input disease
        n : n is value ngrams in which text would be breaken into. n=3 means 3-grams

        """

        string = fix_text(string)  # fix text
        string = string.encode("ascii", errors="ignore").decode()  # remove non ascii chars
        string = string.lower()
        chars_to_remove = [")", "(", ".", "|", "[", "]", "{", "}", "'", "=",
                           "?"]  # specify chars you want to remove here
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        string = re.sub(rx, '', string)
        string = string.replace('&', 'and')
        string = string.replace(',', ' ')
        string = string.replace('-', ' ')
        string = string.title()  # normalise case - capital at start of each word
        string = re.sub(' +', ' ', string).strip()  # get rid of multiple spaces and replace with a single
        string = ' ' + string + ' '  # pad disease names for ngrams
        ngrams = zip(*[string[i:] for i in range(n)])
        return [''.join(ngram) for ngram in ngrams]

    def cossim_top(self, A, B, ntop, lower_bound=0):

        """
        returns the cosine similarity matrix between two matrices

        Parameters:

        A : input matrix 1
        B : input matrix 2 (transpose of A in case mapping against same input)
        ntop : return top n records based on similarity score
        lower_bound : set threshold for scores. values having score less than this would be discarded
        """

        return awesome_cossim_topn(A, B, ntop, lower_bound)

    def get_matches_df(self, sparse_matrix, input_matrix, std_matrix):

        """
        returns a pandas dataframe with columns input disease, standard disease name and their matching score

        Parameters:

        sparse_matrix : matrix having cosine similarity scores calculated using cossim_top function
        input_matrix : input disease data
        std_matrix : standard disease data

        """
        non_zeros = sparse_matrix.nonzero()

        sparserows = non_zeros[0]
        sparsecols = non_zeros[1]

        nr_matches = sparsecols.size
        nr_matches1 = sparserows.size

        left_side = np.empty([nr_matches], dtype=object)
        right_side = np.empty([nr_matches], dtype=object)
        similarity = np.zeros(nr_matches)
        ctln_idx = np.zeros(nr_matches, dtype=np.int)
        input_idx = np.zeros(nr_matches1, dtype=np.int)

        for index in range(0, nr_matches):
            left_side[index] = input_matrix[sparserows[index]]
            right_side[index] = std_matrix[sparsecols[index]]
            similarity[index] = sparse_matrix.data[index]
            ctln_idx[index] = sparsecols[index]
            input_idx[index] = sparserows[index]

        return pd.DataFrame({'map_src_idx': ctln_idx,
                             'map_to_idx': input_idx,
                             'Input Disease': left_side,
                             'Standard Disease': right_side,
                             'similarity': similarity})

    def match(self, df, df_clean, input_col):
        """
        input_col is column from input dataframe

        returns dataframe after calculating similarity between two input columns
        """
        if input_col != "processed":
            df.rename(columns={input_col: 'processed'}, inplace=True)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['processed'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        # Calculating the cosine similarity between the two matrices features
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)

        df.reset_index(inplace=True, drop=True)
        df_clean.reset_index(inplace=True, drop=True)

        matches_df = self.get_matches_df(matches, df['processed'], df_clean['processed'])

        return matches_df

    # len diff <=1-2 can also be added in condition to check for spelling mistakes
    def rem_dup(self, s, delimiter):

        """
        removes duplicates from delimiter separated values in a column
        """

        all_s = str(s).split(delimiter)
        processed_s = []
        unique_s = []

        for val in all_s:
            temp_s = re.sub('[^\w]+', '', val).lower().strip()
            if temp_s not in processed_s:
                processed_s.append(temp_s)
                unique_s.append(val)

        if len(unique_s) > 1:
            return delimiter.join(unique_s)
        elif len(unique_s) == 1:
            return ''.join(unique_s)
        else:
            return ''

    def syn_to_prime_idx_mapping(self, df):
        """
        creating mapping between primary and synonym index of base disease standard file
        """
        mapping = {}
        for prime_idx, syn_idx in zip(df['map_src_idx'], df['syn_index']):
            mapping[syn_idx] = prime_idx

        return mapping

    def explode_str(self, df, col, sep):
        s = df[col]
        i = np.arange(len(s)).repeat(s.str.count(sep) + 1)
        return df.iloc[i].assign(**{col: sep.join(s).split(sep)})

    def get_org_cols(self, df, input_df, std_df):

        """
        get original input drug column and ctln df index

        df: disease source similarity dataframe
        input_df: aact or dqs drugs dataframe
        std_df: base std df

        """
        ## can add here code to trim spaces on indexes columns

        # getting original disease name as similarity out2 df contains processed diseases
        # df_new = pd.merge(df, input_df[['drug_name', 'ID', 'datasource', 'nct_id']], how='left', left_on=['input_idx'], right_on=['ID'])

        # fixed some missing input drugs value here by keeping input df on left
        df_new = pd.merge(input_df[['map_to_idx', 'disease_name', 'datasource', 'trial_id']], df, how='left',
                          on=['map_to_idx'])

        map_dict = self.syn_to_prime_idx_mapping(std_df)
        df_new['idx'] = df_new['map_src_idx'].map(map_dict).fillna(df_new['map_src_idx'])

        return df_new

    def get_mapping_file(self, df, base_std_df, THRESHOLD):

        """
                input: load combined nlp, citeline  data
                purpose: process different data sources data and combine the output for a single mapping file
                returns final mapping file
                """

        logging.info("Cleaning data...")

        base_std_df['diseaseprimaryname_processed'] = base_std_df['standard_disease'].apply(
            lambda x: re.sub('[^\w]+', ' ', x).strip())
        base_std_df['diseasenamesynonym_processed'] = base_std_df['disease'].apply(
            lambda x: re.sub('[^\w]+', ' ', x).strip())

        logging.info("Removing stopwords...")

        base_std_df['diseaseprimaryname_processed'] = base_std_df['diseaseprimaryname_processed'].apply(
            lambda x: self.removeStopwords(x).strip())
        base_std_df['diseasenamesynonym_processed'] = base_std_df['diseasenamesynonym_processed'].apply(
            lambda x: self.removeStopwords(x).strip())

        base_std_df.drop_duplicates(subset=['diseasenamesynonym_processed', 'diseasenamesynonym_processed'],
                                    inplace=True)

        # adding primary and syn index
        base_std_df.insert(0, "map_src_idx", range(0, 0 + len(base_std_df)))

        # syn index col is used to map from synonym to its primary name as we combine both below
        base_std_df.insert(1, "syn_index", range(len(base_std_df), len(base_std_df) + len(base_std_df['disease'])))

        logging.info("Combining primary and synonym disease names...")
        df_clean = self.combine_primary_syn(base_std_df)

        df.dropna(subset=['disease_name'], inplace=True)

        # # Filtering data for aact datasource
        # aact_df = df[df['datasource'] == 'aact']
        #
        # logging.info("Processing AACT data...")
        # # processing aact df
        #
        # aact_df['processed'] = aact_df['disease_name'].apply(lambda x: re.sub('[^\w]+', ' ', x).strip())
        # aact_df['processed'] = aact_df['processed'].apply(lambda x: self.removeStopwords(x))
        # aact_df.insert(0, "map_to_idx", range(0, 0 + len(aact_df)))
        #
        # df1 = aact_df.copy()
        #
        # logging.info("Creating mapping for aact data...")
        #
        # output_aact = self.match(df1, df_clean, "processed")
        # aact_mapping = self.get_org_cols(output_aact, aact_df, base_std_df)

        # Filtering data for nlp datasource
        nlp_df = df[df['datasource'] == 'nlp']

        logging.info("Processing nlp data...")
        # processing aact df

        nlp_df['processed'] = nlp_df['disease_name'].apply(lambda x: re.sub('[^\w]+', ' ', x).strip())
        nlp_df['processed'] = nlp_df['processed'].apply(lambda x: self.removeStopwords(x))
        nlp_df.insert(0, "map_to_idx", range(0, 0 + len(nlp_df)))

        df1 = nlp_df.copy()

        logging.info("Creating mapping for nlp data...")

        output_nlp = self.match(df1, df_clean, "processed")
        nlp_mapping = self.get_org_cols(output_nlp, nlp_df, base_std_df)

        # Filtering data for aact datasource
        # dqs_df = df[df['datasource'] == 'dqs']
        #
        # logging.info("Processing DQS data...")

        # processing dqs df

        # dqs_df['processed'] = dqs_df['disease_name'].apply(lambda x: re.sub('[^\w]+', ' ', x).strip())
        # dqs_df['processed'] = dqs_df['processed'].apply(lambda x: self.removeStopwords(x))
        # dqs_df.insert(0, "map_to_idx", range(0, 0 + len(dqs_df)))
        #
        # df2 = dqs_df.copy()
        #
        # logging.info("Creating mapping for dqs data...")

        # output_dqs = self.match(df2, df_clean, "processed")
        # dqs_mapping = self.get_org_cols(output_nlp, nlp_df, base_std_df)

        logging.info("Processing CITELINE data...")

        # Filtering data for aact datasource
        ctln_df = df[df['datasource'] == 'citeline']

        ctln_df['processed'] = ctln_df['disease_name'].apply(lambda x: re.sub('[^\w]+', ' ', x).strip())
        ctln_df['processed'] = ctln_df['processed'].apply(lambda x: self.removeStopwords(x))
        ctln_df.insert(0, "map_to_idx", range(0, 0 + len(ctln_df)))

        df3 = ctln_df.copy()

        logging.info("Creating mapping for citeline data...")

        output_ctln = self.match(df3, df_clean, "processed")
        ctln_mapping = self.get_org_cols(output_ctln, ctln_df, base_std_df)

        logging.info("Combining mapping from all sources...")
        # append mappings from citeline, aact and dqs
        final_mapping = ctln_mapping.append([nlp_mapping], ignore_index=True)

        final_mapping = final_mapping[final_mapping['similarity'] >= THRESHOLD].copy()

        final_mapping.drop(columns=['map_to_idx', 'map_src_idx', 'Standard Disease'], inplace=True)

        final_mapping_new = pd.merge(base_std_df, final_mapping, how='left', left_on=['map_src_idx'], right_on=['idx'])

        final_mapping_new.drop(
            columns=['map_src_idx', 'syn_index', 'diseaseprimaryname_processed', 'diseasenamesynonym_processed', 'idx'],
            inplace=True)

        final_mapping_new.fillna('', inplace=True)

        final_mapping_new['Synonyms'] = final_mapping_new.apply(
            lambda x: x['disease'] + '|' + x['disease_name'] if x['disease_name'] != '' and x['disease_name'] != x[
                'disease'] else x['disease'], axis=1)

        final_mapping_new.drop(columns=['Input Disease'], inplace=True)
        final_mapping_new.rename(columns={'disease_name': 'Input_disease'}, inplace=True)

        # final_mapping_new.reset_index(drop=True, inplace=True)

        # logging.info("Number of rows before exploding: %d", final_mapping_new.shape[0])
        # final_mapping_exploded = self.explode_str(final_mapping_new, "Synonyms", "|")
        # logging.info("Number of rows after exploding: %d", final_mapping_exploded.shape[0])

        # final_mapping.rename(columns=[], inplace=True)

        return final_mapping_new

    def load_json(self, file_path):

        """
        This function is used to load config json having location info and parameters
        """
        with open(file_path, 'r') as config_file:
            config_data = json.load(config_file)

        return config_data

    def main(self):

        initial_time = time.time()
        # /clinical_design_center/data_management/cdl_sdo/dev/code/disease_standardization_config.json
        disease_std_config_file_location = "disease_standardization_config.json"

        '''

        if disease_std_config_file_location is None:
            if len(sys.argv)<2:
                logging.info("config file location not passed as an argument!")
                sys.exit(1)

            disease_std_config_file_location = sys.argv[1]

        '''

        # config_data = Disease_std.load_json(disease_std_config_file_location)
        with open(disease_std_config_file_location, 'r') as config_file:
            config_data = json.load(config_file)
        logging.info("config json loaded successfully!")

        logging.info("Starting  main method...")

        exploded_file = '/clinical_design_center/data_management/cdl_sdo/dev/code/exploded.csv'
        synonym_file = '/clinical_design_center/data_management/cdl_sdo/dev/code/std_syn_one.csv'
        delta_file = '/clinical_design_center/data_management/cdl_sdo/dev/code/delta.csv'

        if os.path.exists(exploded_file):
            os.system('rm ' + exploded_file)
        os.system('aws s3 cp s3://aws-a0220-use1-00-q-s3b-shrd-shr-data01/clinical-data-lake'
                  '/uploads/'
                  'EXPLODED_FILE/exploded.csv ./')

        if os.path.exists(synonym_file):
            os.system('rm ' + synonym_file)
        os.system('aws s3 cp s3://aws-a0220-use1-00-q-s3b-shrd-shr-data01/clinical-data-lake'
                  '/uploads/'
                  'SYNONYM_FILE/std_syn_one.csv ./')

        if os.path.exists(delta_file):
            os.system('rm ' + delta_file)

        os.system('rm -r /clinical_design_center/data_management/cdl_sdo/dev/code/Delta')
        os.system('hadoop fs -copyToLocal /user/hive/warehouse/Delta '
                  '/clinical_design_center/data_management/cdl_sdo/dev/code/')

        os.system('mv /clinical_design_center/data_management/cdl_sdo/dev/code/Delta/*.csv '
                  '/clinical_design_center/data_management/cdl_sdo/dev/code/delta.csv')

        THRESHOLD = config_data["parameters"]["set_mapping_threshold_score"]

        '''

        logging.info("Downloading stopwords..")
        try:
            nltk.download('stopwords')
            nltk.download('punkt')
        except Exception as e:
            print(e)

        '''
        # ctd exploded
        base_std_df = pd.read_csv("exploded.csv")

        # base_std_df.rename(columns={'standard_disease': 'DiseaseName', 'disease': 'Synonyms'}, inplace=True)
        # os.chdir("/clinical_design_center/data_management/cdl_sdo/dev/code/")
        disease_df = pd.read_csv("delta.csv", error_bad_lines=False)

        mapping_df = self.get_mapping_file(disease_df, base_std_df, THRESHOLD)

        logging.info("Writing mapping file to CSV...")

        # current_timestamp = str(datetime.today()).replace(':', '-').split('.')[0]
        mapping_df.to_csv("disease_mapping.csv", index=False)

        os.system(
            'aws s3 cp disease_mapping.csv s3://aws-a0220-use1-00-q-s3b-shrd-shr-data01/clinical-data-lake/uploads/SDO_UPLOADS/FINAL_DISEASE_MAPPING/')
        end_time = time.time()

        logging.info("Executed in {} minutes".format(round((end_time - initial_time) / 60, 2)))


if __name__ == '__main__':
    Disease_std = DiseaseStandardization()

    Disease_std.main()
