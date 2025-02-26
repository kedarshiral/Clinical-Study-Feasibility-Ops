

## some values might miss out due to parsing errors of CSV, so load dataframe as parquet

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
import pickle
import numpy as np
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from datetime import date, datetime
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
client_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "client_name"])
dags/plugins_path = CommonConstants.EMR_CODE_PATH


# from keras.models import load_model
# from keras.preprocessing.sequence import pad_sequences

from ftfy import fix_text
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)



# Loading tokenizer
# with open("tokenizer.pickle", "rb") as file:
#     tokenizer= pickle.load(file)


# # Loading model
# logging.info("loading model...")
# model=load_model("model.h5")


class SponsorStandardization:

    def ngrams(self, string, n=3):

        """
        returns n-grams of input disease after some preprocessing on it

        Parameters:
        string : input disease
        n : n is value ngrams in which text would be breaken into. n=3 means 3-grams

        """

        string = fix_text(string) # fix text
        string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
        string = string.lower()
        chars_to_remove = [")","(",".","|","[","]","{","}","'","=","?"] #specify chars you want to remove here
        rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
        string = re.sub(rx, '', string)
        string = string.replace('&', 'and')
        string = string.replace(',', ' ')
        string = string.replace('-', ' ')
        string = string.title() # normalise case - capital at start of each word
        string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
        string = ' '+ string +' ' # pad disease names for ngrams
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

        return awesome_cossim_topn(A, B ,ntop, lower_bound)

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
        nr_matches1 =sparserows.size

        left_side = np.empty([nr_matches1], dtype=object)
        right_side = np.empty([nr_matches], dtype=object)
        similarity = np.zeros(nr_matches)
        ctln_idx=np.zeros(nr_matches, dtype=np.int)
        input_idx=np.zeros(nr_matches1, dtype = np.int)

        for index in range(0, nr_matches):
            left_side[index] = input_matrix[sparserows[index]]
            right_side[index] = std_matrix[sparsecols[index]]
            similarity[index] = sparse_matrix.data[index]
            ctln_idx[index] = sparsecols[index]
            input_idx[index] = sparserows[index]

        return pd.DataFrame({'map_src_idx': ctln_idx,
                             'map_to_idx': input_idx,
                             'Input sponsor': left_side,
                             'Standard sponsor': right_side,
                             'similarity': similarity})


    def match(self, df, df_clean, input_col):

        """
        input_col is column from input dataframe

        returns dataframe after calculating similarity between two input columns
        """
        if input_col!= "processed":
            df.rename(columns={input_col : 'processed'}, inplace=True)

        # Creating TF-IDF matrix for input data and standard-synonym data
        vectorizer = TfidfVectorizer(analyzer=self.ngrams)
        tf_idf_matrix_clean = vectorizer.fit_transform(df_clean['processed'])
        tf_idf_matrix_dirty = vectorizer.transform(df['processed'])

        logging.info("Calculating similarity scores...")

        # Calculating the cosine similarity between the two matrices features
        matches = self.cossim_top(tf_idf_matrix_dirty, tf_idf_matrix_clean.transpose(), 1, 0)

        df.reset_index(inplace=True, drop=True)
        df_clean.reset_index(inplace=True, drop=True)


        matches_df = self.get_matches_df(matches, df['processed'], df_clean['processed'])

        return matches_df

    def get_org_cols(self, df, input_df, std_df):

            """
            get original input drug column and ctln df index

            df: disease source similarity dataframe
            input_df: aact or dqs drugs dataframe
            std_df: base std df

            """
            ## can add here code to trim spaces on indexes columns

            # fixed some missing input drugs value here by keeping input df on left

            logging.info("Fetching original columns...")
            df_new = pd.merge(input_df, df, how='left', on=['map_to_idx'])


            return df_new

    def get_mapping_file(self, df, base_std_df, THRESHOLD):

        """
        input: load combined aact, citeline and dqs diseases data
        purpose: process different data sources data and combine the output for a single mapping file
        returns final mapping file
        """

        logging.info("Cleaning data...")

        base_std_df.dropna(subset=['sponsor_processed'], inplace=True)

        base_std_df['processed'] = base_std_df['sponsor_processed'].apply(lambda x: re.sub('[^\w]+', ' ', x).strip())

        base_std_df.drop_duplicates(subset=['processed'], inplace=True)

        ## adding primary index

        base_std_df.insert(0, "map_src_idx", range(0, 0+len(base_std_df)))

        #df.dropna(subset=['disease_name'], inplace=True)

        logging.info("Processing AACT data...")
        # processing aact df

        df['processed']=df['sponsor_processed'].apply(lambda x: re.sub('[^\w]+', ' ', x).strip())
        df.drop_duplicates(subset=['processed'], inplace=True)
        df.insert(0, "map_to_idx", range(0, 0+len(df)))

        df1 = df.copy()
        df_clean = base_std_df.copy()

        logging.info("Creating mapping for aact data...")

        output_aact = self.match(df1, df_clean, "processed")
        aact_mapping = self.get_org_cols(output_aact, df, base_std_df)

        return aact_mapping


    def decode_label(self, score):
        """
        threshold score is used here to label input as investigator or not
        """
        return "inv name" if score < 0.5 else "not inv name"



    def predict(self, text):

        """
        returns prediction score of whether given input value is investigator or not
        """

        SEQUENCE_LENGTH=150

        start_at = time.time()
        # Tokenize text
        x_test = pad_sequences(tokenizer.texts_to_sequences([text]), maxlen=SEQUENCE_LENGTH)
        # Predict
        score = model.predict([x_test])[0]
        # Decode label
        label = self.decode_label(score)

        return float(score)

    def explode(self,df, lst_cols, fill_value=''):
        # make sure `lst_cols` is a list
        if lst_cols and not isinstance(lst_cols, list):
            lst_cols = [lst_cols]
        # all columns except `lst_cols`
        idx_cols = df.columns.difference(lst_cols)

        # calculate lengths of lists
        lens = df[lst_cols[0]].str.len()

        if (lens > 0).all():
            # ALL lists in cells aren't empty
            return pd.DataFrame({
                col: np.repeat(df[col].values, df[lst_cols[0]].str.len())
                for col in idx_cols
            }).assign(**{col: np.concatenate(df[col].values) for col in lst_cols}) \
                       .loc[:, df.columns]
        else:
            # at least one list in cells is empty
            return pd.DataFrame({
                col: np.repeat(df[col].values, df[lst_cols[0]].str.len())
                for col in idx_cols
            }).assign(**{col: np.concatenate(df[col].values) for col in lst_cols}) \
                       .append(df.loc[lens == 0, idx_cols]).fillna(fill_value) \
                       .loc[:, df.columns]


    def main(self):

        initial_time = time.time()
        configuration=JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH+'/'+CommonConstants.ENVIRONMENT_CONFIG_FILE)
        s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,"s3_bucket_name"])

        delta_file = (CommonConstants.EMR_CODE_PATH+'/code/rem_sponsor_withtype_exploded.csv'.replace("$$client_name", client_name))

        if os.path.exists(delta_file):
            os.system('rm ' + delta_file)
        os.system('rm -r /code/rem_sponsor_processed'.replace("$$client_name", client_name))
        os.system('hadoop fs -copyToLocal /user/hive/warehouse/rem_sponsor_processed '
                  '$$dags/plugins_path/code/'.replace("$$dags/plugins_path", dags/plugins_path))

        os.system('mv $$dags/plugins_path/code/rem_sponsor_processed/*.csv '
                  '$$dags/plugins_path/code/rem_sponsor_processed.csv'.replace("$$dags/plugins_path", dags/plugins_path))

        rem_sponsor_df = pd.read_csv("rem_sponsor_processed.csv", error_bad_lines=False)

        # splitting on curly braces and creating and array
        rem_sponsor_df['n'] = rem_sponsor_df.apply(lambda x: list(
            [sponsor.strip().strip('{,}') for sponsor in x['sponsor_processed'].split('{')]) if x[
                                                                                                    'sponsor_processed'].find(
            '{') != -1 else list(x['sponsor_processed'].split("{")), axis=1)

        rem_sponsor_df['sponsor_nm_new'] = rem_sponsor_df['n']

        # exploding the array
        rem_sponsor_df = self.explode(rem_sponsor_df, lst_cols=list('n'))

        rem_sponsor_df = rem_sponsor_df.rename(columns={'n': 'sponsor_exploded'})

        # Filling NaN values so re.sub doesn't give errors of not str like object
        rem_sponsor_df.fillna('', inplace=True)

        # Removing keywords
        rem_sponsor_df['sponsor_processed'] = rem_sponsor_df['sponsor_exploded'].apply(lambda x: re.sub('\s+',
                                                                                         ' ', re.sub(r"H??pital|Hospital|University|Research|Inc.|Medical|Health|Center|Institute|Foundation|Pharmaceuticals|Co.|Ltd.|Pharma|Therapeutics|Pharmaceutical|Centre|Group|Medicine|National|Ltd|GmbH|Clinical|department|MD|LLC|Inc|Trust|Dr.|International|Sciences|Care|Association|College|DEVELOPMENT|Society|Corporation|Limited|Technology|General|Science|M.D.|S.A.|Healthcare|Services|Network|Children's|Institut|Clinic|Technologies|People's|Company|Biotech|Central|Laboratories|Ministry|School|Hospitals|City|PhD|Education|recherche|Instituto|affiliated|Study|Unit|UniversitÃ¤t|Fund|State|Council|Investigaci??n|Public|Dr|Disease|Heart|Community|Prof.|BioPharma|FundaciÃ³n|Hospitalier|Biotechnology|Centro|Royal|Medizinische|Life|Diseases|Universitario|Biomedical|Trials|Associates|InvestigaciÃ³n|Child|Service|Universit??|Family|Rehabilitation|Systems|Global|Laboratory|Grupo|Innovation|Control|Social|Consortium|Universidad|District|Region|subsidiary|Scientific", ' ', x, flags=re.IGNORECASE)).strip() )
        rem_sponsor_df['sponsor_processed'] = rem_sponsor_df['sponsor_processed'].apply(lambda x: re.sub('\s+', ' ', re.sub(r"Co Ltd| Co., Ltd.,| Co.,Ltd.| Pvt. Ltd.| Co. Ltd.| Co., Ltd.| A/S| S.L.| SL| Co.|Ltd.|, Ltd.| Ltd.| Ltd|, LLC| LLC.| LLC|, Inc.| PhD |Inc.| Inc| Plc.| L.L.C.| S.A.S.| SAS| SA| S.A.|  Corporation| Limited|,| Therapeutics| Corp.", ' ', x, flags=re.IGNORECASE)).strip() )

        # Separating aact and citeline sponsor data
        ctln_df = rem_sponsor_df[rem_sponsor_df['source']=="citeline"].copy()
        aact_df = rem_sponsor_df[rem_sponsor_df['source']=="aact"].copy()

        # Reading file having top pharma sponsor names
        os.system('aws s3 cp s3://{}/clinical-data-lake/uploads/sponsor_mapping.csv ./'.format(s3_bucket_name))
        std_top50 = pd.read_csv("sponsor_mapping.csv", error_bad_lines=False,encoding="ISO-8859-1")
        std_top50 = std_top50[std_top50['standard_name']!="Other"].copy()

        # splitting on curly braces and creating and array
        std_top50['n'] = std_top50.apply(lambda x: list(
            [sponsor.strip().strip('{,}') for sponsor in x['sponsor_nm'].split('{')]) if
        x['sponsor_nm'].find('{') != -1 else list(x['standard_name'].split("{")), axis=1)


        # exploding the array
        std_top50_exploded = self.explode(std_top50, lst_cols=list('n'))
        std_top50_exploded = std_top50_exploded.rename(columns={'n': 'standard_nm_new'})
        std_top50_exploded.drop_duplicates(subset=['sponsor_nm', 'standard_nm_new'], inplace=True)

        # extracting top 50 sponsor list
        top50_list = std_top50_exploded.standard_nm_new.unique().tolist()

        # Creating new dataframe with top pharma sponsors
        top50_tomerge = pd.DataFrame(top50_list, columns=['sponsor'])
        top50_tomerge['source'] = "citeline"
        top50_tomerge['sponsor_type'] = "Industry"
        top50_tomerge['sponsor_exploded'] = top50_tomerge['sponsor']

        # Processing and removing keywords from top pharma sponsor names
        top50_tomerge['sponsor_processed'] = top50_tomerge['sponsor'].apply(lambda x: re.sub('\s+', ' ', re.sub(r"Co Ltd| Co., Ltd.,| Co.,Ltd.| Pvt. Ltd.| Co. Ltd.| Co., Ltd.| A/S| S.L.| SL| Co.|Ltd.|, Ltd.| Ltd.| Ltd|, LLC| LLC.| LLC|, Inc.| PhD |Inc.| Inc| Plc.| L.L.C.| S.A.S.| SAS| SA| S.A.|  Corporation| Limited|,| Therapeutics| Corp.", ' ', x, flags=re.IGNORECASE)).strip() )
        top50_tomerge['sponsor_processed'] = top50_tomerge['sponsor_processed'].apply(lambda x: re.sub('\s+', ' ', re.sub(r"H??pital|Hospital|University|Research|Inc.|Medical|Health|Center|Institute|Foundation|Pharmaceuticals|Co.|Ltd.|Pharma|Therapeutics|Pharmaceutical|Centre|Group|Medicine|National|Ltd|GmbH|Clinical|department|MD|LLC|Inc|Trust|Dr.|International|Sciences|Care|Association|College|DEVELOPMENT|Society|Corporation|Limited|Technology|General|Science|M.D.|S.A.|Healthcare|Services|Network|Children's|Institut|Clinic|Technologies|People's|Company|Biotech|Central|Laboratories|Ministry|School|Hospitals|City|PhD|Education|recherche|Instituto|affiliated|Study|Unit|UniversitÃ¤t|Fund|State|Council|Investigaci??n|Public|Dr|Disease|Heart|Community|Prof.|BioPharma|FundaciÃ³n|Hospitalier|Biotechnology|Centro|Royal|Medizinische|Life|Diseases|Universitario|Biomedical|Trials|Associates|InvestigaciÃ³n|Child|Service|Universit??|Family|Rehabilitation|Systems|Global|Laboratory|Grupo|Innovation|Control|Social|Consortium|Universidad|District|Region|subsidiary|Scientific", ' ', x, flags=re.IGNORECASE)).strip() )

        # Merging top pharma sponsors data with citeline data
        ctln_df  = ctln_df.append(top50_tomerge)

        # Dropping duplicates on processed sponsor columns (after removing keywords)
        aact_df.drop_duplicates(subset=['sponsor_processed'], inplace=True)
        ctln_df.drop_duplicates(subset=['sponsor_processed'], inplace=True)

        # adding indexes
        aact_df.insert(0, "aact_id", range(0, len(aact_df)))

        aact_df.rename(columns= {'sponsor_type' : 'sponsor_type_aact'}, inplace=True)

        # standardizing the industry type in sponsor type
        ctln_df['processed_ctln_sponsor_type'] = ctln_df['sponsor_type'].apply(lambda x: "Industry" if x.lower().find("industry")!=-1 else x)

        # Inner join to find exact matches in aact and citeline dataframes
        exact_match_df = pd.merge(ctln_df, aact_df, how='inner', on=['sponsor_processed'], suffixes=('_ctln', '_aact') )

        # Removing rows that were found to have exact match from input dataframe
        idx_to_drop = exact_match_df['aact_id'].unique().tolist()
        aact_df = aact_df.drop(aact_df.index[idx_to_drop], axis=0)

        exact_match_df = exact_match_df[['sponsor_aact', 'sponsor_exploded_ctln',
                                      'processed_ctln_sponsor_type']].copy()

        # setting threshold and calling the mapping function
        THRESHOLD = 0.67
        mapping_df = self.get_mapping_file(aact_df, ctln_df, THRESHOLD)

        ctln_df.rename(columns={'sponsor_exploded':'ctln_sponsor', 'sponsor_type':'ctln_sponsor_type'}, inplace=True)

        # Merging mapping dataframe with citeline dataframe to get citeline sponnsor which will be used as standard sponsor name
        mapping_df1 = pd.merge(mapping_df, ctln_df[['ctln_sponsor', 'ctln_sponsor_type',  'map_src_idx']], on=['map_src_idx'], how='left')

        # Checking if sponsor was mapped to top pharma sponsor or not
        # Here, standard name is not actual standard, it is processed citeline sponsor name that was passed to algo
        top50_list = top50_tomerge['sponsor_processed'].unique().tolist()
        mapping_df1['top50_pharma_flag']=mapping_df1['Standard sponsor'].apply(lambda x: 1 if x in top50_list else 0)


        values= {'ctln_sponsor_type': '', 'similarity':-1}
        mapping_df1.fillna(value=values, inplace=True)
        mapping_df1['processed_ctln_sponsor_type'] = mapping_df1['ctln_sponsor_type'].apply(lambda x: "Industry" if x.lower().find("industry")!=-1 else x)

        mapping_df1 = mapping_df1[['source', 'sponsor', 'sponsor_type_aact', 'sponsor_nm_new', 'sponsor_exploded', 'ctln_sponsor_type', 'processed_ctln_sponsor_type',  'ctln_sponsor', 'similarity']].copy()
        mapping_df1.rename(columns={'ctln_sponsor': 'standard_sponsor'}, inplace=True)


        logging.info("Model loaded successfully!")
        logging.info("Making Predictions...")

        '''

        mapping_df1['pred_score']=mapping_df1['sponsor'].apply(self.predict)
        mapping_df1['inv_flag']=mapping_df1['pred_score'].apply(lambda x: "inv name" if x<0.5 else "not inv name")

        '''

        logging.info("Predictions complete")
        # If similarity greater than threshold, then keep that sponsor type else mark as Other
        mapping_df1['standard_sponsor_type'] = mapping_df1.apply(lambda x: x['processed_ctln_sponsor_type'] if x['similarity']>=0.67 else "Other", axis=1)

        logging.info("Writing mapping file to CSV...")
        mapping_df1.to_csv("other_sponsor_mapping.csv", index=False)

        ## merge std_top50_exploded , exact_match_df with mapping_df1 to get combined others and std, also add exact match 13k values, rest looks fine

        exact_match_df.to_csv("exact_matched_sponsors_mapping.csv", index=False)
        ctln_df.to_csv("std_sponsors_mapping.csv", index=False)

        end_time = time.time()

        logging.info("Executed in {} minutes".format(round((end_time-initial_time)/60,2)))

        os.system('aws s3 cp exact_matched_sponsors_mapping.csv s3://{}/clinical-data-lake/uploads/SPONSOR_MAPPING/'.format(s3_bucket_name))

        os.system('aws s3 cp std_sponsors_mapping.csv s3://{}/clinical-data-lake/uploads/SPONSOR_MAPPING/'.format(s3_bucket_name))

        os.system('aws s3 cp other_sponsor_mapping.csv s3://{}/clinical-data-lake/uploads/SPONSOR_MAPPING/'.format(s3_bucket_name))


if __name__ == '__main__':
    sponsor_std = SponsorStandardization()

    sponsor_std.main()


