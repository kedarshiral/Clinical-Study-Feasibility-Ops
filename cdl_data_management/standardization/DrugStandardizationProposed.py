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
from datetime import date, datetime

from ftfy import fix_text
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from pyspark.sql import *
import pyspark.sql.functions as f
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY,"s3_bucket_name"])

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

class DrugStandardizationProposed:

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
                            'Input Drug': left_side,
                            'Standard Drug': right_side,
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
    
    def get_mapping_file(self, df, base_std_df):
    
        """
        input: load combined aact, citeline and dqs diseases data
        purpose: process different data sources data and combine the output for a single mapping file
        returns final mapping file
        """
    
        logging.info("Cleaning data...")
    
        base_std_df.dropna(subset=['processed'], inplace=True)
        base_std_df.drop_duplicates(subset=['processed'], inplace=True)
    
        ## adding primary index
    
        base_std_df.insert(0, "map_src_idx", range(0, 0+len(base_std_df)))
    
    
        logging.info("Processing AACT data...")
        # processing aact df
    
        df.drop_duplicates(subset=['processed'], inplace=True)
        df.insert(0, "map_to_idx", range(0, 0+len(df)))
    
        df1 = df.copy()
        df_clean = base_std_df.copy()
    
        logging.info("Creating mapping for aact data...")
    
        output_aact = self.match(df1, df_clean, "processed")
        aact_mapping = self.get_org_cols(output_aact, df, base_std_df)
    
        return aact_mapping

    def main(self):
        initial_time = time.time()
        print(CommonConstants.AIRFLOW_CODE_PATH)
        delta_file_path = CommonConstants.AIRFLOW_CODE_PATH+"/Drug_Delta"
        print(delta_file_path)
        if os.path.exists(delta_file_path):
            print("Inside remove if ",delta_file_path)
            os.system("rm -r" + delta_file_path)
        os.system(
            "hadoop fs -copyToLocal /user/hive/warehouse/Drug_Delta/ "
            + CommonConstants.AIRFLOW_CODE_PATH
            + "/"
        )
        os.system("mv "
                            + CommonConstants.AIRFLOW_CODE_PATH
                            + "/Drug_Delta/*.csv "
                            + CommonConstants.AIRFLOW_CODE_PATH
                            + "/drug_delta.csv"
                        )        
        delta_df=pd.read_csv('drug_delta.csv', sep='`')
        
        mapping_file_path =CommonConstants.AIRFLOW_CODE_PATH+"/Drug_Mapping.xlsx"
        if os.path.exists(mapping_file_path):
            os.system("rm " + mapping_file_path)

        os.system('aws s3 cp s3://{}/clinical-data-lake/uploads/DRUG/Drug_Mapping_File/Drug_Mapping.xlsx ./'.format(s3_bucket_name))
        mapping_file_df=pd.read_excel('Drug_Mapping.xlsx')
        
        
        # Filling NaN values so re.sub doesn't give errors of not str like object
        delta_df.fillna('', inplace=True)
        delta_df.drop_duplicates(subset=['raw_drug_name'], inplace=True)
        delta_df.rename(columns={'raw_drug_name':'processed'}, inplace=True)
        
        # adding indexes
        delta_df.insert(0, "delta_id", range(0, len(delta_df)))
        
        
        # extracting top 50 sponsor list
        mapping_list = mapping_file_df.drugnamesynonyms.unique().tolist()
        
        # Creating new dataframe with top pharma sponsors
        clean_df = pd.DataFrame(mapping_list, columns=['drugnamesynonyms'])

        clean_df.rename(columns={'drugnamesynonyms':'processed'}, inplace=True)
        
        
        # setting threshold and calling the mapping function
        
        mapping_df = self.get_mapping_file(delta_df, clean_df)
        THRESHOLD = 0.90
        filter_threshold = mapping_df["similarity"] >= THRESHOLD
        
        mapping_df.where(filter_threshold, inplace = True)
        #mapping_df.to_excel("mapping_df.xlsx")
        
        logging.info("Model loaded successfully!")
        logging.info("Making Predictions...")
        
        
        logging.info("Predictions complete")

        logging.info("Writing mapping file to xlsx...")
            
        ##merge mapping_file_df_exploded , exact_match_df with mapping_df1 to get combined others and std, also add exact match 13k values, rest looks fine
        
        final_df=pd.merge(mapping_df,mapping_file_df,left_on='Standard Drug', right_on='drugnamesynonyms', how='left')
        final_df.drop(['map_to_idx', 'delta_id', 'processed','datasource', 'map_src_idx','drugnamesynonyms'], axis=1,inplace=True)
        final_df.rename(columns={'Input Drug':'raw_drug_name','Standard Drug':'drugnamesynonyms','drugprimaryname':'drugprimaryname'},inplace=True)
        #final_df['drugnamesynonyms'].fillna("Other",inplace=True)
        final_df.drop_duplicates(subset=['raw_drug_name','drugnamesynonyms','similarity','drugprimaryname'], inplace=True)
        final_df = final_df[['raw_drug_name','drugnamesynonyms','similarity','drugprimaryname']]
        final_df.to_excel("drug_delta_ouput.xlsx", index=False)
        print("Output has been stored in xlsx file drug_delta_ouput.xlsx in your current working directory code folder")

        print("Uploading latest drug mapping file to S3...")
        os.system('aws s3 cp drug_delta_ouput.xlsx s3://{}/clinical-data-lake/uploads/DRUG/Temp_Drug_Mapping_Proposed_Holder/drug_delta_ouput.xlsx'.format(s3_bucket_name))        
        end_time = time.time()
        
        logging.info("Executed in {} minutes".format(round((end_time-initial_time)/60,2)))
        

if __name__ == '__main__':
    drug_std = DrugStandardizationProposed()

    drug_std.main()