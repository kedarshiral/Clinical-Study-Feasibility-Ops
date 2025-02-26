# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import sqlparse
import argparse

table_list = """V_AWS_PDL_OCE__MEETINGBUDGET__C
V_AWS_PDL_OCE__MEETINGEXPENSEALLOCATION__C
V_AWS_PDL_OCE__MEETINGEXPENSE__C
V_AWS_PDL_OCE__NOTIFICATION__C
V_AWS_PDL_OCE__TACTIC__C
V_AWS_PDL_OCE__MEETINGHISTORY__C
V_AWS_PDL_OCE__ACCOUNTPLANMEMBERSTAKEHOLDER__C
V_AWS_PDL_OCE__INSIGHTTOPIC__C
V_AWS_PDL_OCE__MILESTONE__C
V_AWS_PDL_OCE__STUDYACCOUNT__C
V_AWS_PDL_OCE__STUDY__C
V_AWS_PDL_OCE__TERRITORYPLAN__C"""
def create_query_sql(src_system,path):
    db_name=str(src_system).upper()
    table_name=table_list.replace("\n","|").split("|")
    print(len(table_name))
    print(table_name)
    insert_query="""
    select * from QA_OCE_SALES_CORE.$$table_name where to_char(to_date(TECH_LAST_MOD_DATE,'DD-MON-YY'),'YYYY-MM-DD')>to_char(to_date('$$TECH_LAST_MOD_DATE','YYYY-MM-DD'),'YYYY-MM-DD') OR to_char(to_date(TECH_INSERT_DATE,'DD-MON-YY'),'YYYY-MM-DD')>to_char(to_date('$$TECH_INSERT_DATE','YYYY-MM-DD'),'YYYY-MM-DD');
    """
    path=str(path)
    for table_data in table_name:
        data=insert_query.replace("$$table_name",table_data)
        formatted_content = sqlparse.format(data, keyword_case='upper', strip_comments=True,
                                            strip_whitespace=True)
        print(formatted_content)
        with open(path+"{}_{}.sql".format(db_name,table_data),'w') as write_query:
            write_query.write(formatted_content)

if __name__=='__main__':
    PARSER = argparse.ArgumentParser(description="create sql file")
    PARSER.add_argument("-sc", "--src_system",
                        help="Data Source system is required to know from which data source we have to extract data",
                        required=True)
    PARSER.add_argument("-lp", "--path", help="Table name is required", required=True)
    ARGS = vars(PARSER.parse_args())
    src_system = ARGS['src_system']
    path=ARGS['path']
    q_obj=create_query_sql(src_system,path)

