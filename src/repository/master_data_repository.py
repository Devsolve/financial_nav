import traceback

import pandas as pd
from datetime import datetime
from src.config.db_config import DbConfig
from src.constant.AppConst import *
from src.model.db_tables import company_info
from src.utils.AppUtils import *


def insert_company_info():
    print('insert_company_info')
    dc = DbConfig()
    con = dc.get_engine()
    sql = '''select distinct mf_name as company_name from nav_details'''
    nav_company_names_df = pd.read_sql_query(sql=sql, con=con, params=None)
    existing_company_sql = '''select distinct company_name from company_info'''
    existing_company_names_df = pd.read_sql_query(sql=existing_company_sql, con=con, params=None)
    new_company_names_df = pd.concat([nav_company_names_df, existing_company_names_df])
    new_company_names_df = new_company_names_df.drop_duplicates(keep=False)
    if not new_company_names_df.empty:
        no_of_rows = new_company_names_df.shape[0]
        print('No of new company info to be added: ', no_of_rows)
        added_on_arr = get_np_array(no_of_rows, datetime.utcnow(), 'object')
        new_company_names_df['added_on'] = added_on_arr
        # print(new_company_names_df)
        new_company_names_df.to_sql('company_info', con=con, if_exists='append', chunksize=1000, index=False)
        print(f'{no_of_rows}  new company info added...')

    # session = dc.get_session()
    # try:
    #     sql = f'''select mf_name as company_name from nav_details where sch_code in {tuple( SCHEME_CODES )} '''
    #     company_names_df = pd.read_sql_query( sql=sql, con=con, params=None )
    #     print( company_names_df.to_dict( orient='records' ) )
    #     session.begin()
    #     session.bulk_insert_mappings( company_info, company_names_df.to_dict( orient='records' ) )
    #     session.commit()
    #     print( 'company_info data inserted.' )
    # except Exception:
    #     session.rollback()
    #     traceback.print_exc()

    # df.to_sql( 'company_info', con=con, if_exists='append', chunksize=1000 )


def insert_scheme_type():
    print('insert_scheme_type')
    dc = DbConfig()
    con = dc.get_engine()
    sql = '''select distinct fund_status_type as sch_type_name, scheme_type as sch_type_short_name from nav_details '''
    nav_scheme_type_df = pd.read_sql_query(sql=sql, con=con, params=None)
    existing_scheme_type_sql = '''select distinct sch_type_name,sch_type_short_name from scheme_type '''
    existing_scheme_type_df = pd.read_sql_query(sql=existing_scheme_type_sql, con=con, params=None)
    new_scheme_type_df = pd.concat([nav_scheme_type_df, existing_scheme_type_df])
    new_scheme_type_df = new_scheme_type_df.drop_duplicates(keep=False)
    new_scheme_type_df.to_sql('scheme_type', con=con, if_exists='append', chunksize=1000, index=False)


def insert_daily_nav():
    print('insert_daily_nav')
    dc = DbConfig()
    con = dc.get_engine()
    company_sql = 'select company_id,company_name from company_info'
    company_df = pd.read_sql_query(company_sql, con)
    comp_nm = 'Baroda Mutual Fund'
    company_id = get_company_id(company_df, comp_nm)
    print(company_id)

    scheme_detail_sql = '''select scheme_code_id,scheme_name from scheme_detail where is_active='Y' '''
    scheme_detail_df = pd.read_sql_query(scheme_detail_sql, con)
    sch_name = 'ICICI Prudential Long Term Equity Fund (Tax Saving) - Growth Payout'
    scheme_code_id = get_scheme_code_id(scheme_detail_df, sch_name)
    print(scheme_code_id)

    scheme_type_sql = 'select sch_type_id,sch_type_name from scheme_type'
    scheme_type_df = pd.read_sql_query(scheme_type_sql, con)
    sch_typ_nam = 'Debt Scheme'
    sch_type_id = get_sch_type_id(scheme_type_df, sch_typ_nam)
    print(sch_type_id)

    tr_dt = '01-Apr-2006'
    nav_detail_sql = '''select sch_name as scheme_name,scheme_type as sch_type_name, isin_payout , isin_reinv , 
    nav_value ,purchase_amt ,sell_amt , tr_date as added_on from nav_details where tr_date =:tr_date '''
    nav_detail_df = pd.read_sql_query(sql=nav_detail_sql, con=con, params={'tr_date': tr_dt})
    print(nav_detail_df)


def get_company_id(company_df, comp_nm):
    company_id = company_df.loc[company_df.company_name == comp_nm, 'company_id'].values[0]
    return company_id


def get_scheme_code_id(scheme_detail_df, sch_name):
    scheme_code_id = scheme_detail_df.loc[scheme_detail_df.scheme_name == sch_name, 'scheme_code_id'].values[0]
    return scheme_code_id


def get_sch_type_id(scheme_type_df, sch_typ_nam):
    sch_type_id = scheme_type_df.loc[scheme_type_df.sch_type_name == sch_typ_nam, 'sch_type_id'].values[0]
    return sch_type_id


# insert_daily_nav()
# insert_company_info()
insert_scheme_type()
