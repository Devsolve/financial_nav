import traceback


import pandas as pd
from datetime import datetime
from src.config.db_config import DbConfig
from src.constant.AppConst import *
from src.utils.AppUtils import *


def existing_company_info():
    dc = DbConfig()
    con = dc.get_engine()
    existing_company_sql = '''select distinct company_name,company_id from company_info'''
    existing_company_names_df = pd.read_sql_query(sql=existing_company_sql, con=con, params=None)
    return existing_company_names_df


def existing_scheme_type():
    dc = DbConfig()
    con = dc.get_engine()
    existing_scheme_type_sql = '''select distinct sch_type_name,sch_type_short_name,sch_type_id from scheme_type '''
    existing_scheme_type_df = pd.read_sql_query(sql=existing_scheme_type_sql, con=con, params=None)
    return existing_scheme_type_df


def existing_fund_type():
    dc = DbConfig()
    con = dc.get_engine()
    existing_fund_type_sql = '''select distinct fund_type_name,fund_type_id from fund_type'''
    existing_fund_type_df = pd.read_sql_query(sql=existing_fund_type_sql, con=con, params=None)
    return existing_fund_type_df


def existing_scheme_detail():
    dc = DbConfig()
    con = dc.get_engine()
    sql = '''select scheme_id, scheme_code, scheme_name,isin, is_active from scheme_detail '''
    existing_scheme_detail_df = pd.read_sql_query(sql=sql, con=con, params=None)
    return existing_scheme_detail_df


def insert_company_info(nav_company_names_df, nav_date):
    print(f'insert_company_info for nav_date: {nav_date}')

    new_company_names_df = nav_company_names_df[['company_name']].drop_duplicates()

    existing_company_names_df = existing_company_info()
    if not existing_company_names_df.empty:
        existing_company_names = existing_company_names_df['company_name'].values
        new_company_names_df = new_company_names_df.loc[~new_company_names_df['company_name'].isin(existing_company_names)]

    # print(f'**new_company_names_df: {new_company_names_df.shape}')
    if not new_company_names_df.empty:
        no_of_rows = new_company_names_df.shape[0]
        print('No of new company info to be added: ', no_of_rows)
        added_on_arr = get_np_array(no_of_rows, nav_date, 'object')
        new_company_names_df['added_on'] = added_on_arr
        dc = DbConfig()
        con = dc.get_engine()
        new_company_names_df.to_sql('company_info', con=con, if_exists='append', chunksize=1000, index=False)
        print(f'{no_of_rows}  new company info added for nav_date: {nav_date}...')


def insert_scheme_type(nav_scheme_type_df,nav_date):
    print(f'insert_scheme_type for nav_date: {nav_date}')

    nav_scheme_type_df = nav_scheme_type_df.drop_duplicates(subset=['sch_type_name','sch_type_short_name'])


    existing_scheme_type_df = existing_scheme_type()

    if existing_scheme_type_df.shape[0] != 0:
        existing_scheme_type_df['sch_type_nm_short_nm'] = existing_scheme_type_df['sch_type_name'] + \
                                                          existing_scheme_type_df['sch_type_short_name']
        new_sch_types = []
        for sch_type_row in nav_scheme_type_df.itertuples(index=False, name='st_iter'):
            sch_type_name = getattr(sch_type_row, 'sch_type_name')
            sch_type_short_name = getattr(sch_type_row, 'sch_type_short_name')
            sch_type_nm_short_nm = sch_type_name + sch_type_short_name
            if sch_type_nm_short_nm not in existing_scheme_type_df['sch_type_nm_short_nm'].values:
                new_sch_types.append({'sch_type_name': sch_type_name,'sch_type_short_name': sch_type_short_name,'added_on': nav_date})
        new_scheme_type_df = pd.DataFrame(new_sch_types)
    else:
        new_scheme_type_df = nav_scheme_type_df
        new_scheme_type_df['added_on'] = get_np_array(new_scheme_type_df.shape[0], nav_date, 'object')

    if not new_scheme_type_df.empty:
        no_of_rows = new_scheme_type_df.shape[0]
        print(f'{no_of_rows} new scheme_type to be added for nav_date : {nav_date}')
        dc = DbConfig()
        con = dc.get_engine()
        new_scheme_type_df.to_sql('scheme_type', con=con, if_exists='append', chunksize=500, index=False)
        del new_scheme_type_df
        print(f'{no_of_rows} new scheme type added in scheme_type table for nav_date : {nav_date}')


def insert_fund_type(nav_fund_type_df,nav_date):
    print('insert_fund_type for nav_date: {nav_date}')
    dc = DbConfig()
    con = dc.get_engine()
    new_fund_type_df = nav_fund_type_df.drop_duplicates()

    existing_fund_type_df = existing_fund_type()
    if not existing_fund_type_df.empty:
        existing_fund_types = existing_fund_type_df['fund_type_name'].values
        new_fund_type_df = new_fund_type_df.loc[~new_fund_type_df['fund_type_name'].isin(existing_fund_types)]

    if not new_fund_type_df.empty:
        no_of_rows = new_fund_type_df.shape[0]
        print(f'{no_of_rows} new fund_type to be added for nav_date: {nav_date}', )
        new_fund_type_df['added_on'] = get_np_array(no_of_rows, nav_date, 'object')
        new_fund_type_df.to_sql('fund_type', con=con, if_exists='append', chunksize=1000, index=False)
        print(f'{no_of_rows} new fund type added in fund_type table for nav_date: {nav_date}...')


def get_company_id(company_df, comp_nm):
    company_id = company_df.loc[company_df.company_name == comp_nm, 'company_id'].values[0]
    return company_id


def get_fund_type_id(fund_type_df, fund_type_name):
    fund_type_id = fund_type_df.loc[fund_type_df['fund_type_name'] == fund_type_name, 'fund_type_id'].values[0]
    return fund_type_id


def get_scheme_code_id(scheme_detail_df, sch_name):
    scheme_code_id = scheme_detail_df.loc[scheme_detail_df.scheme_name == sch_name, 'scheme_code_id'].values[0]
    return scheme_code_id


def get_sch_type_id(scheme_type_df, sch_type_name, sch_type_short_name):
    sch_condition = (scheme_type_df['sch_type_name'] == sch_type_name) \
                    & (scheme_type_df['sch_type_short_name'] == sch_type_short_name)
    sch_type_id = scheme_type_df.loc[sch_condition, 'sch_type_id'].values[0]
    return sch_type_id


def insert_scheme_detail(nav_scheme_detail_df, nav_date):
    print(f'insert scheme detail for nav_date: {nav_date}')
    existing_company_names_df = existing_company_info()
    existing_fund_type_df = existing_fund_type()
    existing_scheme_type_df = existing_scheme_type()
    existing_scheme_detail_df = existing_scheme_detail()

    has_value = lambda value: True if value and len(value) > 0 else False

    rows = []

    for nav_sch_row in nav_scheme_detail_df.itertuples(index=False, name='nav_sch_iter'):
        isin_payout = getattr(nav_sch_row, 'isin_payout')
        isin_reinv = getattr(nav_sch_row, 'isin_reinv')
        sch_code = getattr(nav_sch_row, 'scheme_code')
        sch_name = getattr(nav_sch_row, 'scheme_name')
        company_name = getattr(nav_sch_row, 'company_name')
        sch_type_name = getattr(nav_sch_row, 'sch_type_name')
        sch_type_short_name = getattr(nav_sch_row, 'sch_type_short_name')
        fund_type_name = getattr(nav_sch_row, 'fund_type_name')
        tr_dt = getattr(nav_sch_row, 'tr_date')

        # comp_id = company_info_df.loc[company_info_df['company_name'] == company_name, 'company_id'].values[0]
        comp_id = get_company_id(existing_company_names_df, company_name)
        fnd_typ_id = get_fund_type_id(existing_fund_type_df, fund_type_name)
        sch_ty_id = get_sch_type_id(existing_scheme_type_df, sch_type_name, sch_type_short_name)

        # print('fund_type_name: ', fund_type_name, ' fund_type_id: ', fnd_typ_id)

        if has_value(isin_reinv) and has_value(isin_payout) and (
                (PAYOUT not in sch_name) or (REINVEST not in sch_name)):
            p_sch_name = sch_name + PAYOUT
            p_row = create_scheme_detail_row(sch_code, p_sch_name, isin_payout, comp_id, sch_ty_id, fnd_typ_id, tr_dt)
            rows.append(p_row)
            r_sch_name = sch_name + REINVEST
            r_row = create_scheme_detail_row(sch_code, r_sch_name, isin_reinv, comp_id, sch_ty_id, fnd_typ_id, tr_dt)
            rows.append(r_row)
        else:
            isin_val = isin_payout if has_value(isin_payout) else isin_reinv
            row = create_scheme_detail_row(sch_code, sch_name, isin_val, comp_id, sch_ty_id, fnd_typ_id, tr_dt)
            rows.append(row)

    if len(rows) > 0:
        print(f'No of new schemes to be inserted {len(rows)} for nav_date: {nav_date}')
        scheme_detail_df = pd.DataFrame(rows)
        scheme_detail_df = scheme_detail_df.drop_duplicates(subset='scheme_name', keep=False)
        # print('******* scheme detail Before: ', scheme_detail_df.shape[0])
        if not existing_scheme_detail_df.empty:
            print('Remove existing schemes to avoid duplicate rows in scheme details table....')
            existing_sch_names = existing_scheme_detail_df['scheme_name'].values
            scheme_detail_df = scheme_detail_df.loc[~scheme_detail_df['scheme_name'].isin(existing_sch_names)]

        no_of_rows = scheme_detail_df.shape[0]
        scheme_detail_df['added_on'] = get_np_array(no_of_rows, nav_date, 'object')
        print(f'{no_of_rows} new scheme detail will be added for nav_date: {nav_date}')
        dc = DbConfig()
        con = dc.get_engine()
        scheme_detail_df.to_sql('scheme_detail', con=con, if_exists='append', chunksize=1000, index=False)
        print(f'{no_of_rows} new scheme detail is added for nav_date: {nav_date}...')


def create_scheme_detail_row(sch_code, sch_name, isin, company_id, sch_type_id, fund_type_id, tr_date):
    row = {'scheme_code': sch_code, 'scheme_name': sch_name
        , 'isin': isin, 'company_id': company_id,
           'is_active': 'Y', 'sch_type_id': sch_type_id,
           'fund_type_id': fund_type_id, 'added_on': tr_date}
    return row


def populate_master_data_tables(nav_scheme_detail_df, nav_date):
    # master table names [fund_type, company_info, scheme_type]
    print(f'populate_master_data_tables for nav_date: {nav_date}, no of schemes nav found: {nav_scheme_detail_df.shape[0]}')
    if not nav_scheme_detail_df.empty:
        nav_fund_type_df = nav_scheme_detail_df['fund_type_name']
        insert_fund_type(nav_fund_type_df.to_frame(), nav_date)

        nav_scheme_type_df = nav_scheme_detail_df[['sch_type_name', 'sch_type_short_name']]
        insert_scheme_type(nav_scheme_type_df,nav_date)

        nav_company_names_df = nav_scheme_detail_df[['company_name']]
        insert_company_info(nav_company_names_df,nav_date)

        insert_scheme_detail(nav_scheme_detail_df,nav_date)

