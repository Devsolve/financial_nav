import traceback

import pandas as pd
from datetime import datetime
from src.config.db_config import DbConfig
from src.constant.AppConst import *
from src.model.db_tables import company_info
from src.utils.AppUtils import *


def insert_company_info(nav_company_names_df):
    print('insert_company_info')
    dc = DbConfig()
    con = dc.get_engine()
    # sql = '''select distinct mf_name as company_name from nav_details'''
    # nav_company_names_df = pd.read_sql_query(sql=sql, con=con, params=None)
    nav_company_names_df = nav_company_names_df.drop_duplicates()
    # print(f'***nav_company_names_df: type: {type(nav_company_names_df)}, shape: {nav_company_names_df.shape}')
    existing_company_sql = '''select distinct company_name from company_info'''
    existing_company_names_df = pd.read_sql_query(sql=existing_company_sql, con=con, params=None)
    # print(f'**existing_company_names_df: {existing_company_names_df.shape}')
    new_company_names_df = pd.concat([nav_company_names_df, existing_company_names_df])
    new_company_names_df = new_company_names_df.drop_duplicates(keep=False)
    # print(f'**new_company_names_df: {new_company_names_df.shape}')
    if not new_company_names_df.empty:
        no_of_rows = new_company_names_df.shape[0]
        print('No of new company info to be added: ', no_of_rows)
        added_on_arr = get_np_array(no_of_rows, datetime.utcnow(), 'object')
        new_company_names_df['added_on'] = added_on_arr
        # print(new_company_names_df)
        new_company_names_df.to_sql('company_info', con=con, if_exists='append', chunksize=1000, index=False)
        print(f'{no_of_rows}  new company info added...')


def insert_scheme_type(nav_scheme_type_df):
    print('insert_scheme_type')
    dc = DbConfig()
    con = dc.get_engine()
    nav_scheme_type_df = nav_scheme_type_df.drop_duplicates()
    # sql = '''select distinct fund_status_type as sch_type_name, scheme_type as sch_type_short_name from nav_details
    # '''
    # nav_scheme_type_df = pd.read_sql_query(sql=sql, con=con, params=None)
    # print(f'***nav_scheme_type_df: type: {type(nav_scheme_type_df)}, shape: {nav_scheme_type_df.shape}')
    existing_scheme_type_sql = '''select distinct sch_type_name,sch_type_short_name from scheme_type '''
    existing_scheme_type_df = pd.read_sql_query(sql=existing_scheme_type_sql, con=con, params=None)
    # print(f'**existing_scheme_type_df: {existing_scheme_type_df.shape}')
    new_scheme_type_df = pd.concat([nav_scheme_type_df, existing_scheme_type_df])
    new_scheme_type_df = new_scheme_type_df.drop_duplicates(keep=False)
    # print(f'**new_scheme_type_df: {new_scheme_type_df.shape}')
    if not new_scheme_type_df.empty:
        no_of_rows = new_scheme_type_df.shape[0]
        print('no of new scheme_type added', no_of_rows)
        added_on_arr = get_np_array(no_of_rows, datetime.utcnow(), 'object')
        new_scheme_type_df['added_on'] = added_on_arr
        print('new_scheme_type_df*****', new_scheme_type_df)
        new_scheme_type_df.to_sql('scheme_type', con=con, if_exists='append', chunksize=1000, index=False)
        print(f'{no_of_rows} new scheme type added in scheme_type')


def insert_fund_type(nav_fund_type_df):
    print('insert_fund_type')
    dc = DbConfig()
    con = dc.get_engine()
    nav_fund_type_df = nav_fund_type_df.drop_duplicates()
    # sql = '''select distinct fund_type as fund_type_name from nav_details'''
    # nav_fund_type_df = pd.read_sql_query(sql=sql, con=con, params=None)
    print(f'***nav_fund_type_df: type: {type(nav_fund_type_df)}, shape: {nav_fund_type_df.shape}')
    existing_fund_type_sql = '''select distinct fund_type_name from fund_type'''
    existing_fund_type_df = pd.read_sql_query(sql=existing_fund_type_sql, con=con, params=None)
    new_fund_type_df = pd.concat([nav_fund_type_df, existing_fund_type_df])
    new_fund_type_df = new_fund_type_df.drop_duplicates(keep=False)
    if not new_fund_type_df.empty:
        no_of_rows = new_fund_type_df.shape[0]
        print('no of new fund_type added', no_of_rows)
        added_on_arr = get_np_array(no_of_rows, datetime.utcnow(), 'object')
        new_fund_type_df['added_on'] = added_on_arr
        print('***new_fund_type_df', new_fund_type_df)
        new_fund_type_df.to_sql('fund_type', con=con, if_exists='append', chunksize=1000, index=False)
        print(f'{no_of_rows} new fund type added in fund_type')


def insert_scheme_detail(nav_scheme_detail_df, tr_date):
    print('insert scheme detail..')
    dc = DbConfig()
    con = dc.get_engine()

    sql = '''select sch_code, mf_name as company_name, sch_name as scheme_name, isin_payout,isin_reinv,nav_value,
    purchase_amt,sell_amt,fund_status_type as sch_type_name, scheme_type as sch_type_short_name,fund_type as
    fund_type_name from scheme_detail '''
    existing_scheme_detail_df = pd.read_sql_query(sql=sql, con=con, params=None)

    print('******nav_scheme_detail_df: ', nav_scheme_detail_df.shape)
    __populate_master_data_tables(nav_scheme_detail_df)

    company_info_df = pd.read_sql_table(table_name='company_info', con=con, columns=['company_name', 'company_id'])
    scheme_type_df = pd.read_sql_table(table_name='scheme_type', con=con,
                                       columns=['sch_type_id', 'sch_type_short_name', 'sch_type_name'])
    fund_type_df = pd.read_sql_table(table_name='fund_type', con=con, columns=['fund_type_name', 'fund_type_id'])

    scheme_code = []
    scheme_name = []
    isin = []
    company_id = []
    sch_type_id = []
    fund_type_id = []
    is_active = []
    added_on = []
    now = datetime.utcnow()

    has_value = lambda value: True if value and len(value) > 0 else False

    for nav_sch_row in nav_scheme_detail_df.itertuples(index=False, name='nav_sch_iter'):
        isin_payout = getattr(nav_sch_row, 'isin_payout')
        isin_reinv = getattr(nav_sch_row, 'isin_reinv')
        sch_code = getattr(nav_sch_row, 'sch_code')
        sch_name = getattr(nav_sch_row, 'scheme_name')
        company_name = getattr(nav_sch_row, 'company_name')
        sch_type_name = getattr(nav_sch_row, 'sch_type_name')
        sch_type_short_name = getattr(nav_sch_row, 'sch_type_short_name')
        fund_type_name = getattr(nav_sch_row, 'fund_type_name')
        tr_dt = getattr(nav_sch_row, 'tr_date')
        added_on.append(tr_dt)

        comp_id = company_info_df.loc[company_info_df['company_name'] == company_name, 'company_id'].values[0]
        company_id.append(comp_id)

        is_active.append('Y')

        fnd_typ_id = fund_type_df.loc[fund_type_df['fund_type_name'] == fund_type_name, 'fund_type_id'].values[0]
        fund_type_id.append(fnd_typ_id)

        sch_condition = (scheme_type_df['sch_type_name'] == sch_type_name) & (
                scheme_type_df['sch_type_short_name'] == sch_type_short_name)
        sch_ty_id = scheme_type_df.loc[sch_condition, 'sch_type_id'].values[0]
        sch_type_id.append(sch_ty_id)
        # print('sch_ty_id****',sch_ty_id)

        # print('fund_type_name: ', fund_type_name, ' fund_type_id: ', fnd_typ_id)

        if has_value(isin_reinv) and has_value(isin_payout):
            scheme_code.append(sch_code + '-P')
            scheme_name.append(sch_name + ' Payout')
            isin.append(isin_payout)
            scheme_code.append(sch_code + '-R')
            scheme_name.append(sch_name + ' Reinvestment')
            isin.append(isin_reinv)
            is_active.append('Y')
            company_id.append(comp_id)
            fund_type_id.append(fnd_typ_id)
            sch_type_id.append(sch_ty_id)
            added_on.append(tr_dt)
        else:
            scheme_code.append(sch_code)
            scheme_name.append(sch_name)
            isin_val = isin_payout if has_value(isin_payout) else isin_reinv
            isin.append(isin_val)

    print('isin len: ', len(isin), ' comp_id: ', len(company_id))
    scheme_detail_df = pd.DataFrame({'scheme_code': np.array(scheme_code), 'scheme_name': np.array(scheme_name)
                                        , 'isin': np.array(isin), 'company_id': np.array(company_id),
                                     'is_active': is_active, 'sch_type_id': np.array(sch_type_id),
                                     'fund_type_id': np.array(fund_type_id)})

    scheme_detail_df = scheme_detail_df.drop_duplicates()

    new_scheme_detail_df = pd.concat([nav_scheme_detail_df, scheme_detail_df])

    print('****scheme_detail_df****: ', scheme_detail_df.shape)

    # scheme_detail_df['added_on'] = np.array(added_on)
    scheme_detail_df['updated_on'] = get_np_array(scheme_detail_df.shape[0], now, 'object')

    print('****scheme_detail_df: ', scheme_detail_df.shape)
    scheme_detail_df.to_sql('scheme_detail', con=con, if_exists='append', chunksize=1000, index=False)


def __populate_master_data_tables(nav_scheme_detail_df):
    # master table names [fund_type, company_info, scheme_type]
    if not nav_scheme_detail_df.empty:
        nav_fund_type_df = nav_scheme_detail_df['fund_type_name']
        insert_fund_type(nav_fund_type_df.to_frame())

        nav_scheme_type_df = nav_scheme_detail_df[['sch_type_name', 'sch_type_short_name']]
        insert_scheme_type(nav_scheme_type_df)

        nav_company_names_df = nav_scheme_detail_df[['company_name']]
        insert_company_info(nav_company_names_df)


def insert_daily_nav(tr_date):
    print('insert_daily_nav')
    dc = DbConfig()
    con = dc.get_engine()
    # company_sql = 'select company_id,company_name from company_info'
    # company_df = pd.read_sql_query(company_sql, con)
    # comp_nm = 'Baroda Mutual Fund'
    # company_id = get_company_id(company_df, comp_nm)
    # print(company_id)
    #
    # scheme_detail_sql = '''select scheme_code_id,scheme_name from scheme_detail where is_active='Y' '''
    # scheme_detail_df = pd.read_sql_query(scheme_detail_sql, con)
    # sch_name = 'ICICI Prudential Long Term Equity Fund (Tax Saving) - Growth Payout'
    # scheme_code_id = get_scheme_code_id(scheme_detail_df, sch_name)
    # print(scheme_code_id)
    #
    # scheme_type_sql = 'select sch_type_id,sch_type_name from scheme_type'
    # scheme_type_df = pd.read_sql_query(scheme_type_sql, con)
    # sch_typ_nam = 'Debt Scheme'
    # sch_type_id = get_sch_type_id(scheme_type_df, sch_typ_nam)
    # print(sch_type_id)

    tr_date = '01-Apr-2006'
    sql = '''select sch_code, mf_name as company_name, sch_name as scheme_name, isin_payout,isin_reinv,nav_value,
    purchase_amt,sell_amt,fund_status_type as sch_type_name, scheme_type as sch_type_short_name,fund_type as
    fund_type_name  from nav_details  where tr_date =:tr_date'''
    # nav_detail_sql = '''select sch_name as scheme_name,scheme_type as sch_type_name, isin_payout , isin_reinv ,
    # nav_value ,purchase_amt ,sell_amt , tr_date as added_on from nav_details where tr_date =:tr_date '''
    nav_detail_df = pd.read_sql_query(sql=sql, con=con, params={'tr_date': tr_date})
    print(nav_detail_df)
    insert_scheme_detail(nav_detail_df, tr_date)


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
# insert_scheme_type()
# insert_fund_type()

insert_scheme_detail()
