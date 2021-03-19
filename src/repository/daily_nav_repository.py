import pandas as pd
from src.config.db_config import DbConfig
from src.constant.AppConst import *
from src.repository.master_data_repository import populate_master_data_tables, existing_scheme_detail


def insert_daily_nav(tr_date):
    dc = DbConfig()
    con = dc.get_engine()

    date_sql = '''select nav_date from daily_nav where nav_date =:tr_date '''
    date_df = pd.read_sql_query(sql=date_sql, con=con, params={'tr_date': tr_date})

    if date_df.empty:
        sql = '''select sch_code as scheme_code, mf_name as company_name, sch_name as scheme_name, isin_payout,isin_reinv,nav_value,
        purchase_amt,sell_amt,fund_status_type as sch_type_name, scheme_type as sch_type_short_name,fund_type as
        fund_type_name,tr_date from nav_details  where tr_date =:tr_date'''

        nav_detail_df = pd.read_sql_query(sql=sql, con=con, params={'tr_date': tr_date})
        populate_master_data_tables(nav_detail_df)
        daily_nav_df = pd.DataFrame()
        daily_nav_df[['nav_value', 'purchase_amt', 'sell_amt', 'nav_date']] = nav_detail_df[
            ['nav_value', 'purchase_amt', 'sell_amt', 'tr_date']]

        existing_scheme_detail_df = existing_scheme_detail()
        sch_ids = []
        for nav_row in nav_detail_df.itertuples(index=False, name='daily_nav_iter'):
            sch_nm = getattr(nav_row, 'scheme_name')
            sch_nm = sch_nm.strip()
            sch_id = existing_scheme_detail_df.loc[existing_scheme_detail_df['scheme_name'] == sch_nm, 'scheme_id']
            if sch_id.empty:
                sch_id = existing_scheme_detail_df.loc[
                    existing_scheme_detail_df['scheme_name'] == sch_nm + PAYOUT, 'scheme_id']
            if sch_id.empty:
                sch_id = existing_scheme_detail_df.loc[
                    existing_scheme_detail_df['scheme_name'] == sch_nm + REINVEST, 'scheme_id']
            sch_ids.append(sch_id.values[0])
        # sch_cond = (existing_scheme_detail_df['scheme_name'].isin(nav_detail_df['scheme_name'])
        #             | existing_scheme_detail_df['scheme_name'].isin(nav_detail_df['scheme_name'].str.cat(PAYOUT))
        #             | existing_scheme_detail_df['scheme_name'].isin(nav_detail_df['scheme_name'].str.cat(REINVEST))
        # daily_nav_df['scheme_id'] = existing_scheme_detail_df.loc[sch_cond,'scheme_id']
        daily_nav_df['scheme_id'] = sch_ids
        del nav_detail_df
        print('insert_daily_nav')
        daily_nav_df.to_sql('daily_nav', con=con, chunksize=1000, index=False, if_exists='append')
        print(f'Nav date {tr_date}, No of Nav rows inserted: {daily_nav_df.shape[0]}')
        del daily_nav_df
    else:
        print(f'Nav data already saved for date :{tr_date}')


insert_daily_nav('05-Jan-2019')



def carry_forward_nav():
    pass

def close_scheme():
    pass
