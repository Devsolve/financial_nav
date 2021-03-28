import pandas as pd
import traceback
from src.config.db_config import DbConfig
from src.constant.AppConst import *
from src.repository.master_data_repository import populate_master_data_tables, existing_scheme_detail
from src.repository.nav_data_repository import NavDataRepository
from src.utils import AppUtils
from datetime import datetime


def last_daily_nav_insert_date():
    nav_dt = None
    dc = DbConfig()
    con = dc.get_engine()
    # nav_date_sql = '''select  max(nav_date) from daily_nav '''
    # for Sqlite because date column does not work with max function
    nav_date_sql = '''select nav_date from daily_nav where nav_id in ( select max(nav_id) from daily_nav)'''
    date_df = pd.read_sql_query( sql=nav_date_sql, con=con )
    if not date_df.empty:
        nav_dt = date_df['nav_date'].values[0]

    print( f'last daily nav insert date: {nav_dt}' )
    return nav_dt


def insert_daily_nav(no_of_days=1):
    print( f'insert_daily_nav for no_of_days: {no_of_days}' )
    msg = "Nav deta not found in nav_details table !!!!!!!"
    last_nav_date = None
    success = False
    try:
        last_nav_date = last_daily_nav_insert_date()

        dc = DbConfig()
        con = dc.get_engine()

        no_of_days_nxt = 1
        if not last_nav_date:
            print( 'Data not available in daily_nav table' )
            no_of_days_nxt = 0
            print( 'find starting tr_date in nav_detail table' )
            data_repo = NavDataRepository()
            last_nav_date = data_repo.get_last_tr_date()
        print( f'last_nav_date: {last_nav_date}' )

        if last_nav_date:

            while no_of_days_nxt <= int(no_of_days):
                last_nav_dt = datetime.strptime( last_nav_date, '%d-%b-%Y' )
                tr_date = AppUtils.get_next_date_str( last_nav_dt, no_of_days_nxt=no_of_days_nxt, format='%d-%b-%Y' )

                sql = '''select sch_code as scheme_code, mf_name as company_name, sch_name as scheme_name, isin_payout,isin_reinv,nav_value,
                purchase_amt,sell_amt,fund_status_type as sch_type_name, scheme_type as sch_type_short_name,fund_type as
                fund_type_name,tr_date, 'N' carry_forward from nav_details where tr_date = :tr_date '''
                print( f'find nav_details for tr_date: {tr_date}' )
                nav_detail_df = pd.read_sql_query( sql=sql, con=con, params={'tr_date': tr_date} )
                print( f'No of NAV_DETAIL rows found [{nav_detail_df.shape[0]}] for tr_date: {tr_date}.' )

                if not nav_detail_df.empty:
                    populate_master_data_tables( nav_detail_df, tr_date )
                    daily_nav_df = pd.DataFrame()
                    daily_nav_df[['nav_value', 'purchase_amt', 'sell_amt', 'nav_date', 'carry_forward']] = \
                    nav_detail_df[
                        ['nav_value', 'purchase_amt', 'sell_amt', 'tr_date', 'carry_forward']]
                    daily_nav_df['last_nav_date'] = nav_detail_df['tr_date']

                    existing_scheme_detail_df = existing_scheme_detail()
                    sch_ids = []
                    for nav_row in nav_detail_df.itertuples( index=False, name='daily_nav_iter' ):
                        sch_nm = getattr( nav_row, 'scheme_name' )
                        sch_nm = sch_nm.strip()
                        sch_id = existing_scheme_detail_df.loc[
                            existing_scheme_detail_df['scheme_name'] == sch_nm, 'scheme_id']
                        if sch_id.empty:
                            sch_id = existing_scheme_detail_df.loc[
                                existing_scheme_detail_df['scheme_name'] == sch_nm + PAYOUT, 'scheme_id']
                        if sch_id.empty:
                            sch_id = existing_scheme_detail_df.loc[
                                existing_scheme_detail_df['scheme_name'] == sch_nm + REINVEST, 'scheme_id']
                        sch_ids.append( sch_id.values[0] )
                    # sch_cond = (existing_scheme_detail_df['scheme_name'].isin(nav_detail_df['scheme_name']) |
                    # existing_scheme_detail_df['scheme_name'].isin(nav_detail_df['scheme_name'].str.cat(PAYOUT)) |
                    # existing_scheme_detail_df['scheme_name'].isin(nav_detail_df['scheme_name'].str.cat(REINVEST))
                    # daily_nav_df['scheme_id'] = existing_scheme_detail_df.loc[sch_cond,'scheme_id']
                    daily_nav_df['scheme_id'] = sch_ids
                    del nav_detail_df
                    print( 'insert_daily_nav Nav date: {tr_date}' )
                    daily_nav_df.to_sql( 'daily_nav', con=con, chunksize=1000, index=False, if_exists='append' )
                    print( f'Nav date: {tr_date}, No of Nav rows inserted: {daily_nav_df.shape[0]}' )
                    del daily_nav_df

                    if no_of_days_nxt > 1:
                        print( f'carry_forward_nav for nav_date: {tr_date}' )
                        carry_forward_nav( tr_date )
                        print( f'close_scheme for nav_date: {tr_date}, EXPIRY_DAY_LIMIT: {EXPIRY_DAY_LIMIT}' )
                        close_scheme( tr_date )

                no_of_days_nxt = no_of_days_nxt + 1

            msg = f"Daily Nav details populated for {no_of_days} days starting from {last_nav_date} !!!!!!!"
        else:
            print( 'Nav Detail Table is empty!!!!!!!!!!' )
        success = True
    except Exception:
        msg = f"Failed to populate daily Nav details for {no_of_days} days starting from {last_nav_date} !!!!!!!"
        print( f'Exception in insert_daily_nav for nav_date: {last_nav_date}..' )
        traceback.print_exc()
    return {'message': msg, 'success': success}


def carry_forward_nav(tr_dt):
    today = datetime.strptime( tr_dt, '%d-%b-%Y' )
    prev_dt_str = AppUtils.get_prev_date_str( today )
    print( f'carry_forward_nav for nav_date: {tr_dt}, previous date: {prev_dt_str}' )
    sql = f'''insert into daily_nav (scheme_id,nav_value,purchase_amt,sell_amt,nav_date,last_nav_date,carry_forward)
        select scheme_id, nav_value,purchase_amt,sell_amt,'{tr_dt}' nav_date, last_nav_date, 'Y' carry_forward 
        from daily_nav where nav_date = '{prev_dt_str}' and scheme_id not in
        (select scheme_id from daily_nav where nav_date = '{tr_dt}')'''
    dc = DbConfig()
    session = dc.get_session()
    try:
        session.begin()
        session.execute( sql )
        session.commit()
        print( f'carry_forward_nav executed for current_date: {tr_dt}, previous date: {prev_dt_str}' )
    except Exception:
        session.rollback()
        print( f'Exception in carry_forward_nav for nav_date: {tr_dt}..' )
        traceback.print_exc()


def close_scheme(tr_dt):
    today = datetime.strptime( tr_dt, '%d-%b-%Y' )
    last_nav_date = AppUtils.get_prev_date_str( today_dt=today, no_of_days_back=EXPIRY_DAY_LIMIT, format='%d-%b-%Y' )
    date_tuple = AppUtils.get_prev_dates_str( today, EXPIRY_DAY_LIMIT )
    print(
        f'calling close_scheme for nav_date: {tr_dt}, last_nav_date: {last_nav_date}, expiry day_limit: {EXPIRY_DAY_LIMIT},\n dates:{date_tuple}' )
    dc = DbConfig()
    engine = dc.get_engine()
    count_sql = f'''select scheme_id , count(scheme_id) count_present
                    from daily_nav 
                    where nav_date in {date_tuple} and nav_date != last_nav_date
                    and scheme_id in (select scheme_id from scheme_detail where is_active='Y')
                    group by scheme_id'''
    session = dc.get_session()
    try:
        session.begin()
        print( f'find scheme ids not present in last {EXPIRY_DAY_LIMIT} days.' )
        scheme_count_df = pd.read_sql_query( con=engine, sql=count_sql )
        if not scheme_count_df.empty:
            expired_scheme_df = scheme_count_df.loc[scheme_count_df['count_present'] == EXPIRY_DAY_LIMIT]
            print(
                f'Nav date: {tr_dt}, No of scheme ids expired [{expired_scheme_df.shape[0]}] in last {EXPIRY_DAY_LIMIT} days.' )
            if not expired_scheme_df.empty:
                print( f'Nav date: {tr_dt}, close schemes one by one...' )
                for ex_sch_row in expired_scheme_df.itertuples( index=False, name='ex_iter' ):
                    scheme_id = getattr( ex_sch_row, 'scheme_id' )
                    sch_updt_qry = f'''update scheme_detail set is_active ='N', updated_on='{last_nav_date}' 
                                        where scheme_id = {scheme_id} '''
                    del_nav_qry = f'''delete from daily_nav where scheme_id = {scheme_id} 
                                    and carry_forward='Y' and last_nav_date='{last_nav_date}' '''
                    session.execute( sch_updt_qry )
                    session.execute( del_nav_qry )
                print( f'{expired_scheme_df.shape[0]} scheme ids closed on {last_nav_date}....' )
        session.commit()
    except Exception:
        session.rollback()
        print( f'Exception in close_scheme for nav_date: {tr_dt}..' )
        traceback.print_exc()



# insert_daily_nav( 365 )
