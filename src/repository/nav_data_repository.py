import datetime
import traceback


from src.config.db_config import DbConfig
from src.parser.parse_nav_data import ParseNavData
import pandas as pd


class NavDataRepository:

    def __init__(self):
        self.db = DbConfig()

    def save_nav_history_data(self, search_param=None):
        print( 'save_nav_history_data for search_param: ', search_param )
        msg = None
        success = False
        frmdt = None
        try:
            if search_param:
                frmdt = search_param['frmdt'] if 'frmdt' in search_param else '01-Apr-2006'
            else:
                last_tr_date = self.get_last_tr_date()
                frmdt = last_tr_date if last_tr_date else '01-Apr-2006'

            print( f'save_nav_history_data From Date: {frmdt}' )
            dt_obj = datetime.datetime.strptime( frmdt, '%d-%b-%Y' )
            today = datetime.datetime.today()
            prevday = today - datetime.timedelta( days=1 )
            no_of_days = 0
            while dt_obj <= prevday:
                frmdt = dt_obj.strftime( '%d-%b-%Y' )
                search_param = {'frmdt': frmdt, 'todt': frmdt}
                sql = 'select sch_code from nav_details where tr_date= :frmdt'
                result = pd.read_sql_query( sql=sql, con=self.db.get_engine(), params={'frmdt': frmdt} )
                if result.empty:
                    print( 'save_nav_history_data for date: ', frmdt )
                    pr = ParseNavData()
                    nav_details_df = pr.get_nav_history( search_param )
                    print( f'Nav Date: {frmdt}, no of rows collected : {nav_details_df.shape[0]}' )
                    nav_details_df.to_sql( 'nav_details', con=self.db.get_engine(), chunksize=2000, if_exists='append',
                                           index=False )
                    print( f'Nav Date: {frmdt}, no of rows saved : {nav_details_df.shape[0]}..' )
                    del nav_details_df
                    print( f'nav data collected for : {frmdt}..' )
                    no_of_days += 1
                else:
                    msg = f'Nav data already available for date: {frmdt}'
                dt_obj = dt_obj + datetime.timedelta( days=1 )
            if no_of_days > 0:
                msg = f'Nav data collected for {no_of_days} starting from {frmdt}.'
            success = True
        except Exception:
            msg = f'Failed to collect Nav data for nav_date: {frmdt}'
            print( msg )
            traceback.print_exc()
        print( msg )
        return {'message': msg, 'success': success}

    def find_nav_history_data(self):
        sql = 'select * from nav_details'
        result_df = pd.read_sql_query( sql, con=self.db.get_engine() )
        result_df.to_csv( 'E:/office work/NavHistory.csv', index=False )

    def get_last_tr_date(self):
        # tr_date_sql = '''select min(tr_date) nav_date from nav_details'''
        tr_date_sql = '''select tr_date as nav_date from nav_details where nav_data_id in 
                                    (select min(nav_data_id) from nav_details)'''
        date_df = pd.read_sql_query( sql=tr_date_sql, con=self.db.get_engine() )
        last_nav_date = None if date_df.empty else date_df['nav_date'].values[0]
        return last_nav_date


# nd = NavDataRepository()
# # # search_params = {'source': 'file'}
# # # search_params = {'frmdt': '31-Jan-2019'}
# search_params = {}
# nd.save_nav_history_data( search_params )
# nd.find_nav_history_data()
