import datetime
from src.config.db_config import DbConfig
from src.parser.parse_nav_data import ParseNavData
import pandas as pd


class NavDataRepository:

    def __init__(self):
        self.db = DbConfig()

    def save_nav_history_data(self, search_param):
        print('save_nav_history_data for search_param: ', search_param)
        msg = ''
        success = False
        frmdt = search_param['frmdt']
        dt_obj = datetime.datetime.strptime(frmdt, '%d-%b-%Y')
        today = datetime.datetime.today()
        prevday = today - datetime.timedelta(days=1)

        while dt_obj <= prevday:
            frmdt = dt_obj.strftime('%d-%b-%Y')
            search_param = {'frmdt': frmdt, 'todt': frmdt}
            sql = 'select sch_code from nav_details where tr_date= :frmdt'
            result = pd.read_sql_query(sql=sql, con=self.db.get_engine(), params={'frmdt': frmdt})
            if result.empty:
                print( 'save_nav_history_data for date: ', frmdt )
                pr = ParseNavData()
                nav_details_df = pr.get_nav_history( search_param )
                print( 'nav_details_df :' )
                print( nav_details_df )

                nav_details_df.to_sql( 'nav_details', con=self.db.get_engine(), chunksize=2000, if_exists='append',
                                       index=False )
                print( 'nav history saved for : ', search_param )
                msg = 'Data saved for date: {}'.format( frmdt )
                success = True
            else:
                msg = 'Data already available for date: ' + frmdt
            dt_obj = dt_obj + datetime.timedelta( days=1 )
        print( msg )
        return {'message': msg, 'success': success}

    def find_nav_history_data(self):
        sql = 'select * from nav_details'
        result_df = pd.read_sql_query( sql, con=self.db.get_engine() )
        result_df.to_csv( 'E:/office work/NavHistory.csv', index=False )

# nd = NavDataRepository()
# search_params = {'source': 'file'}
# search_params = {'frmdt': '01-Jan-2019', 'source': 'file'}
# nd.save_nav_history_data( search_params )
# nd.find_nav_history_data()
