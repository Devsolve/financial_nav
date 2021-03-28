import os
import numpy as np
import pandas as pd
import requests
from src.constant.AppConst import *


class ParseNavData:

    def __init__(self):
        self.headers = ['Scheme Code', 'Scheme Name', 'ISIN Div Payout/ISIN Growth', 'ISIN Div Reinvestment',
                        'Net Asset Value', 'Repurchase Price', 'Sale Price', 'Date']

    def get_nav_history(self, search_param):
        source = search_param['source'] if 'source' in search_param else ''
        print( 'get_nav_history from {}'.format( source ) )
        lines = []
        if source == SOURCE:
            # nav_file = 'E:\office work\financial_nav\src\resources\data\jan1_19.txt'
            nav_file = os.path.abspath(
                os.path.join( os.path.dirname( __file__ ), '..', 'resources', 'data', 'jan1_19.txt' ) )
            content = open( nav_file )
            lines = content.readlines()
            print( "Here is the list: {}".format( lines ) )
        else:
            # portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Jan-2019&todt=10-Jan-2019
            frmdt = search_param['frmdt']
            todt = frmdt
            # search_param['todt']
            # url = AMIINDIA_URL+'frmdt='+frmdt+'&todt='+todt
            url = AMIINDIA_URL.format( frmdt, todt )
            print( 'get_nav_history for url {}'.format( url ) )
            resp = requests.get( url )
            if resp.status_code == 200:
                res_text = resp.text
                lines = res_text.splitlines()
            else:
                print( '*******Failed to find data for request: ', url )
        print( 'number of lines to parse: {}'.format( len( lines ) ) )
        nav_detail_df = self.__parse_nav_detail( lines )
        print( 'number of rows & columns after parsing: {}'.format( nav_detail_df.shape ) )
        return nav_detail_df

    def __parse_nav_detail(self, lines):
        print( 'parse nav details...' )
        sch_code = []
        mf_name = []
        sch_name = []
        isin_payout = []
        isin_reinv = []
        nav_value = []
        purchase_amt = []
        sell_amt = []
        tr_date = []
        fund_status_type = []
        scheme_type = []
        fund_type = []

        mutual_fund_name = ''
        each_fund_status_type = ''
        each_scheme_type = ''
        each_fund_type = ''

        for line in lines:
            if line and len( line.strip() ) > 0:
                # header
                if line.startswith( 'Scheme Code;Scheme Name;' ):
                    continue
                # mutual fund
                elif ';' not in line and '(' not in line and ')' not in line:
                    mutual_fund_name = line

                # Open Ended Schemes ( Equity Scheme - Large Cap Fund )
                # fund_status_type - scheme_type - fund_type
                elif ';' not in line and '(' in line and ')' in line:
                    each_fund_status_type = line[0:line.index( '(' )]
                    sch_fund_type = line[line.index( '(' ) + 1:line.index( ')' ) - 1]

                    if '-' in sch_fund_type:
                        each_scheme_type = sch_fund_type[: sch_fund_type.index( '-' ) - 1]
                        each_fund_type = sch_fund_type[sch_fund_type.index( '-' ) + 1:]

                    else:
                        each_scheme_type = ''
                        each_fund_type = sch_fund_type


                # values
                else:
                    scheme_cd = int( self.__value_by_header( line, 'Scheme Code' ) )
                    if scheme_cd in SCHEME_CODES:
                        sch_code.append( scheme_cd )
                        sch_name.append( self.__value_by_header( line, 'Scheme Name' ) )
                        isin_payout.append( self.__value_by_header( line, 'ISIN Div Payout/ISIN Growth' ) )
                        isin_reinv.append( self.__value_by_header( line, 'ISIN Div Reinvestment' ) )
                        nav_value.append( self.__num_value_by_header( line, 'Net Asset Value' ) )
                        purchase_amt.append( self.__num_value_by_header( line, 'Repurchase Price' ) )
                        sell_amt.append( self.__num_value_by_header( line, 'Sale Price' ) )
                        tr_date.append( self.__value_by_header( line, 'Date' ) )
                        mf_name.append( mutual_fund_name.strip() )
                        scheme_type.append( each_scheme_type.strip() )
                        fund_type.append( each_fund_type.strip() )
                        fund_status_type.append( each_fund_status_type.strip() )

        nav_detail_df = pd.DataFrame( {'sch_code': np.array( sch_code ), 'sch_name': np.array( sch_name )
                                          , 'isin_payout': np.array( isin_payout ), 'isin_reinv': np.array( isin_reinv )
                                          , 'nav_value': np.array( nav_value, dtype='float' ),
                                       'purchase_amt': np.array( purchase_amt )
                                          , 'sell_amt': np.array( sell_amt ),
                                       'tr_date': np.array( tr_date, dtype='object' )
                                          , 'mf_name': np.array( mf_name ),
                                       'fund_status_type': np.array( fund_status_type )
                                          , 'scheme_type': np.array( scheme_type ), 'fund_type': np.array( fund_type )
                                       } )

        # print( nav_detail_df.head( 10 ).to_csv( index=False ) )

        return nav_detail_df

    def __value_by_header(self, line, header):
        values = line.rstrip( '\n' ).split( ';' )
        header_index = self.headers.index( header )
        value = values[header_index].strip() if values[header_index] else ''
        return value

    def __num_value_by_header(self, line, header):
        values = line.rstrip( '\n' ).split( ';' )
        header_index = self.headers.index( header )
        value = values[header_index]
        val = 0.0
        try:
            if value:
                val = float( value.strip() )
        except Exception:
            val = 0.0

        # print(f'***header: {header}, val1: {value}, val2: {val}, digit: {value.isdigit()}')
        # return 0.0 if len( value.strip() ) == 0 or not value.isdigit() else value
        return val

# p = ParseNavData()
# paramss = {'source': 'api', 'frmdt': '01-Jan-2019', 'todt': '01-Jan-2019'}
# p.get_nav_history( paramss )
