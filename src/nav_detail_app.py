from datetime import datetime
from flask import Flask, request
from flask_cors import CORS


from src.repository.daily_nav_repository import insert_daily_nav
from src.repository.nav_data_repository import NavDataRepository
import json

app = Flask( __name__ )
cors = CORS( app )


# @app.route( '/', methods=['POST'] )
# def persist_nav_detail():
#     search_params_json = request.get_json()
#     print('***search_params_json: {}'.format( search_params_json ) )
#     nv = NavDataRepository()
#     res = nv.save_nav_history_data( search_params_json )
#     return res

@app.route( '/collect_nav_data', methods=['GET'] )
def collect_nav_data():
    nv = NavDataRepository()
    res = nv.save_nav_history_data()
    return res


@app.route( '/save_daily_nav_detail/<no_of_days>', methods=['GET'] )
def save_nav_detail(no_of_days):
    print( f'persist_nav_detail for [{no_of_days}] days..' )
    res = insert_daily_nav( no_of_days )
    return res


if __name__ == '__main__':
    app.run( debug=True, port=4050 )
