from datetime import datetime
from flask import Flask, request
from flask_cors import CORS
from src.repository.nav_data_repository import NavDataRepository
import json

app = Flask( __name__ )
cors = CORS( app )


@app.route( '/', methods=['POST'] )
def persist_nav_detail():
    search_params_json = request.get_json()
    print( '***search_params_json: {}'.format( search_params_json ) )
    nv = NavDataRepository()
    res = nv.save_nav_history_data( search_params_json )
    return res


if __name__ == '__main__':
    app.run( debug=True, port=4050 )
