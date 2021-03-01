# create DbConfig class
# add 2 methods get_engine(), get_session()
# sqlite database name nav_history_sqlite

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os


class DbConfig:

    def __init__(self):
        self.db_path = os.path.abspath(
            os.path.join( os.path.dirname( __file__ ), '..', 'resources', 'nav_history_sqlite.db' ) )
        print( 'db path: ', self.db_path )
        self.engine = create_engine( 'sqlite:///' + self.db_path, echo=True )

    def get_engine(self):
        return self.engine

    def get_session(self):
        Session = sessionmaker( bind=self.engine, autocommit=True )
        return Session()
