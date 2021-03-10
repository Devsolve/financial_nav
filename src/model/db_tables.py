# create table with following columns:
# nav_data_id         number primarykey
# sch_code            number
# mf_name             String2(200)
# sch_name            String2(300)
# isin_payout         String2(50)
# isin_reinv          String2(50)
# nav_value           number      not nll
# purchase_amt        number
# sell_amt            number
# tr_date             date        not null
# fund_status_type    String2(200)
# scheme_type         String2(200)
# fund_type           String2(200)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, INTEGER, String, DATE, FLOAT, DATETIME
from src.config.db_config import DbConfig

dc = DbConfig()

Base = declarative_base()


class nav_details( Base ):
    __tablename__ = 'nav_details'
    nav_data_id = Column( INTEGER, primary_key=True )
    tr_date = Column( DATE, nullable=False )
    sch_code = Column( String( 50 ) )
    mf_name = Column( String( 200 ) )
    sch_name = Column( String( 250 ) )
    isin_payout = Column( String( 50 ) )
    isin_reinv = Column( String( 50 ) )
    nav_value = Column( FLOAT, nullable=False )
    purchase_amt = Column( FLOAT )
    sell_amt = Column( FLOAT )
    fund_status_type = Column( String( 200 ) )
    scheme_type = Column( String( 200 ) )
    fund_type = Column( String( 200 ) )


class scheme_detail( Base ):
    __tablename__ = 'scheme_detail'
    scheme_code = Column(String( 50 ), primary_key=True)
    company_id = Column( INTEGER, nullable=False )
    scheme_name = Column( String(250), nullable=False,unique=True )
    isin = Column(String(30))
    scheme_description = Column( String )
    added_on = Column( DATETIME )
    is_active = Column( String, nullable=False )
    updated_on = Column( DATETIME )


class scheme_type( Base ):
    __tablename__ = 'scheme_type'
    sch_type_id = Column( INTEGER, primary_key=True )
    sch_type_short_name = Column( String, nullable=False )
    sch_type_name = Column( String, nullable=False )
    sch_type_description = Column( String )


class company_info( Base ):
    __tablename__ = 'company_info'
    company_id = Column( INTEGER, primary_key=True )
    company_name = Column( String, nullable=False , unique=True)
    company_description = Column( String )
    added_on = Column( DATETIME )


class daily_nav( Base ):
    __tablename__ = 'daily_nav'
    nav_id = Column( INTEGER, primary_key=True )
    scheme_code = Column( String( 50 ))
    sch_type_id = Column( INTEGER )
    nav_value = Column( FLOAT, nullable=False )
    carry_forward = Column( String )
    purchase_amt = Column( FLOAT )
    sell_amt = Column( FLOAT )
    added_on = Column( DATETIME )



# Base.metadata.drop_all( dc.get_engine() )
# Base.metadata.create_all( dc.get_engine() )
