from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, INTEGER, String, DATE, FLOAT, DATETIME, BIGINT
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


class scheme_type( Base ):
    __tablename__ = 'scheme_type'
    sch_type_id = Column( INTEGER, primary_key=True )
    sch_type_short_name = Column( String, nullable=False )
    sch_type_name = Column( String, nullable=False )
    sch_type_description = Column( String )
    added_on = Column( DATETIME )


class FundType( Base ):
    __tablename__ = 'fund_type'
    fund_type_id = Column( INTEGER, primary_key=True )
    fund_type_name = Column( String )
    description = Column( String )
    added_on = Column( DATETIME )


class company_info( Base ):
    __tablename__ = 'company_info'
    company_id = Column( INTEGER, primary_key=True )
    company_name = Column( String, nullable=False, unique=False )
    company_description = Column( String )
    added_on = Column( DATETIME )


class scheme_detail( Base ):
    __tablename__ = 'scheme_detail'
    scheme_id = Column( INTEGER, primary_key=True )
    scheme_code = Column( String( 50 ) )
    scheme_name = Column( String( 250 ), nullable=False, unique=False )
    company_id = Column( INTEGER, nullable=False )
    sch_type_id = Column( INTEGER )
    fund_type_id = Column( INTEGER )
    isin = Column( String( 30 ) )
    scheme_description = Column( String )
    added_on = Column( DATETIME )
    is_active = Column( String, nullable=False )
    updated_on = Column( DATETIME )


class daily_nav( Base ):
    __tablename__ = 'daily_nav'
    nav_id = Column( INTEGER, primary_key=True, autoincrement=True )
    scheme_id = Column( INTEGER )
    nav_value = Column( FLOAT, nullable=False )
    purchase_amt = Column( FLOAT )
    sell_amt = Column( FLOAT )
    nav_date = Column( DATE, nullable=False )
    last_nav_date = Column( DATE )
    carry_forward = Column( String )


# Base.metadata.drop_all( dc.get_engine() )
# Base.metadata.create_all( dc.get_engine() )
