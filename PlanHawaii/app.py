###################################################
############### Dependencies ###############
###################################################
# Import Flask and jsonify
from flask import Flask, jsonify

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect, func
from sqlalchemy import Column, Integer, String, Float

import numpy as np
import pandas as pd

import datetime as dt
from dateutil.parser import parse

###################################################
############### Create an Flask app ###############
###################################################
app = Flask(__name__)

###################################################
########### Database Setup, automap base ##########
###################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

############################################
############### Flask Routes ###############
############################################

# index Route
@app.route("/")
def welcome():
    print("Server received request for 'welcome' page...")
    return (
        f"Welcome to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )

# precipitation Route
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Number of years data needed
    years_needed = 1

    # get date range using the function date_range
    last_date_st, one_year_ago_st = date_range(years_needed)

    # Perform a query to retrieve the data and precipitation scores
    OneYearPrcpData = session.query(Measurement.date, Measurement.prcp). \
    filter(Measurement.date>=one_year_ago_st). \
    filter(Measurement.date<=last_date_st). \
    order_by(Measurement.date).all()    

    # close session
    session.close()

    # Store data as dictionary of date and prcp as key, value pair 
    prcp_record ={}
    for date, prcp in OneYearPrcpData:
        prcp_record[date] = prcp

    return jsonify(prcp_record)

# station Route
@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # List the stations.
    sel = [Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation]
    stations_list = session.query(*sel). \
    order_by(Station.station). \
    all()

    # close session
    session.close()  

    # Create Station Data dictionary    
    stations_record = []
    for station, name, latitude, longitude, elevation in stations_list:
        station_dict = {}
        station_dict["station"] = station
        station_dict["name"] = name
        station_dict["latitude"] = latitude
        station_dict["longitude"] = longitude
        station_dict["elevation"] = elevation
        stations_record.append(station_dict)
    
    return jsonify(stations_record)

# Tobs
@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #get most active station, using fav_station
    most_active_station = fav_station()

    # Number of years data needed
    years_needed = 1
    # get date range using the function date_range
    last_date_st, one_year_ago_st = date_range(years_needed)

    # Perform a query to retrieve the data and precipitation scores
    OneYearTempData = session.query(Measurement.date, Measurement.tobs). \
    filter(Measurement.date>=one_year_ago_st). \
    filter(Measurement.date<=last_date_st). \
    order_by(Measurement.date).all()    

    # close session
    session.close()

    # Store data as dictionary of date and prcp as key, value pair 
    tobs_record ={}
    for date, tobs in OneYearTempData:
        tobs_record[date] = tobs

    mas_dict = {}    
    mas_dict[most_active_station] = tobs_record

    return jsonify(mas_dict)


###################################################
####### Last Measured Date, date (n) years ago ######
###################################################
def date_range(nYearAgo):
    
    """
    Args:
        nYearAgo (integer): number of years to subtract from last measured date 
    Returns:
        last_date_st (string): last date measured in %Y-%m-%d
        n_year_ago_st (string): date n years from last measured date in  %Y-%m-%d  
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Get the last date the measurement recorded.
    last_date_record = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    # close session
    session.close()
    # Get last date as date type
    last_date = parse(last_date_record[0])
    # Calculate the date 1 year ago from the last data point in the database
    n_year_ago = last_date.replace(year=last_date.year-nYearAgo) + dt.timedelta(days=1)
    # convert to date calculated to string, as the data is stored as string
    last_date_st = last_date.strftime('%Y-%m-%d')
    n_year_ago_st = n_year_ago.strftime('%Y-%m-%d')
    
    date_range = (last_date_st, n_year_ago_st)

    return date_range

# Favorite Station / Most Active Station
def fav_station():
    """
    Args:
        No args
    Returns:
        most_active_station (string): returns the most active station, based on measurement counted
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)

    station_byCount = session.query(Station.station,Station.name). \
    filter(Station.station == Measurement.station). \
    group_by(Station.station,Station.name). \
    order_by(func.count(Measurement.id).desc()).first()

    # close session
    session.close()
    # extract the station from query result
    most_active_station = station_byCount[0]

    return most_active_station

if __name__ == "__main__":
    app.run(debug=True)