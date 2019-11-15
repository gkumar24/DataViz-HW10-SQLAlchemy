############### Dependencies ###############
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

############### Create an Flask app ###############
app = Flask(__name__)

########### Database Setup, automap base ##########

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
# reflect the tables
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

############### Flask Routes ###############
###################### Index Route ################
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

############### precipitation Route ################
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Number of years data needed
    years_needed = 1

        # Get last date measured, and convert it to 
    last_date_record = last_measured_date()    

    # Calculate the date 1 year ago from the last data point in the database
    # and convert to date calculated to string, as the data is stored as string
    last_date = parse(last_date_record) 
    n_year_ago = last_date.replace(year=last_date.year-years_needed) + dt.timedelta(days=1)
    n_year_ago_st = n_year_ago.strftime('%Y-%m-%d')

    # Perform a query to retrieve the data and precipitation scores
    OneYearPrcpData = session.query(Measurement.date, Measurement.prcp). \
    filter(Measurement.date>=n_year_ago_st). \
    filter(Measurement.date<=last_date_record). \
    order_by(Measurement.date).all()    

    # close session
    session.close()

    # Store data as dictionary of date and prcp as key, value pair 
    prcp_record ={}
    for date, prcp in OneYearPrcpData:
        prcp_record[date] = prcp
    
    return jsonify(prcp_record)
    

    
############### station Route ################
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

############### Temperature Route ################
@app.route("/api/v1.0/tobs")
def tobs():
    # Number of years data needed
    years_needed = 1

    # Create our session (link) from Python to the DB
    session = Session(engine)

    #get most active station, using fav_station
    most_active_station = fav_station()

    # Get last date measured, and convert it to 
    last_date_record = last_measured_date()    

    # Calculate the date 1 year ago from the last data point in the database
    # and convert to date calculated to string, as the data is stored as string
    last_date = parse(last_date_record) 
    n_year_ago = last_date.replace(year=last_date.year-years_needed) + dt.timedelta(days=1)
    n_year_ago_st = n_year_ago.strftime('%Y-%m-%d')

    # Perform a query to retrieve the data and precipitation scores
    OneYearTempData = session.query(Measurement.date, Measurement.tobs). \
    filter(Measurement.date>=n_year_ago_st). \
    filter(Measurement.date<=last_date_record). \
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

############### Temperature Statistics ################
@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def Tobs_Statistics(start=None, end=None):
    #Validate the entered Date
    valueErrorDict = {"Error":"Value Error",
                "Error Description":"Invalid or unknown string format",
                "Suggested Format":"YYYY-MM-DD"}

    overflowErrorDict =  {"Error":"OverflowError ",
                "Error Description":"parsed date exceeds the largest valid date in the system",
                "Suggested Format":"YYYY-MM-DD"}
    if start is not None:
        try:
            start_dt = parse(start).strftime('%Y-%m-%d')
        except ValueError:
            valueErrorDict["Entered Start Date"]=start_date
            return jsonify(valueErrorDict)
        except OverflowError:
            valueErrorDict["Entered Start Date"]=start_date
            return jsonify(overflowErrorDict)
        except:
            return jsonify({"Error":"Unknown"})

    if end is not None:
        try:
            end_dt = parse(end).strftime('%Y-%m-%d')
        except ValueError:
            valueErrorDict["Entered End Date"]=end_date
            return jsonify(valueErrorDict)
        except OverflowError:
            valueErrorDict["Entered End Date"]=end_date
            return jsonify(overflowErrorDict)
        except:
            return jsonify({"Error":"Unknown"})

    # Get last date measured, and convert it to 
    last_date_record = last_measured_date()
    # last_date = parse(last_date_record)
    
    if start_dt > last_date_record:
        dataUnavailDict = {"Exception":"Data unavailable",
                        "Last Measured Date":last_date_record,
                        "Suggestion":"Try date earlier than last measured date and end date"
                        }
        return jsonify(dataUnavailDict)    

    if end is None:
        end_dt = last_date_record

    tmin, tavg, tmax = calc_temps(start_dt, end_dt)[0]

    result_dict = { "Start Date":start_dt,
                    "End Date":end_dt, 
                    "Minimum Temperature" : tmin, 
                    "Maximum Temperature" : tmax,
                    "Average Temperature" : tavg
    }
    return jsonify(result_dict)

############### Custom Functions ################
############### Last Measured Dates ################
def last_measured_date():    
    """
    Args:
        No Args
    Returns:
        last_date_record (string): last date measured in %Y-%m-%d
         
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Get the last date the measurement recorded.
    last_date_record = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    # close session
    session.close()
    #Return Value
    return last_date_record[0]

### Get Favorite station / most active station ####
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

### Temperature Statistics for a given range ######
# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    # Create our session (link) from Python to the DB
    session = Session(engine)

    temp_stats = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

    # close session
    session.close()
    #Return Value
    return temp_stats

if __name__ == "__main__":
    app.run(debug=True)