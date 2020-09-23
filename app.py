import numpy as np
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine( "sqlite:///C:\\db\\hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
measurments = Base.classes.measurement
station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/(start)<br/>"
        f"/api/v1.0/(start)/(end)<br/>"
    )


@app.route("/api/v1.0/precipitation")
def prcps():

    # Convert the query results to a dictionary using date as the key and prcp as the value.

    # Create our session (link) from Python to the DB
    session = Session(engine)

   
    # Query all dates and paticipations
    results = session.query(measurments.date, measurments.prcp).all()

    session.close()

    # Create a dictionary of the dates and participations
    prcps = dict((date, prcp) for date, prcp in results)

    return jsonify(prcps)

@app.route("/api/v1.0/stations")
def stations():

    # Return a JSON list of stations from the dataset.

    from sqlalchemy import distinct

    session = Session(engine)

    results = session.query(distinct(station.station)).all()

    session.close()

    stations = list(np.ravel(results))

    print(stations)

    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def tobs():

    # Query the dates and temperature observations of the most active station for the last year of data.

    # Return a JSON list of temperature observations (TOBS) for the previous year.

    session = Session(engine) 

    ########## Determine the most active station #############

    station_count = func.count(measurments.station).label('Count')   

    results = session.query(measurments.station,station_count)\
    .group_by(measurments.station)\
    .order_by(station_count.desc())\
    .limit(1).all()

    mactive_station = results[0][0]

    ########## Determine records' Year to Date #############

    # Calculate the date 1 year ago from the last data point in the database
    LastDate = session.query(func.max(measurments.date)).all()[0][0]
    LastDate = dt.datetime.strptime(LastDate, '%Y-%m-%d')
    Delta = dt.timedelta(days=365)

    # calculate year to date
    YTD = LastDate - Delta

    ########## Retrieve desired info from the measurments table #############
    
    results = session.query(measurments.date, measurments.tobs).\
    filter(measurments.station == mactive_station).\
    filter(measurments.date > YTD).all()


    session.close


    # Create a dictionary of the dates and participations
    tobs_dict = dict((date, tobs) for date, tobs in results)


    # Return the tobs dictionary
    return jsonify(tobs_dict)

@app.route("/api/v1.0/<start>")
def temp_start(start):

    # Return a JSON list of the minimum temperature, the average temperature, 
    # and the max temperature for a given start or start-end range.

    # When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.

    # Perform a query to retrieve the data and precipitation scores

    # Create aggregation functions
    MinTemp = func.min(measurments.tobs).label('TMIN')
    MaxTemp = func.max(measurments.tobs).label('TMAX')
    MeanTemp = func.round(func.avg(measurments.tobs)).label('TAVG')

    # Query the TMIN, TAVG, and TMAX 

    session = Session(engine) 

    results = session.query(MinTemp, MaxTemp, MeanTemp )\
    .filter(measurments.date > start).all()[0]

    session.close

     # Return results in JSON format

    return jsonify( {

        'Start Date': start,
        'TMIN': results[0],
        'TMAX': results[1],
        'TAVG': results[2]
    })


@app.route("/api/v1.0/<start>/<end>")
def temp_startend(start,end):

    # Return a JSON list of the minimum temperature, the average temperature, 
    # and the max temperature for a given start or start-end range.

    # When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.

    # Perform a query to retrieve the data and precipitation scores

    # Create aggregation functions
    MinTemp = func.min(measurments.tobs).label('TMIN')
    MaxTemp = func.max(measurments.tobs).label('TMAX')
    MeanTemp = func.round(func.avg(measurments.tobs)).label('TAVG')

    # Query the TMIN, TAVG, and TMAX 

    session = Session(engine) 

    results = session.query(MinTemp, MaxTemp, MeanTemp )\
    .filter(measurments.date > start).filter(measurments.date < end).all()[0]

    session.close

     # Return results in JSON format

    return jsonify( {

        'Start Date': start,
        'End Date': end,
        'TMIN': results[0],
        'TMAX': results[1],
        'TAVG': results[2]
    })



if __name__ == '__main__':
    app.run(debug=True)
