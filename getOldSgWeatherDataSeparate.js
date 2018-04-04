const request = require('request'),
fs = require('fs'),
moment = require('moment'),

airTemperature = {
    "uri": "air-temperature",
    "name": "Air Temperature",
    "dbTable": "SGData_AirTemperature",
    "dbColumn": "airTemperature_celsius"
},
windSpeed = {
    "uri": "wind-speed",
    "name": "Wind Speed",
    "dbTable": "SGData_WindSpeed",
    "dbColumn": "windSpeed_knots"
},
windDirection = {
    "uri": "wind-direction",
    "name": "Wind Direction",
    "dbTable": "SGData_WindDirection",
    "dbColumn": "windDirection_degrees"
},
relativeHumidity = {
    "uri": "relative-humidity",
    "name": "Relative Humidity",
    "dbTable": "SGData_RelativeHumidity",
    "dbColumn": "relativeHumidity_percentage"
},
rainfall = {
    "uri": "rainfall",
    "name": "Rainfall",
    "dbTable": "SGData_Rainfall",
    "dbColumn": "rainfall_mm"
};

const { Client } = require ('pg');
const client = new Client({
    user: "postgres",
    host: "localhost",
    database: "database",
    password: "password",
    port: "5432"
});

var weatherDataSet = [];
weatherDataSet.push(airTemperature);
weatherDataSet.push(windSpeed);
weatherDataSet.push(windDirection);
weatherDataSet.push(relativeHumidity);
weatherDataSet.push(rainfall);

AirportStationCode = "S24"; // Change to whichever preferred; S24 is for Changi Airport

var responseBodyJson;

var dateStr;

var startTime;

const numOfMsecInFiveMin = 5 * 60 * 1000;

client.connect();

function getData() {

    var getMoment = moment(time.getTime());
    var getDateTime = getMoment.format("YYYY-MM-DD[T]HH:mm:ss");
    var dbTimeStamp = getMoment.format("YYYY-MM-DD HH:mm:ss");
    getDateTimeEncodeURI = encodeURIComponent(getDateTime);

    var timeNow = new Date();

    if (timeNow.getTime() < time.getTime()) {
        console.log("Updated to current time.");
        process.exit(0);
    }
    
    logToDataFile("----------------------");
    logToDataFile("Data retrieved at " + getDateTime + ":- ");

    for (var dataType in weatherDataSet) {
        request.get("https://api.data.gov.sg/v1/environment/" + weatherDataSet[dataType]["uri"] + "?date_time=" + getDateTimeEncodeURI, function (error, response, body) {
            if (error) {
                console.log("Error.");
                console.log(error);
                logError(error);
                process.exit();
            }
            responseBodyJson = JSON.parse(body);
        
            var getDataTypeReadings = responseBodyJson.items[0].readings; // array of readings
        
            var cgaDataTypeValue;
        
            for (var i = 0; i < getDataTypeReadings.length; i++) {
                if ((getDataTypeReadings[i])["station_id"] === AirportStationCode) {

                    if ((getDataTypeReadings[i])["value"] === undefined) {
                        cgaDataTypeValue = "NULL";
                    } else {
                        cgaDataTypeValue = (getDataTypeReadings[i])["value"];
                    }

                    logToDataFile(weatherDataSet[this.dataType]["name"] + ": " + cgaDataTypeValue + " " + responseBodyJson.metadata.reading_unit);

                    updateDatabase(dbTimeStamp, weatherDataSet[this.dataType]["dbTable"],  weatherDataSet[this.dataType]["dbColumn"], cgaDataTypeValue);

                    break;
                }

                if (i == (getDataTypeReadings.length - 1)) {
                    console.log("No data for " + weatherDataSet[this.dataType]["name"] + " for " + getDateTime);
                }
            }

        }.bind( { dataType: dataType }));
    }

    var newTime = time.getTime() + numOfMsecInFiveMin;
    time.setTime(newTime); // increment by every 5 minutes
    var newTimeFormat = moment(newTime).format("YYYY-MM-DD[T]HH:mm:ss");
    fs.writeFile("time.txt", newTimeFormat, function (err) {
        if (err) {
            throw err;
        }
    })
}

function logToDataFile(content) {
    fs.appendFile("data.log", content + "\r\n", function (err) {
        if (err) throw err;
    });
}

function logError(err) {
    fs.appendFile("error.log", "ERROR at time " + (new Date()).toISOString() + "!" + "\r\n");
    fs.appendFile("error.log", "Message: " + err.message + "\r\n");
    fs.appendFile("error.log", "Stack: " + err.stack + "\r\n");
}

function updateDatabase(timestamp, table, column, value) {
    //console.log("Updating database...");        
    var queryStr = "INSERT INTO \"" + table + "\"(\"timestamp\", \"" + column + "\") VALUES ('" + timestamp + "', '" + value + "');";
    client.query(queryStr, (err, res) => {
        console.log("Insert query statement: " + queryStr);
        if (err) {
            console.log(err);
            logError(err)
        }
    });
}

fs.readFile("time.txt", function (err, data) {
    if (err) {
        throw err;
        logError(err)
        process.exit();
    }
    dateStr = data;
    startTime = new Date(dateStr);
    time = startTime;
    getData();
    setInterval(getData, 1000);
    var getDateTime = moment().format("YYYY-MM-DD[T]HH:mm:ss");
    fs.writeFile("data.log", "Started process at " + getDateTime + ".\r\n", function (err) {
        if (err)
        {
            logError(err);
            throw err;
        }
    });
});