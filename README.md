# Forecast
Assisting script: Parses data retrieved from an FTP server and stores weather data in the Postgres DB.

### To run the script locally
Import the project into IDEA and set the Project SDK.<br/><br/>
Then Build > Make Project<br/>
Then Run the Project.<br/>
In the main class (src > ForecastParser) we choose the correct value for the variable **propertiesPath**.

### To rebuild the JAR Artifact
This is the source code of the script that runs in a crontab process in the server. In order to make changes, rebuild the artifact and upload the .jar output file on the server path (as defined in crontab).
