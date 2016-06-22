# Forecast parsing

## Run the script locally
Import the project into IDEA and set the Project SDK.<br/><br/>
Then Build > Make Project<br/>
Then Run the Project.<br/>
In the main class (src > ForecastParser) we choose the correct value for the variable **propertiesPath**.

## Extract to JAR and upload on server
From the IDEA Project Structure menu, we add a new Artifact (JAR > Empty) for the project.<br/>
We build the new artifact (menu Build > Build Artifacts). We upload the output jar e.g. Forecast.jar to the scripts directory in our server and ensure it is enabled in the crontab list.
