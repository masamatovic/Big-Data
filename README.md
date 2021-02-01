# Big-Data
Student project for batch and real time processing. The main goal of the project is to process the calculation of the Air Quality Index(AQI).
### BATCH PROCESSING

  Datasets for air quality are downloaded [here](https://www.kaggle.com/ruben99/air-pollution-dataset-india20162018?select=2018_india.csv) <br />
  Dataset for mortality in Chennai are downloaded [here](https://www.kaggle.com/sujays/chennai-corporation-death-count-2011-to-june-2020)  <br />
  Goals of batch processing:
  - Calculation of daily AQI for cities in India based on parameters pm25, pm10, no2, so2, o3, co. Calculated for 2016 and 2017.
  - For the cities of Chennai, Mumbai, Delhi, Hyderabad calculate the number of days in 2017 with a certain AQI
  - Comparison of the number of days with a certain AQI and the number of deaths per month in Chennai in 2016

### STREAM PROCESSING
Goals of stream processing:
  * Acquire data from [API](https://aqicn.org/api/) and send it every 30s
  * Calculating the average AQI every three minutes and displaying the appropriate message for that level of pollution
# run docker
```
$ cd docker-specification
$ docker-compose up --build
```
# put datasets on hdfs
* unzip datasets.zip 
* in root directory run
```
$ ./run.sh
```
# run batch processing
```
$ cd batch
$ ./run.sh
```
# save results of bacth processing as png
```
$ cd results
$ ./run.sh
```
# run stream processing
```
$ cd stream
$ ./run.sh
```
