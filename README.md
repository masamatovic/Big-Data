# Big-Data
Student project for batch and real time processing. The main goal of the project is to process the calculation of the Air Quality Index(AQI).
### BATCH PROCESSING
  Goals of batch processing:
    1. Calculation of daily AQI for cities in India based on parameters pm25, pm10, no2, so2, o3, co. Calculated for 2016 and 2017.
    2. For the cities of Chennai, Mumbai, Delhi, Hyderabad calculate the number of days in 2017 with a certain AQI
    3. Comparison of the number of days with a certain AQI and the number of deaths per month in Chennai in 2016 
  Datasets for air quality are downloaded [here](https://www.kaggle.com/ruben99/air-pollution-dataset-india20162018?select=2018_india.csv)
  Dataset for mortality in Chennai are downloaded [here](https://www.kaggle.com/sujays/chennai-corporation-death-count-2011-to-june-2020)
### STREAM PROCESSING
  Goals of stream processing:
    1. Acquire data from [API](https://aqicn.org/api/) and send it every 30s
    2. Calculating the average AQI every three minutes and displaying the appropriate message for that level of pollution
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
