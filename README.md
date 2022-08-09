# Sources

## Temperature

Temperature data definition as specified in https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/README_stockholm_daily_temp_obs.txt

## Pressure

Temperature data definition as specified in https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/air_pressure/README_stockholm_pressure.txt

# Output Datasets

## temperatures

root  
 |-- month: integer (nullable = falase)  
 |-- day: integer (nullable = false)  
 |-- t_morn: float (nullable = true), temperature measurement morning, unit: degC  
 |-- t_noon: float (nullable = true), temperature measurement noon, unit: degC  
 |-- t_evn: float (nullable = true), temperature measurement evening, unit: degC  
 |-- tmax: float (nullable = true), max temperature per day, unit: degC  
 |-- tmin: float (nullable = true), min temperature per day, unit: degC  
 |-- tmean: float (nullable = true), mean temperature per day, unit: degC  
 |-- method: string (nullable = true), enum: <"manual","automatic">  
 |-- year: integer (nullable = false)  

## pressure

root  
 |-- month: integer (nullable = false)  
 |-- day: integer (nullable = false)  
 |-- b_morn: double (nullable = true), pressure measurement morning, unit: hPa  
 |-- t_morn: float (nullable = true), temperature measurement morning, unit: degC  
 |-- b_noon: double (nullable = true), pressure measurement noon, unit: hPa  
 |-- t_noon: double (nullable = true), temperature measurement noon, unit: degC  
 |-- b_evn: double (nullable = true), pressure measurement evening, unit: hPa  
 |-- t_evn: double (nullable = true), temperature measurement evening, unit: degC  
 |-- method: string (nullable = true), enum: <"manual","automatic">  
 |-- p_morn_0: float (nullable = true), pressure measurement morning reduced to 0 degC, unit: hPa  
 |-- p_noon_0: float (nullable = true), pressure measurement noon reduced to 0 degC, unit: hPa  
 |-- p_evn_0: string (nullable = true), pressure measurement evening reduced to 0 degC, unit: hPa  
 |-- year: integer (nullable = false)

Assumptions and decisions made:
* "NaN" denotes missing meausre, replaced with None (null)
* expected units: temperatures degC, pressure hPa
* both 'manual' and 'automatic' measurements are placed in the same table, they can be differentied by column 'method'. This must be taken into consideration eg. when checking for duplicates.

# Data Quality

Basic data quality checks are performed in the notebook data_quality.ipynb. Profiling summaries were exported to profiling_reports/{pressure,temperatures}_profile.html
