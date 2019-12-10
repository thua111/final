--Create directories
-bash-4.2$  hdfs dfs –mkdir sofia
-bash-4.2$ hdfs dfs -mkdir sofia/2017
-bash-4.2$ hdfs dfs -mkdir sofia/2018 
-bash-4.2$ hdfs dfs -mkdir sofia/2019

 
--Put 2017 data to sofia/2017.
Hdfs dfs –put 2017***.csv sofia/2017 
--Do the same with 2018 and 2019 data
--check if files are correctly in relevant folder
Hdfs dfs –ls sofia/201* 

--Load dataset
grunt> sofia = LOAD 'user/yourusername/sofia/2017/201***.csv' USING PigStorage(',') AS ( sensor_id:int, location:int, lat: double, lon:double, timestamp:datetime, pressure:double, temperature:double, humidity:double);
grunt>sofia = LOAD 'user/yourusername/sofia/2018/201***.csv' USING PigStorage(',') AS ( sensor_id:int, location:int, lat: double, lon:double, timestamp:datetime, pressure:double, temperature:double, humidity:double);
grunt>sofia = LOAD 'user/yourusername/sofia/2019/201***.csv' USING PigStorage(',') AS ( sensor_id:int, location:int, lat: double, lon:double, timestamp:datetime, pressure:double, temperature:double, humidity:double); 

--describe dataset
describe ssofia;

--sort data for top 10 hotest place

hotplace = ORDER location by temperature DESC; 
top10hot = limit hotplace 10; 
dump top10hot; 

--sort for top 10 coldest place
coldplace = ORDER location BY temperature ASC;
top10cold = limit coldplace 10; 
dump top10cold;

--sort for top 10 most humid place
humidplace= ORDER location by humidity DESC; 
top10humid = limit humidplace 10; 
dump top10humid; 