1.Run build-images.sh 
2.Run docker-compose.

To make sure each worker(node) and the master has the same envionment, 
if you need other python packages such as pandas, you have to run "spark-base" and install the packages you need, 
then recreate it as a image before running docker-compose.


