
cd /usr/hdp/2.6.2.0-205/spark2
./bin/spark-submit /citibike/Streaming.py

su hdfs <<'EOF'

hadoop fs -put  /citibike/output/*   /user/hdfs/testing 

EOF

rm /citibike/output/*
