
hadoop fs -put /home/acadgild/Downloads/customers.dat /user/acadgild

export HADOOP_CLASSPATH=$HBASE_HOME/lib/*

hadoop jar /home/acadgild/Desktop/BKLoad.jar /user/acadgild/customers.dat /hbase_output_dir customer

hadoop jar /home/acadgild/Downloads/hbase-server-0.98.14-hadoop2.jar completebulkload /hbase_output_dir/ customer


echo "Loaded data to customer"

