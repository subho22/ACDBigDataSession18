import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Assi18 {

	Configuration config = HBaseConfiguration.create();
	public void createtable(String name,String[] colfamily) throws MasterNotRunningException, 
	                                               ZooKeeperConnectionException, IOException
	{
	HBaseAdmin admin = new HBaseAdmin(config);
	HTableDescriptor des = new HTableDescriptor(Bytes.toBytes(name));
	for(int i=0;i<colfamily.length;i++){
	des.addFamily(new HColumnDescriptor(colfamily[i]));
	}
	HTable table =  new HTable(config, "customer");
	
	if(admin.tableExists(name)){
	System.out.println("Table already exists. Dropping and recreating ...");
	admin.disableTable(name);
	admin.deleteTable(name);
	
	admin.createTable(des);
	System.out.println("Table: "+name+ " Sucessfully created");
	
	Put put = new Put(Bytes.toBytes("1"));
	//Add the column into the column family Emp_name with qualifier name
	put.add(Bytes.toBytes("details"), Bytes.toBytes("name"),Bytes.toBytes("Kiran"));
	//Add the column into the column family sal with qualifier name
	put.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes("10"));
	put.add(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes("India"));
	//insert the put instance to table
	table.put(put);
	System.out.println("Values inserted : ");
	table.close();
	
	}
	else{
	admin.createTable(des);
	System.out.println("Table: "+name+ " Sucessfully created");
	Put put = new Put(Bytes.toBytes("1"));
	//Add the column into the column family Emp_name with qualifier name
	put.add(Bytes.toBytes("details"), Bytes.toBytes("name"),Bytes.toBytes("Kiran"));
	//Add the column into the column family sal with qualifier name
	put.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes("10"));
	put.add(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes("India"));
	//insert the put instance to table
	table.put(put);
	System.out.println("Values inserted : ");
	table.close();
	
	
	}
	}
	public static void main(String args[]) throws MasterNotRunningException, 
	                               ZooKeeperConnectionException,IOException{
		Assi18 op = new Assi18();
	String tablename = "customer";
	String[] familys = {"details"};
	op.createtable(tablename, familys);
	}

}
