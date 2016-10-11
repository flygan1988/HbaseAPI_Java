package com.fiu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

public class HBaseAPI {
	
	private static Configuration conf = null;
	//Initialization
	static {
		conf = HBaseConfiguration.create();
	}
	//Create a table
	public static void creatTable(String tableName, String[] families) throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(tableName)) {
			System.out.println("Table already exists!");
		}else {
			HTableDescriptor tableDescriptor = new HTableDescriptor();
			for (int i = 0; i < families.length; i++) {
				tableDescriptor.addFamily(new HColumnDescriptor(families[i]));
			}
			admin.createTable(tableDescriptor);
			System.out.println("Create table "+ tableName + " ok!");
		}
		admin.close();
	}
	//Delete a table
	public static void deleteTable(String tableName) throws Exception {
        HBaseAdmin admin = null;
		try {
            admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }finally{
        	if(admin != null) admin.close();
        }
    }
	//Put (or insert) a row
	 public static void addRecord(String tableName, String rowKey,
	            String family, String qualifier, String value) throws Exception {
	        try {
	            HTable table = new HTable(conf, tableName);
	            Put put = new Put(Bytes.toBytes(rowKey));
	            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
	                    .toBytes(value));
	            table.put(put);
	            System.out.println("insert recored " + rowKey + " to table "
	                    + tableName + " ok.");
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	 //Delete a row
	 public static void delRecord(String tableName, String rowKey)
	            throws IOException {
	        HTable table = new HTable(conf, tableName);
	        List<Delete> list = new ArrayList<Delete>();
	        Delete del = new Delete(rowKey.getBytes());
	        list.add(del);
	        table.delete(list);
	        System.out.println("del recored " + rowKey + " ok.");
	    }
	 //Get a row
	 public static void getOneRecord (String tableName, String rowKey) throws IOException{
	        HTable table = new HTable(conf, tableName);
	        Get get = new Get(rowKey.getBytes());
	        Result rs = table.get(get);
	        for(KeyValue kv : rs.raw()){
	            System.out.print(new String(kv.getRow()) + " " );
	            System.out.print(new String(kv.getFamily()) + ":" );
	            System.out.print(new String(kv.getQualifier()) + " " );
	            System.out.print(kv.getTimestamp() + " " );
	            System.out.println(new String(kv.getValue()));
	        }
	  }
	  //Scan (or list) a table
	 public static void getAllRecord (String tableName) {
		 
	        try{
	             HTable table = new HTable(conf, tableName);
	             Scan s = new Scan();
	             ResultScanner ss = table.getScanner(s);
	             for(Result r:ss){
	                 for(KeyValue kv : r.raw()){
	                    System.out.print(new String(kv.getRow()) + " ");
	                    System.out.print(new String(kv.getFamily()) + ":");
	                    System.out.print(new String(kv.getQualifier()) + " ");
	                    System.out.print(kv.getTimestamp() + " ");
	                    System.out.println(new String(kv.getValue()));
	                 }
	             }
	        } catch (IOException e){
	            e.printStackTrace();
	        }
	    }
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
            String tablename = "scores";
            String[] families = { "grade", "course" };
            HBaseAPI.creatTable(tablename, families);
 
            // add record zkb
            HBaseAPI.addRecord(tablename, "zkb", "grade", "", "5");
            HBaseAPI.addRecord(tablename, "zkb", "course", "", "90");
            HBaseAPI.addRecord(tablename, "zkb", "course", "math", "97");
            HBaseAPI.addRecord(tablename, "zkb", "course", "art", "87");
            // add record baoniu
            HBaseAPI.addRecord(tablename, "baoniu", "grade", "", "4");
            HBaseAPI.addRecord(tablename, "baoniu", "course", "math", "89");
 
            System.out.println("===========get one record========");
            HBaseAPI.getOneRecord(tablename, "zkb");
 
            System.out.println("===========show all record========");
            HBaseAPI.getAllRecord(tablename);
 
            System.out.println("===========del one record========");
            HBaseAPI.delRecord(tablename, "baoniu");
            HBaseAPI.getAllRecord(tablename);
 
            System.out.println("===========show all record========");
            HBaseAPI.getAllRecord(tablename);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
