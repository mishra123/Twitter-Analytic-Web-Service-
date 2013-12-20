package hbase4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.*;
//import bulkLoading.Map;


public class Executor 
{
	public static void main(String arsg[]) throws Exception
	{
	    	        Configuration conf = HBaseConfiguration.create();
	    	        conf.set("hbase.cluster.distributed", "true");
	    	        conf.set("hbase.master", "ec2-54-226-44-110.compute-1.amazonaws.com:60000");
	    	        conf.set("hbase.zookeeper.quorum", "ec2-54-226-44-110.compute-1.amazonaws.com");
	    	        conf.set("hbase.rootdir", "hdfs://ec2-54-226-44-110.compute-1.amazonaws.com:20001/hbase");
	    	        
	    	       // job.getConfiguration().set("tableName", args[2].equals("q3")? "quantity": "tweeters");
	    	        
	    	       	    			
	    	        //FileInputFormat.setInputPaths(job, new Path());
	    	        HTable table = new HTable(conf, "query4");       
	
	    	        LoadIncrementalHFiles lihf;
					try {
						lihf = new LoadIncrementalHFiles(conf);
					
	    	        lihf.doBulkLoad(new Path(arsg[0]), table);
					} catch (Exception e) {
						// TODO Auto-generated catch block
					}
					}
}
