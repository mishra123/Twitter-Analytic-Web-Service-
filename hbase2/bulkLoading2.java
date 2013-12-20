package hbase2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class bulkLoading2
{
    public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> 
	{
    	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
    	{
    		String sc = value.toString();
    		String pattern = "(.*),,(.*),(.*)\\s[0-9]+";
    		Pattern r = Pattern.compile(pattern);
    		 Matcher m = r.matcher(sc);
    	if(m.matches()==true)
    	{
    		 String src2 = sc.replace(",,", ",");
     		String src1 =src2.replace(",", "\t");
     		//String src3 =src1.replace("\"", "");
     		String[] str= src1.split("\\s+");	
     	   // String[] t = str[0].split(",");
     	
    		Put row = new Put(Bytes.toBytes(str[2]+str[0]));
    		row.add(Bytes.toBytes("cf2"),Bytes.toBytes("TweetID"),Bytes.toBytes(str[0]));
    		
    		row.add(Bytes.toBytes("cf2"),Bytes.toBytes("Text"),Bytes.toBytes(str[1]));
    		row.add(Bytes.toBytes("cf2"),Bytes.toBytes("Time"),Bytes.toBytes(str[2]));
    		row.add(Bytes.toBytes("cf2"),Bytes.toBytes("count"),Bytes.toBytes(str[3]));
    		
    		try
    		{
    			context.write(new ImmutableBytesWritable(Bytes.toBytes(str[0])),row);
    		}
    		catch(Exception e)
    		{
    			
    		}
     		}
    	}
	}
    public static void main(String[] args) throws Exception 
	{
    	
    Job job = new Job();
    
    	job.setJobName("BulkLoad2");
    	        Configuration conf = HBaseConfiguration.create(job.getConfiguration());
    	        conf.set("hbase.cluster.distributed", "true");
    	        conf.set("hbase.master", "ec2-54-221-74-193.compute-1.amazonaws.com:60000");
    	        conf.set("hbase.zookeeper.quorum", "ec2-54-221-74-193.compute-1.amazonaws.com");
    	        conf.set("hbase.rootdir", "hdfs://ec2-54-221-74-193.compute-1.amazonaws.com:20001/hbase");
    	        
    	       // job.getConfiguration().set("tableName", args[2].equals("q3")? "quantity": "tweeters");
    	        
    	        job.setMapperClass(Map.class);
    	        job.setJarByClass(bulkLoading2.class);
    	        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    	        job.setMapOutputValueClass(Put.class);        
    	        job.setInputFormatClass(TextInputFormat.class);
    	        job.setOutputFormatClass(HFileOutputFormat.class);
    	        job.setNumReduceTasks(0);
    			
    	        FileInputFormat.setInputPaths(job, new Path(args[0]));
    	        HTable table = new HTable(conf, "query2");       
    	        HFileOutputFormat.configureIncrementalLoad(job, table);
    	        HFileOutputFormat.setOutputPath(job, new Path(args[1]));  
    	        job.waitForCompletion(true);
    	        }
}
