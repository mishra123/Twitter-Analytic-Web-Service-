

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Query_2
{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
	{
	public static JSONObject jsonObject=null;
    private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
      public void map(LongWritable key, Text value, OutputCollector<Text,  IntWritable> output, Reporter reporter) throws IOException
      {
    	 
    	  String line = value.toString();
    	  try
    	  {
        JSONParser jsonparse =new JSONParser();
        Object object = jsonparse.parse(line);
        jsonObject = (JSONObject) object;
        String date = jsonObject.get("created_at").toString();
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd+kk:mm:ss");
        SimpleDateFormat sdf2 = new SimpleDateFormat("EEE MMM dd kk:mm:ss Z yyyy");
        Date d =new Date();
        try
        {
        	d = sdf2.parse(date);
        }catch(Exception ww)
        {
        	
        }
        String timeStamp = sdf1.format(d);
        String tweetid = jsonObject.get("id").toString();
        String text = jsonObject.get("text").toString();
        String out = tweetid + "," + ",\"" + text + "\",\"" + timeStamp+"\"";
        
          word.set(out);
          output.collect(word, one);
     }
    	  catch(Exception e)
    	  {}
	}
	}
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
	{
      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
	  {
        int sum = 0;
        while (values.hasNext()) 
		{
          sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
      }
    }
    public static void main(String[] args) throws Exception 
	{
      JobConf conf = new JobConf(Query_2.class);
      conf.setJobName("wordcount");
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);
      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      JobClient.runJob(conf);
    }
}

