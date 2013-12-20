

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


public class Query_3
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
        String userId = ((JSONObject)jsonObject.get("user")).get("id").toString();
        word.set(userId);
        output.collect(word, one);
    	  }
    	  catch(Exception ee)
    	  {
    		  
    	  }
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
      JobConf conf = new JobConf(Query_3.class);
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

