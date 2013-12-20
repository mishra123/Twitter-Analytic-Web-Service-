package AnoopRESTWebService.team;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
 
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
 
public class thirdQuery extends HttpServlet {
	//static String res1;
	
	 protected void doGet(HttpServletRequest request, HttpServletResponse response)
		    throws ServletException, IOException 
		    {
		 	Configuration myconf = HBaseConfiguration.create();
			
			myconf.set("hbase.cluster.distributed", "true");
			myconf.set("hbase.master", "ec2-54-226-44-110.compute-1.amazonaws.com:60000");
			myconf.set("hbase.zookeeper.quorum", "ec2-54-226-44-110.compute-1.amazonaws.com");
			myconf.set("hbase.rootdir", "hdfs://ec2-54-226-44-110.compute-1.amazonaws.com:20001/hbase");		
			HTable uTable = new HTable(myconf, "query3");
		    PrintWriter out = response.getWriter();
		    response.setContentType("text/plain");
	        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
	        Date d =new Date();
	        String timeStamp = sdf1.format(d);
		    String teamId = "AP13";
			String account_id = "1597-7165-9323";
			response.getWriter().write(teamId + ", " +  account_id + ",  " + timeStamp);
			
			String userid_min = request.getParameter("userid_min");
			String userid_max = request.getParameter("userid_max");
			
			
			Scan s = new Scan(Bytes.toBytes(userid_min),Bytes.toBytes(userid_max));
			ResultScanner rs = uTable.getScanner(s);
			
			
			int sum=0;
			
			for(Result q:rs)
			{
				String res = Bytes.toString(q.getValue(Bytes.toBytes("cf"), Bytes.toBytes("UserID")));
				//out.println(res);
				//out.println("Scan: " + Bytes.toString(q.getRow()) + res);
				res.trim();
				int x = Integer.parseInt(res);
				sum = sum+x;
			}
			//out.println("Number of users are: " + sum);
			response.getWriter().write("\n" +sum);
			
		    out.close();
		  
	 }
}
