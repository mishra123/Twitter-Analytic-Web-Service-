package AnoopRESTWebService.team;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.*;


import java.io.IOException;
import java.io.PrintWriter;
 
import java.util.regex.Pattern;
import java.util.regex.Matcher;
 
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
 
public class FirstQuery extends HttpServlet 
{
	
 
	 protected void doGet(HttpServletRequest request, HttpServletResponse response)
		      throws ServletException, IOException {
		    PrintWriter out = response.getWriter();
		    response.setContentType("text/plain");
	        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
	        Date d =new Date();
	        String timeStamp = sdf1.format(d);
	        
		    String teamId = "AP13";
			String account_id = "1597-7165-9323";
		out.println(teamId + ", " +  account_id + ",  " + timeStamp);
		
		
		    out.close();
		  }
}