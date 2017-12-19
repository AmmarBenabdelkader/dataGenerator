/**
 * @author abenabdelkader
 *
 * taxonomy.java
 * Nov 8, 2016
 */
package com.wccgroup.elise.testdata;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Properties;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
/**
 * @author abenabdelkader
 *
 */


public class taxonomy {
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static String hostname="http://demotaxonomy:9008/services/rest/v2/webapp/models/bis/taxonomies/occupation";

	private final static String USER_AGENT = "Mozilla/5.0";
	public static void main(String[] args) throws ClientProtocolException, IOException, SQLException, ClassNotFoundException {
		readProperties(); //load dataGerator properties
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		String query;
		//query = "update bissimple3.occupation_tm set code_tm=null, parent_tm=null";
		//System.out.println("removing occupation_tm data: " + stmt.executeUpdate(query));
	  try {

			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet getRequest = new HttpGet(hostname);
            
			String userCredentials = "demo:wcc123";
			String basicAuth = "Basic " + new String(new Base64().encode(userCredentials.getBytes()));
			getRequest.setHeader("Authorization", basicAuth);
			
			getRequest.setHeader("Accept", "application/json");
			//getRequest.setHeader("Connection", "close");

			
			//getRequest.addHeader("accept", "application/json");
            HttpResponse response = httpClient.execute(getRequest);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
				   + response.getStatusLine().getStatusCode()
				   + "\n" + response.getStatusLine().getReasonPhrase());
			}

			BufferedReader br = new BufferedReader(
	                         new InputStreamReader((response.getEntity().getContent())));

			//String output;
			System.out.println("Output from Server .... \n");
			JSONParser parser = new JSONParser();

	        try {

	            Object obj = parser.parse(br);

	            JSONObject jsonObject = (JSONObject) obj;

	            // loop array
	            JSONArray msg = (JSONArray) jsonObject.get("childNodes");
	            Iterator<JSONObject> iterator = msg.iterator();
	            JSONObject occupation;
	            int i = 1;
	            // iterates through berufunttergruppen
	            while (iterator.hasNext()) {
	            	occupation = iterator.next();
	                System.out.print( "\n" + (i++) + "- " + occupation.get("id") + "\t" + occupation.get("name"));
	                //stmt.executeUpdate("update bissimple3.occupation_tm set code_tm = " + occupation.get("id") + " where name= \"" + occupation.get("name") + "\"");
	    			HttpGet getSubRequest = new HttpGet("http://demotaxonomy:9008/services/rest/v2/webapp/models/bis/taxonomies/occupation/" + occupation.get("id"));
	                HttpResponse subResponse = httpClient.execute(getSubRequest);

	    			if (subResponse.getStatusLine().getStatusCode() != 200) {
	    				throw new RuntimeException("Failed : HTTP error code : "
	    				   + subResponse.getStatusLine().getStatusCode()
	    				   + "\n" + subResponse.getStatusLine().getReasonPhrase());
	    			}

	    			BufferedReader SubBR = new BufferedReader(
	    	                         new InputStreamReader((subResponse.getEntity().getContent())));

	    			JSONParser SubParser = new JSONParser();
		            Object subObj = SubParser.parse(SubBR);

		            JSONObject subJsonObject = (JSONObject) subObj;

		            // loop array
		            JSONArray subMsg = (JSONArray) subJsonObject.get("childNodes");
		            Iterator<JSONObject> iterator2 = subMsg.iterator();
		            JSONObject occupation2;
		            // iterates through spezialezieren
		            while (iterator2.hasNext()) {
		            	occupation2 = iterator2.next();
		                System.out.println( "\t" + (i++) + "- " + occupation2.get("id") + "\t" + occupation2.get("name"));
		                System.out.print(".");
		                //stmt.executeUpdate("update bissimple3.occupation_tm set code_tm=" + occupation2.get("id") + ", parent_tm=" +  occupation2.get("id") + " where name=\"" + occupation2.get("name") + "\"");
		            }

	            }
/*	            // update the affinities
				HttpPost postRequest = new HttpPost();
				
	            query = "SELECT a.code_tm, b.code_tm, c.gewicht "
	            	+ "FROM bissimple3.occupationaffinity c, bissimple3.occupation_tm a, bissimple3.occupation_tm b "
	            	+ "where c.occupation_a=a.code and c.occupation_b=b.code limit 5,10";
				ResultSet rs = stmt.executeQuery(query);
				while  (rs.next())
				{
					System.out.println(rs.getString("a.code_tm") +  "{destinationNodeId: " + rs.getString("b.code_tm") + ", score: " + rs.getString("c.gewicht") + "}" );
				


					postRequest = new HttpPost(
						"http://demotaxonomy:9008/services/rest/v2/webapp/nodes/" + rs.getString("a.code_tm") + "/relations-with-score/3");
													
					StringEntity input = new StringEntity("{\"destinationNodeId\": " + rs.getString("b.code_tm") + ", \"score\": " + rs.getString("c.gewicht") + "}");
					//input.setContentType("application/json");
					postRequest.setEntity(input);
					input.setContentType("application/json;charset=UTF-8");
					
					postRequest.setEntity(input);
		
					response = httpClient.execute(postRequest);
		
					if (response.getStatusLine().getStatusCode() != 200) {
						throw new RuntimeException("Failed : HTTP error code : "
							+ response.getStatusLine().getStatusCode()
							+ "\n"
							+ response.getStatusLine().getReasonPhrase());
					}
					
					// all this just to commit
					postRequest = new HttpPost(
						"http://demotaxonomy:9008/services/rest/v2/webapp/transactions/commit");
													
					input = new StringEntity("{\"message\": \"\"}");
					//input.setContentType("application/json");
					postRequest.setEntity(input);
					input.setContentType("application/json;charset=UTF-8");
					
					postRequest.setEntity(input);
		
					response = httpClient.execute(postRequest);
		
					if (response.getStatusLine().getStatusCode() != 200) {
						throw new RuntimeException("Failed : HTTP error code : "
							+ response.getStatusLine().getStatusCode()
							+ "\n"
							+ response.getStatusLine().getReasonPhrase());
					}
				}
*/		

	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (org.json.simple.parser.ParseException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        stmt.close();
	        conn.close();
	        //rs.close();
			
			if (1==1)
				return;

	  try {

			//POST
			HttpPost postRequest = new HttpPost(
				"http://demobistaxonomy:9000/services/rest/frontend/nodes/%2393%3A31/children");
			
			StringEntity input = new StringEntity("{\"code\":\"211\",\"name\":\"211- Concept211\"}");
			input.setContentType("application/json");
			postRequest.setEntity(input);
			input.setContentType("application/json");
			
			postRequest.setEntity(input);

			response = httpClient.execute(postRequest);
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatusLine().getStatusCode());
			}

			// COMMIT
			commit(httpClient);
/*			HttpPost postRequest2 = new HttpPost("http://demobistaxonomy:9000/services/rest/frontend/transactions/commit");
			//input = new StringEntity("{\"message\": {\"message\"}}");	
			input = new StringEntity("{message: \"\"}");
			postRequest2.setEntity(input);
			input.setContentType("application/json");
			postRequest2.setEntity(input);
			HttpResponse response2 = httpClient.execute(postRequest2);
			System.out.println(response2.getStatusLine().getStatusCode()
				+ ": "
				+ response2.getStatusLine().getReasonPhrase()
				+ "\n"
				+ input.toString());
			
			if (response2.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ response2.getStatusLine().getStatusCode() + ": "
					+ response2.getStatusLine().getReasonPhrase());
			}

			br = new BufferedReader(
	                        new InputStreamReader((response2.getEntity().getContent())));

			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				//System.out.println(output);
			}
*/
			
			//Connection conn = null;
			//Statement stmt = null;
			ResultSet rs = null;
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
/*			String query = "SELECT code, name "
				+ "FROM bissimple.education "
				+ "union "
				+ "SELECT code, name "
				+ "FROM bissimple.educationCategory";
*/			//String 
				query = "SELECT code, name "
				+ "FROM bissimple.education limit 2";
			rs = stmt.executeQuery(query);
			while  (rs.next())
			{
				System.out.println("{\"name\":\"" + rs.getString(1) + " - " + rs.getString(2) + "\"}" );
			


				postRequest = new HttpPost(
					"http://demobistaxonomy:9000/services/rest/frontend/nodes/%2393%3A30/children");
												
				input = new StringEntity("{\"code\":\"" + rs.getString(1) + "\", \"name\":\"" + rs.getString(1) + " - " + rs.getString(2) + "\"}");
				input.setContentType("application/json");
				postRequest.setEntity(input);
				input.setContentType("application/json");
				
				postRequest.setEntity(input);
	
				response = httpClient.execute(postRequest);
	
				if (response.getStatusLine().getStatusCode() != 200) {
					throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatusLine().getStatusCode()
						+ "\n"
						+ response.getStatusLine().getReasonPhrase());
				}
	
/*				br = new BufferedReader(
		                        new InputStreamReader((response.getEntity().getContent())));
	
				System.out.println("Output from Server .... \n");
				while ((output = br.readLine()) != null) {
					System.out.println(output);
				}
*/			}
			stmt.close();
			conn.close();

	  }
	catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	catch (ClassNotFoundException e)
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	  } catch (ClientProtocolException e) {

		e.printStackTrace();

	  } catch (IOException e) {

		e.printStackTrace();
	  }
		}
	public static void commit(HttpClient httpClient) {

		  try {

			//HttpClient httpClient = HttpClientBuilder.create().build();
			HttpPost postRequest = new HttpPost(
				"http://demobistaxonomy:9000/services/rest/frontend/transactions/commit");

			StringEntity input = new StringEntity("{message: \"\"}");
			input.setContentType("application/json");
			postRequest.setEntity(input);

			HttpResponse response = httpClient.execute(postRequest);

			System.out.println(response.getStatusLine().getStatusCode()
				+ ": "
				+ response.getStatusLine().getReasonPhrase()
				+ "\n"
				+ input.toString());
			

			if (response.getStatusLine().getStatusCode() != 201) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatusLine().getStatusCode());
			}

			BufferedReader br = new BufferedReader(
	                        new InputStreamReader((response.getEntity().getContent())));

			String output;
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}


		  } catch (ClientProtocolException e) {

			e.printStackTrace();

		  } catch (IOException e) {

			e.printStackTrace();

		  }

		}

	/* Loads data generation properties into the system
	 * allows the connection to the database
	 */
	public static void readProperties()
	{
		Properties prop = new Properties();
		InputStream input = null;

		try
		{

			input = new FileInputStream("dataGenerator.properties");

			// load a properties file
			prop.load(input);
			JDBC_DRIVER = prop.getProperty("driver");
			DB_URL = prop.getProperty("url");
			USER = prop.getProperty("user");
			PASS = prop.getProperty("pass");

		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
	}

}