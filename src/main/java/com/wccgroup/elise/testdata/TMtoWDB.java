/**
 * @author abenabdelkader
 *
 * niriParser.java
 * Dec 17, 2015
 */
package com.wccgroup.elise.testdata;

/**
 * @author abenabdelkader
 *
 */
import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

import java.util.*;
import java.io.*;
import java.sql.*;
public class TMtoWDB extends DefaultHandler 
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Connection conn = null;
	static Statement stmt = null;
	static String[][] columns = {
		//{"Name","Name"},
		{"key","Key"},
		};
	static Map<String, Boolean> elmtStaus = new HashMap<String, Boolean>();
	static int counter;
	static String jobTitle;
	static String jobId;

	static public void main(String[] args) throws Exception {
		String filename = null;
		counter =1;
		jobTitle="";

		for (int i = 0; i < args.length; i++) {
			filename = args[i];
			if (i != args.length - 1) {
				usage();
			}
		}

		if (filename == null) {
			usage();
		} 
		for (int i=0; i<columns.length; i++)
			elmtStaus.put(columns[i][0], false);
		readProperties();
/*		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt = conn.createStatement();
*/
		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setNamespaceAware(true);
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		xmlReader.setContentHandler(new TMtoWDB());
        UserHandler userhandler = new UserHandler();
        saxParser.parse(convertToFileURL(filename), userhandler);     

		//STEP 6: Clean-up environment
/*		stmt.close();
		conn.close();

	}
	catch (SQLException se)
	{
		//Handle errors for JDBC
		se.printStackTrace();
	}
*/	}
	private static class UserHandler extends DefaultHandler {

		String table="",column="";
		String querySkills = "";
		boolean SkillNameProfessional = true;

		@Override
		public void startElement(String uri, 
			String localName, String qName, Attributes attributes)
				throws SAXException {
			if (qName.equalsIgnoreCase("Taxonomy")) {
				table="create table " + attributes.getValue("key") + "( id int not null, name varchar(200) not null, nodeStatus varchar(45), parent int, primary key (id));";
			}
			if (qName.equalsIgnoreCase("MultiLineTextPropertyType")) {
				column = "";
				SkillNameProfessional = true;
			}
			for (int i=0; i<columns.length; i++) {
				if (qName.equalsIgnoreCase(columns[i][0])) 
					elmtStaus.put(columns[i][0], true);
			}
		}

		@Override
		public void endElement(String uri, 
			String localName, String qName) throws SAXException {
/*			try
			{
*/				if (qName.equalsIgnoreCase("Taxonomy")) {
					//System.out.println(qName + " query: " + query1.substring(0, query1.length()-2) + query2.substring(0, query2.length()-2) + ")" );
					System.out.print(("\n" + counter++) + ": " + table + "(");
					//stmt.executeUpdate(query1.substring(0, query1.length()-2) + query2.substring(0, query2.length()-2) + ")");
				}

				if (qName.equalsIgnoreCase("MultiLineTextPropertyType")) {
					//System.out.println(qName + " query: " + query1.substring(0, query1.length()-2) + query2.substring(0, query2.length()-2) + ")" );
					System.out.print(column);
					//stmt.executeUpdate(query1.substring(0, query1.length()-2) + query2.substring(0, query2.length()-2) + ")");
				}

/*				if (qName.equalsIgnoreCase("SkillItem")) {
					System.out.println("\t" + querySkills);
				}
			}
			catch (SQLException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
*/		}



		@Override
		public void characters(char ch[], 
			int start, int length) throws SAXException {
			String tmp="";
			for (int i=0; i<columns.length; i++) {
				if (elmtStaus.get(columns[i][0])) {
					//query1 += jobs[i][1] + ", ";
					tmp = new String(ch, start, length);
					column += "\"" + tmp.replaceAll("\"", "'") + "\", ";
					elmtStaus.put(columns[i][0],false);
					if (i==1)
						jobTitle = tmp;
					if (i==0)
						jobId = tmp;
				}
			}
			if (SkillNameProfessional) {
				tmp = new String(ch, start, length);
				if (tmp.length()>245)
					tmp = tmp.substring(0,245);
				
				querySkills =  "insert into niri.job_skills (job_id,skill_id) values ('" + jobId + "', \"" + tmp.replaceAll("\"", "'") + "\")";
				//ystem.out.println("\t" + querySkills);
				//System.out.print(".");
/*				try
				{
					stmt.executeUpdate(querySkills);
				}
				catch (SQLException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
*/				SkillNameProfessional = false;
			}
			
		}
	}
	private static void usage() {
		System.err.println("Usage: TMtoWB <domain model.xml>");
		System.err.println("       -usage or -help = this message");
		System.exit(1);
	}
	private static String convertToFileURL(String filename) {
		String path = new File(filename).getAbsolutePath();
		if (File.separatorChar != '/') {
			path = path.replace(File.separatorChar, '/');
		}

		if (!path.startsWith("/")) {
			path = "/" + path;
		}
		return "file:" + path;
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

			input = new FileInputStream("TMtoWDB.properties");

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
