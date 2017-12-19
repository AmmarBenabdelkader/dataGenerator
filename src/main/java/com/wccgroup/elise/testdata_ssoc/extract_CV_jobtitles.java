/**
 * @author abenabdelkader
 *
 * niriParser.java
 * Dec 17, 2015
 */
package com.wccgroup.elise.testdata_ssoc;

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
public class extract_CV_jobtitles extends DefaultHandler 
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Connection conn = null;
	static Statement stmt = null;
	static String[][] CVs = {
		{"Title","0"},
		};
	static Map<String, Boolean> elmtStaus = new HashMap<String, Boolean>();
	static Map<String, String> jobTitles = new HashMap<String, String>();

	static String outputFile = "";
	static BufferedWriter writer;
	static public void main(String[] args) throws Exception {
		String filename = null;

/*		for (int i = 0; i < args.length; i++) {
			filename = args[i];
			if (i != args.length - 1) {
				usage();
			}
		}

		if (filename == null) {
			usage();
		} 
*/		for (int i=0; i<CVs.length; i++)
			elmtStaus.put(CVs[i][0], false);
		readProperties();
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt = conn.createStatement();

		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setNamespaceAware(true);
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		xmlReader.setContentHandler(new extract_CV_jobtitles());
		String path = "C:\\data\\Cvs\\ManpowerEnglishResumeParserOutput";
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();

		    for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile()) {
		    	  filename = listOfFiles[i].getName();
		        //System.out.println(i + ": " + filename);
		        UserHandler userhandler = new UserHandler();
		        outputFile = "C:\\data\\Cvs\\jobTitles_fromCVs.txt";
		        if (!filename.contains("#")) {
			        saxParser.parse(convertToFileURL(path + "\\" + filename), userhandler); 
		        }
		      }
		    }
			writer = new BufferedWriter(new FileWriter(new File(outputFile)));
			StringBuilder query = new StringBuilder();
			query.append("insert into ssoc2.jobtitles values ");
			for (Map.Entry<String, String> entry : jobTitles.entrySet()) {
				//System.out.println(entry.getKey());
				writer.write(entry.getKey() + "\n");
				if (entry.getKey().length()>3) {
					query.append("(\"" + entry.getKey().replaceAll("\t", " ") + "\", 'CV', null),");
					writer.write(entry.getKey().replaceAll("\t", " ").replaceAll("\n", " ") + "\n");
				}
			}
			System.out.println(query.toString().substring(0, query.toString().length()-1));
			stmt.executeUpdate(query.toString().substring(0, query.toString().length()-1));
			writer.close();


		//STEP 6: Clean-up environment
		stmt.close();
		conn.close();

	}
	catch (SQLException se)
	{
		//Handle errors for JDBC
		se.printStackTrace();
	}
	}
	private static class UserHandler extends DefaultHandler {

		@Override
		public void startElement(String uri, 
			String localName, String qName, Attributes attributes)
				throws SAXException {

			for (int i=0; i<CVs.length; i++) {
				if (qName.equalsIgnoreCase(CVs[i][0])) 
					elmtStaus.put(CVs[i][0], true);
			}
		}

		@Override
		public void endElement(String uri, 
			String localName, String qName) throws SAXException {
			if (qName.equalsIgnoreCase("EmploymentEvent")) {
				jobTitles.put(CVs[0][1], CVs[0][1]);
			}
		}



		@Override
		public void characters(char ch[], 
			int start, int length) throws SAXException {
			String tmp="";
			for (int i=0; i<CVs.length; i++) {
				if (elmtStaus.get(CVs[i][0])) {
					tmp = new String(ch, start, length);
					CVs[i][1]= tmp;
					elmtStaus.put(CVs[i][0],false);
				}
			}
			
		}
	}
	private static void usage() {
		System.err.println("Usage: niriParser <file.xml>");
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

			input = new FileInputStream("niriParser.properties");

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
