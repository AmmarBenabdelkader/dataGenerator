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
public class Parser_niriJobs extends DefaultHandler 
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Connection conn = null;
	static Statement stmt = null;
	static String[][] jobs = {
		{"VacancyID","job_id"},
		{"HarvestedJobTitle","job_title"},
		{"HarvestedJobDescription","job_description"},
		{"HarvestedPartTimeOrFullTime","full_part_time"},
		{"Language","language"},
		{"HarvestedGender","desired_gender"},
		{"HarvestedContractType","job_contract_type"},
		{"HarvestedIndustry","job_sector"},
		{"HarvestedSalary","salary"},
		{"FunctionTitle","job_occupation"},
		{"HarvestedCompany","employer_id"},
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
		for (int i=0; i<jobs.length; i++)
			elmtStaus.put(jobs[i][0], false);
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
		xmlReader.setContentHandler(new Parser_niriJobs());
        UserHandler userhandler = new UserHandler();
        saxParser.parse(convertToFileURL(filename), userhandler);     

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

		String query1="",query2="";
		String querySkills = "";
		boolean SkillNameProfessional = true;

		@Override
		public void startElement(String uri, 
			String localName, String qName, Attributes attributes)
				throws SAXException {
			if (qName.equalsIgnoreCase("doc")) {
				query1="Jobid: ";
				query2=") values (";
			}
			if (qName.equalsIgnoreCase("SkillNameProfessional")) {
				querySkills="Job Skills" + jobId;
				SkillNameProfessional = true;
			}
			for (int i=0; i<jobs.length; i++) {
				if (qName.equalsIgnoreCase(jobs[i][0])) 
					elmtStaus.put(jobs[i][0], true);
			}
		}

		@Override
		public void endElement(String uri, 
			String localName, String qName) throws SAXException {
				if (qName.equalsIgnoreCase("doc")) {
					//System.out.println(qName + " query: " + query1.substring(0, query1.length()-2) + query2.substring(0, query2.length()-2) + ")" );
					System.out.print(("\n" + counter++) + ": " + jobTitle + " (" + jobId + ")\n\t- Skills: \n");
					//stmt.executeUpdate(query1.substring(0, query1.length()-2) + query2.substring(0, query2.length()-2) + ")");
				}

/*				if (qName.equalsIgnoreCase("SkillItem")) {
					System.out.println("\t" + querySkills);
				}
*/		}



		@Override
		public void characters(char ch[], 
			int start, int length) throws SAXException {
			String tmp="";
			for (int i=0; i<jobs.length; i++) {
				if (elmtStaus.get(jobs[i][0])) {
					query1 += jobs[i][1] + ", ";
					tmp = new String(ch, start, length);
					query2 += "\"" + tmp.replaceAll("\"", "'") + "\", ";
					elmtStaus.put(jobs[i][0],false);
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
				
				querySkills =  jobId + ": " + tmp.replaceAll("\"", "'");
				System.out.println("\t\t" + querySkills);
				SkillNameProfessional = false;
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
