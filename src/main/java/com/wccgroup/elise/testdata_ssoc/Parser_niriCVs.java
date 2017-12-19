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
public class Parser_niriCVs extends DefaultHandler 
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
		{"DocID","0"},
		{"FirstName","1"},
		{"LastName","2"},
		{"StreetAndNumber","3"},
		{"City","4"},
		{"Postcode","5"},
		{"Country","6"},
		{"EliseDate","7"},
		{"FromDate","8"},
		{"ToDate","9"},
		{"Title","10"},
		{"Organization","11"},
		{"Description","12"},
		};
	static String[][] education = {
		{"DegreeName",""},
		{"StartDate",""},
		{"EndDate",""},
		{"SchoolName",""},
		};
	static Map<String, Boolean> elmtStaus = new HashMap<String, Boolean>();
	static int counter;
	static String FullName;
	static String cv_id;
	static Map<String, String> skills = new HashMap<String, String>();
	static String top = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\t<Resume lang=\"en\" >\n";
	static String bottom = "\t</Resume>";

	static StringBuilder cv = new StringBuilder();
	static StringBuilder workHistory = new StringBuilder();
	static StringBuilder competencies = new StringBuilder();
	static StringBuilder educations = new StringBuilder();
	static String outputFile = "";
	static BufferedWriter writer;
	static public void main(String[] args) throws Exception {
		String filename = null;

		for (int i = 0; i < args.length; i++) {
			filename = args[i];
			if (i != args.length - 1) {
				usage();
			}
		}

		if (filename == null) {
			usage();
		} 
		for (int i=0; i<CVs.length; i++)
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
		xmlReader.setContentHandler(new Parser_niriCVs());
		String path = "C:\\data\\Cvs\\ManpowerEnglishResumeParserOutput";
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();

		    for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile()) {
		    	  filename = listOfFiles[i].getName();
		        System.out.println(i + ": " + filename);
		        UserHandler userhandler = new UserHandler();
		        outputFile = "C:\\data\\Cvs\\Actonomy\\" + listOfFiles[i].getName();
		        if (!filename.contains("#")) {
					//writer = new BufferedWriter(new FileWriter(new File(outputFile)));
					writer = new BufferedWriter(new FileWriter(new File(outputFile.substring(0, outputFile.length()-4))));
			        saxParser.parse(convertToFileURL(path + "\\" + filename), userhandler); 
			        writer.close();
		        }
		      }
		    }


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

		boolean SkillNamePersonal = false;
		boolean educationEvent = false;
		boolean educationTitle = false;
		boolean educationStart = false;
		boolean educationEnd = false;
		boolean educationOrg = false;

		@Override
		public void startElement(String uri, 
			String localName, String qName, Attributes attributes)
				throws SAXException {

			if (qName.equalsIgnoreCase("SkillNamePersonal")) {
				SkillNamePersonal = true;
			}
			if (qName.equalsIgnoreCase("EducationEvent")) {
				educationEvent = true;
			}
			if (educationEvent && qName.equalsIgnoreCase("Title")) {
				educationTitle = true;education[0][1]="";
			}
			if (educationEvent && qName.equalsIgnoreCase("FromDate")) {
				educationStart = true;education[1][1]="";
			}
			if (educationEvent && qName.equalsIgnoreCase("ToDate")) {
				educationEnd = true;education[2][1]="";
			}
			if (educationEvent && qName.equalsIgnoreCase("Organization")) {
				educationOrg = true;education[3][1]="";
			}
			for (int i=0; i<CVs.length; i++) {
				if (qName.equalsIgnoreCase(CVs[i][0])) 
					elmtStaus.put(CVs[i][0], true);
			}
		}

		@Override
		public void endElement(String uri, 
			String localName, String qName) throws SAXException {
			try
			{
				if (qName.equalsIgnoreCase("root")) {
				competencies.append("\t\t<Competencies>\n");
				for (Map.Entry<String, String> entry : skills.entrySet()) {
						competencies.append("\t\t\t<Competency>\n\t\t\t\t<Name>" + entry.getKey() + "</Name>\n\t\t\t</Competency>\n");
					}
				competencies.append("\t\t</Competencies>\n");
					cv.append(top);
					cv.append("\t\t<Personal>\n\t\t\t<Name>" + CVs[1][1]  + " " + CVs[2][1] + "</Name>\n");
					cv.append("\t\t\t<BirthDate>" + CVs[7][1] + "</BirthDate>\n");
					cv.append("\t\t\t<Telephone>" + "</Telephone>\n");
					cv.append("\t\t\t<Email>" + "</Email>\n");
					cv.append("\t\t\t<Address>\n\t\t\t\t<Country>" + CVs[6][1] + "</Country>\n");
					cv.append("\t\t\t\t<PostalCode>" + CVs[5][1] + "</PostalCode>\n");
					cv.append("\t\t\t\t<City>" + CVs[4][1] + "</City>\n");
					cv.append("\t\t\t</Address>\n\t\t</Personal>\n");
					cv.append("\t\t<EmploymentHistories>\n");
					cv.append(workHistory.toString());
					cv.append("\t\t</EmploymentHistories>\n");
					cv.append(competencies.toString());
					cv.append("\t\t<EducationHistories>\n");
					cv.append(educations.toString());
					cv.append("\t\t</EducationHistories>\n");
					cv.append(bottom);
					writer.write(cv.toString());
					cv = new StringBuilder();
					competencies = new StringBuilder();
					educations = new StringBuilder();
					workHistory = new StringBuilder();
					//System.out.println(cv);
			}
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
			if (qName.equalsIgnoreCase("EmploymentEvent")) {
				workHistory.append("\t\t\t<Employment>\n\t\t\t\t<FromDate>" + CVs[8][1] + "</FromDate>\n");
				workHistory.append("\t\t\t\t<ToDate>" + CVs[9][1] + "</ToDate>\n");
				workHistory.append("\t\t\t\t<Title>" + CVs[10][1] + "</Title>\n");
				workHistory.append("\t\t\t\t<Organization>" + CVs[11][1] + "</Organization>\n");
				workHistory.append("\t\t\t\t<Description>" + CVs[12][1].replaceAll("\n", " ") + "</Description>\n\t\t\t</Employment>\n");
			}
			if (qName.equalsIgnoreCase("EducationEvent")) {
				educations.append("\t\t\t<Education>\n\t\t\t\t<" + education[0][0] + ">" + education[0][1] + "</" + education[0][0] + ">\n");
				//educations.append("\t\t\t\t<" + education[1][0] + ">" + education[1][1] + "</" + education[1][0] + ">\n");
				//educations.append("\t\t\t\t<" + education[2][0] + ">" + education[2][1] + "</" + education[2][0] + ">\n");
				educations.append("\t\t\t\t<" + education[3][0] + ">" + education[3][1] + "</" + education[3][0] + ">\n");
				educations.append("\t\t\t</Education>\n");
				educationEvent = false;
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
			if (SkillNamePersonal) {
				tmp = new String(ch, start, length);
				skills.put(tmp.replaceAll("\"", "'"), "skill");
				SkillNamePersonal = false;
			}
			
			if (educationEvent) {
				tmp = new String(ch, start, length).replaceAll("\"", "'");
				if (educationTitle) {
					education[0][1]= tmp;
					educationTitle = false;
				}
				if (educationStart) {
					education[1][1]= tmp;
					educationStart = false;
				}
				if (educationEnd) {
					education[2][1]= tmp;
					educationEnd = false;
				}
				if (educationOrg) {
					education[3][1]= tmp;
					educationOrg = false;
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
