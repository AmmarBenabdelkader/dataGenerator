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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;
import java.io.*;
import java.sql.*;

public class Niri_OccupationValidator  
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Connection conn = null;
	static Statement stmt = null;
	static Statement stmt2 = null;
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
	static int counter;
	static String jobTitle;
	static String jobId;
	static String[] batchQueries = new String[10];
	static 	List<String> words = new ArrayList<String>();

	static public void main(String[] args) throws Exception {


		readProperties();
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt = conn.createStatement();

			//stmt.executeUpdate("delete from niri.job_skills");  // clean all job skills
			//stmt.executeUpdate("delete from niri.job");  // clean all jobs 
			
			stmt.executeUpdate("update niri.job set job_code=null");  // reset all job_code(s) to null
			stmt.executeUpdate("update niri.job set prediction_code=null");  // reset all prediction_code(s) to null
			// STEP 1
			System.out.print("\n\n **** Processing batch queries STEP 1 **** \n\tPlease wait .");
			readBatchQueries("aquery_niri");
			for (int q=0; batchQueries[q]!=null ; q++){
				System.out.print(" ...");
				stmt.executeUpdate(batchQueries[q]);
			}
			System.out.println("\nDone.");

			// Intermediate STEP
			stmt.executeUpdate("update niri.job set job_title_cleaned=job_title");
			cleanJobTitles();
			cleanJobTitles();
			replaceWithSpace();			
			// STEP 2 and STEP 3
			System.out.print("\n\n **** Processing batch queries STEP 2 and STEP 3  **** \n\tPlease wait .");
			readBatchQueries("bquery_niri");
			for (int q=0; batchQueries[q]!=null ; q++){
				System.out.print(" ...");
				stmt.executeUpdate(batchQueries[q]);
			}
			System.out.println("\nDone.");


			// STEP 3
			String[] seps = {"/", "|", "-",","};
			exactJobTitles(seps);
			matchExampleJobTitles();
			matchJobTitles(seps, 2);
			matchJobTitles(seps, 3);
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
	/* Cleans test data for a given occupation (isco code) 
	 * allows to re-generate the data for that specific occupation
	 * in case of errors while parsing the data
	 */
	public static void exactJobTitles(String[] sep) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		ResultSet rs, rs2;
		String query ="";
		//String query = "select job_code, job_title_en from asoc.occupation";
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			String job="";

			for (int i=0; i<sep.length; i++) {
				query = "SELECT job_title_cleaned FROM niri.job where job_code is null and job_title_cleaned like '%" + sep[i] + "%'"; // and job_title not like '%(%' and job_title not like '%,%' and job_title not like '%|%'" ;
				rs = stmt.executeQuery(query);
			while (rs.next())
			{
				//System.out.println("select * from niri.job where job_code is null and job_title like '%" + rs33.getString(2) + "%'");
				//System.out.println(rs.getString("job_title_cleaned"));
				StringTokenizer st = new StringTokenizer(rs.getString("job_title_cleaned"), sep[i]);
				while (st.hasMoreTokens()) {
					job = st.nextToken().trim();
					//query = "select job_title_en from asoc.occupation where job_title_en like '%" + job + "%'";
					query = "select job_code, job_title_en from asoc.occupation where job_title_en = \"" + job + "\" or job_title_ar = \"" + job + "\"";
					//System.out.println("\t- " + job);
					rs2 = stmt2.executeQuery(query);
					if (rs2.next()) {
						System.out.println(rs.getString("job_title_cleaned") + "\n\t--> " + rs2.getString("job_title_en") );
						query = "update niri.job a set a.job_code='" + rs2.getString("job_code") + "' where job_title_cleaned = '" + rs.getString("job_title_cleaned") + "'";
						//System.out.println(query);
						stmt3.executeUpdate(query);
					}

				}
			}
			}
			stmt3.executeUpdate("update niri.job set prediction_code='DC4' where job_code is not null and prediction_code is null");
			stmt.close();
			stmt2.close();
			stmt3.close();
			conn.close();
	}

	public static void matchExampleJobTitles() throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		ResultSet rs = null;
		String query = "";
		//String query = "select job_code, job_title_en from asoc.occupation";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();

		query = "SELECT a.job_title, a.job_occupation, b.job_title_en, b.job_title_ar, b.occup_code  FROM niri.job a, asoc.example_job_titles b where a.job_code is null and (a.job_title=b.job_title_en or a.job_title=b.job_title_ar or a.job_occupation=b.job_title_en or a.job_occupation=b.job_title_ar)";
		rs = stmt.executeQuery(query);
		while (rs.next())
		{
			query = "update niri.job set prediction_code = 'DC5', job_code='" + rs.getString("occup_code") + " - 01' where job_code is null and ("
				+ " job_title=\"" + rs.getString("job_title_en") 
				+ "\" or job_title=\"" + rs.getString("job_title_ar") 
				+ "\" or job_occupation=\"" + rs.getString("job_title_en")
				+ "\"  or job_occupation=\"" + rs.getString("job_title_ar") + "\")";
			System.out.println(rs.getString("job_title") + " \t--> " + rs.getString("job_title_en"));

			stmt2.executeUpdate(query);

		}
		query = "SELECT a.job_code, a.job_title_cleaned, b.job_title_en, b.job_title_ar, b.occup_code  FROM niri.job a, asoc.example_job_titles b where a.job_code is null and (a.job_title_cleaned=b.job_title_en or a.job_title_cleaned=b.job_title_ar)";
		rs = stmt.executeQuery(query);
		while (rs.next())
		{
			query = "update niri.job set prediction_code = 'DC5', job_code='" + rs.getString("occup_code") + " - 01' where job_code is null and ("
				+ " job_title_cleaned=\"" + rs.getString("job_title_en") 
				+ "\" or job_title_cleaned=\"" + rs.getString("job_title_ar") + "\")";
			System.out.println(query + "\n" + rs.getString("job_title_cleaned") + " \t--> " + rs.getString("job_title_en"));

			stmt2.executeUpdate(query);

		}
		stmt.close();
		stmt2.close();
		conn.close();
	}

	public static void matchJobTitles(String[] sep, int ratio) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		ResultSet rs, rs2;
		String query ="";
		//String query = "select job_code, job_title_en from asoc.occupation";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		Statement stmt3 = conn.createStatement();
		String job="";

		query = "SELECT job_title_cleaned FROM niri.job where job_code is null"; 
		rs = stmt.executeQuery(query);
		while (rs.next())
		{
			job = rs.getString("job_title_cleaned");
			query = "select job_code, job_title_en from asoc.occupation where job_title_en like \"%" + job + "%\" or job_title_ar = \"" + job + "\"";
			rs2 = stmt2.executeQuery(query);
			if (rs2.next() && job.length()>rs2.getString("job_title_en").length()/ratio) {
				System.out.println(rs.getString("job_title_cleaned") + " a)--> " + rs2.getString("job_title_en") );
				query = "update niri.job a set a.job_code='" + rs2.getString("job_code") + "' where job_title_cleaned = '" + rs.getString("job_title_cleaned") + "'";
				stmt3.executeUpdate(query);
			}

		}
		stmt3.executeUpdate("update niri.job set prediction_code='" + (ratio==2?"UC1":"UC2") + "' where job_code is not null and prediction_code is null");

		for (int i=0; i<sep.length; i++) {
			query = "SELECT job_title_cleaned FROM niri.job where job_code is null and job_title_cleaned like '%" + sep[i] + "%'"; // and job_title not like '%(%' and job_title not like '%,%' and job_title not like '%|%'" ;
			rs = stmt.executeQuery(query);
			while (rs.next())
			{
				StringTokenizer st = new StringTokenizer(rs.getString("job_title_cleaned"), sep[i]);
				while (st.hasMoreTokens()) {
					job = st.nextToken().trim();
					query = "select job_code, job_title_en from asoc.occupation where job_title_en like \"%" + job + "%\" or job_title_ar = \"" + job + "\"";
					rs2 = stmt2.executeQuery(query);
					if (rs2.next() && job.length()>rs2.getString("job_title_en").length()/ratio) {
						System.out.println(rs.getString("job_title_cleaned") + " b)--> " + rs2.getString("job_title_en") );
						query = "update niri.job a set a.job_code='" + rs2.getString("job_code") + "' where job_title_cleaned = '" + rs.getString("job_title_cleaned") + "'";
						stmt3.executeUpdate(query);
					}

				}
			}
		}

		stmt3.executeUpdate("update niri.job set prediction_code='" + (ratio==2?"UC1":"UC2") + "' where job_code is not null and prediction_code is null");
		stmt.close();
		stmt2.close();
		stmt3.close();
		conn.close();
	}

	public static void cleanJobTitles() throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		ResultSet rs;
		//String query = "select job_code, job_title_en from asoc.occupation";
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			String job="";
			words.add("1."); words.add("2.");words.add("3.");words.add("4.");words.add("5.");words.add("3 x");words.add("x");words.add("stars");
			words.add("5 stars");words.add("urgent"); words.add("/females");words.add("/males");words.add("females");words.add("males");
			words.add("/male");words.add("/female");words.add("male"); words.add("female"); words.add("()"); words.add("Sr.");words.add("Sr");
			words.add("Senior");words.add("Jenior");words.add("Jr.");words.add("Jr ");words.add("5 نجوم");
			Collections.sort(words, new comp());

			String query = "SELECT job_title_cleaned FROM niri.job where job_code is null"; 
			rs = stmt.executeQuery(query);
			while (rs.next())
			{
				job = cleanStopwords(rs.getString("job_title_cleaned"));
				if (!job.equalsIgnoreCase(rs.getString("job_title_cleaned"))) {
					System.out.println(rs.getString("job_title_cleaned") + "\n\t --> " + job);
					query = "update niri.job a set a.job_title_cleaned=\"" + job + "\" where job_title_cleaned = \"" + rs.getString("job_title_cleaned") + "\"";
					stmt2.executeUpdate(query);
				}
			}
			stmt.close();
			stmt2.close();
			conn.close();
	}

	public static void replaceWithSpace() throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		ResultSet rs;
		//String query = "select job_code, job_title_en from asoc.occupation";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String job="";
		String[] toReplace = {"/F ", "/M ", "   ", "  "};
		String query = "SELECT job_title_cleaned FROM niri.job where job_code is null and ("; 
		for (int i=0; i<toReplace.length; i++)
			query += " job_title_cleaned like '%" + toReplace[i] + "%' or ";
		query = query.substring(0, query.length()-4) + ")";
		rs = stmt.executeQuery(query);
		while (rs.next())
		{
			job = rs.getString("job_title_cleaned");
			for (int i=0; i<toReplace.length; i++)
				job = job.replaceAll(toReplace[i], " ");

			System.out.println(rs.getString("job_title_cleaned") + "\n\t --> " + job);
			query = "update niri.job a set a.job_title_cleaned=\"" + job + "\" where job_title_cleaned = \"" + rs.getString("job_title_cleaned") + "\"";
			stmt2.executeUpdate(query);
		}
		stmt.close();
		stmt2.close();
		conn.close();
	}

	public static String cleanStopwords(String title)
	{

		String REGEX = "";
		String INPUT = title;
		String REPLACE = "";
//		Collections.sort(words, new comp());
		//Collections.reverse(words);
		for (int i = 0; i < words.size(); i++)
		{
			REGEX = words.get(i);
			//System.out.println(REGEX);
			//Pattern p = Pattern.compile("\\b" + REGEX + "\\b", Pattern.CASE_INSENSITIVE);
			Pattern p = Pattern.compile(REGEX, Pattern.CASE_INSENSITIVE);
			// get a matcher object
			Matcher m = p.matcher(INPUT);
			INPUT = m.replaceAll(REPLACE);
			//System.out.println(INPUT);
		}

		//System.out.println(INPUT.trim());
		return INPUT.trim();
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
	/* Loads data generation properties into the system
	 * allows the connection to the database
	 */
	public static void processXML(String fileName) throws XMLStreamException
	{
		XMLInputFactory2 xmlif2 = (XMLInputFactory2)XMLInputFactory2.newInstance();
	    try{
	    	XMLStreamReader2 reader = (XMLStreamReader2)xmlif2.createXMLStreamReader(fileName, new FileInputStream(fileName));
	        int eventType = 0;
	    	while(reader.hasNext()){
	    		eventType = reader.next();
	    		switch (eventType) {
	    		case XMLEvent.START_ELEMENT:
	    			System.out.print("<"+reader.getName().toString()+">");
	    			break;
	    		case XMLEvent.CHARACTERS:
	    			System.out.print(reader.getText());
	    			break;
	    		case XMLEvent.END_ELEMENT:
	    			System.out.println("</"+reader.getName().toString()+">");
	    			break;
	    		default:
	    			break;
	    		}
	    	}
	    }

		catch (IOException ex)
		{
			ex.printStackTrace();
		}
	}

	/* Loads data generation properties into the system
	 * allows the connection to the database
	 */
	public static void readBatchQueries(String type)
	{
	
		Properties prop = new Properties();
		InputStream input = null;

		try
		{

			input = new FileInputStream("QueriesNiri.properties");
			prop.load(input);
			for (int i=0; i<10; i++)
				batchQueries[i] = prop.getProperty(type+i);

		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
	}

}
