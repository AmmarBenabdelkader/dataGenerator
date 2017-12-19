/**
 * @author abenabdelkader
 *
 * utilities_asoc.java
 * Oct 7, 2015
 */
package com.wccgroup.elise.testdata;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/*
 * This Packages contains some utility methods related to HRDF-ASOC projects
 * Utilities like: processing the asoc occupations (excel to taxonomy), 
 */

public class utilities_asoc_hrdf
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		readProperties(); //load dataGerator properties
		String input = "";
		ArrayList<String> options = new ArrayList<>();
		options.add("1");
		options.add("2");
		options.add("3");
		options.add("4");
		options.add("5");
		options.add("6");
		
		while (true)
		{
			System.out.printf("Please select the action to perform from the following:\n");
			System.out.println("\t- 1- Process Occupations");
			System.out.println("\t- 2- Process Skills");
			System.out.println("\t- 3- Process Relations");
			System.out.println("\t- 4- Occupation Alternative/Hidden Titles");
			System.out.println("\t- 4- Skill Alternative/Hidden Titles");
			System.out.println("\t- 6- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			ProcessOccupations("T:\\JanS\\asocmlsdV1.3\\occupationasoc.csv", 1);
			break;
		case "2":
			ProcessSkills("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\ESCO\\skill.csv", 1);
			break;
		case "3":
			ProcessRelationss("T:\\JanS\\asocmlsdV1.3\\relations.csv", 0);
			break;
		case "4":
			ProcessTitles("esco2017.alternativetitles", "SELECT distinct occupation, AlternativeTitles FROM esco2017.occupation_en where alternativetitles is not null;");
			ProcessTitles("esco2017.HiddenTitles", "SELECT distinct occupation, HiddenTitles FROM esco2017.occupation_en where HiddenTitles!='NA';");
			break;
		case "5":
			ProcessTitles("esco2017.alternativeskilltitles", "SELECT distinct skill, AlternativeTitles FROM esco2017.skill where alternativetitles!='NA';");
			ProcessTitles("esco2017.HiddenSkillTitles", "SELECT distinct skill, HiddenTitles FROM esco2017.skill where HiddenTitles!='NA';");
			break;
		case "6":
			generateCSV("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\ESCO_2017\\");
			break;
		default:
			System.out.println("Bye"); // quit

		}

	}
	// process Alternative titles and hidden titles
	public static void ProcessTitles(String sqlTable, String query) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		Statement stmt2 = conn.createStatement();
		//stmt.executeUpdate("truncate table " + sqlTable ); //esco2017.occupation");
		ResultSet rs = stmt.executeQuery(query ); 
		int Id=0;
		while (rs.next()) {
			StringTokenizer st = new StringTokenizer(rs.getString(2), ";");
			StringBuilder queryString = new StringBuilder();
			queryString.append("insert into " + sqlTable + " values ");
			while (st.hasMoreTokens()) {
				queryString.append("(\""+ rs.getString(1) + "\",\"" + (st.nextToken().replaceAll("\"", "'").replaceAll("@en", "")).trim() + "\"),");
			}
			//if (Id%100 == 0) {
				System.out.println(Id + "\t- " + queryString.substring(0, queryString.length()-1));
				stmt2.executeUpdate(queryString.substring(0, queryString.length()-1));
				//queryString = new StringBuilder();
				//queryString.append("insert into " + sqlTable + " values ");
			//}
			Id++;
		}
		rs.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/* Cleans test data for a given occupation (isco code) 
	 * allows to re-generate the data for that specific occupation
	 * in case of errors while parsing the data
	 */
	public static void ProcessOccupations(String filepath, int skip) throws ClassNotFoundException, SQLException, IOException
	{
		System.out.println("\t\t * * * * PROCESSING ASOC OCCUPATION * * * * ");
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt.executeUpdate("truncate table asoc_hrdf.occupation");
		//stmt.executeUpdate("SET CHARACTER SET utf8");
		int Id = 0;
		int i=1;
			StringBuilder queryString = new StringBuilder();
		try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			queryString.append("insert into asoc_hrdf.occupation values ");
			String line = br.readLine();
			for (int j=0; j<skip; j++)
				line = br.readLine();
			

			while (line != null) {
				System.out.println(line);
				queryString.append("(");
				String[] words = line.split("\t");
				
				for (String word : words) {
					if (Id>0)
						queryString.append(",");
					queryString.append("\"" + word.replaceAll("\"", "'") + "\"");
					Id++;
					}
					for (int j=Id; j<7; j++)
						queryString.append(",null");

					Id=0;
				queryString.append("),");
				if (i%100 == 0) {
					System.out.println("\n" + queryString.substring(0, queryString.length()-1));
					stmt.executeUpdate(queryString.substring(0, queryString.length()-1));
					queryString = new StringBuilder();
					queryString.append("insert into asoc_hrdf.occupation values ");
				}
				
				//System.out.println("\t --> (" + queryString.toString() + ")");
				line = br.readLine();
				i++;
			}
		}
		stmt.executeUpdate(queryString.substring(0, queryString.length()-1));
		stmt.close();
		conn.close();
	}

	public static void ProcessSkills(String filepath, int skip) throws ClassNotFoundException, SQLException, IOException
	{
		String query = "";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt.executeUpdate("truncate table esco2017.skill");
		int Id = 1;
		try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			StringBuilder queryString = new StringBuilder();
			queryString.append("insert into esco2017.skill values ");
			String line = br.readLine();
			for (int j=0; j<skip; j++)
				line = br.readLine();
			

			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "\t");
				System.out.println(line);
				queryString.append("(" + Id);
				while (st.hasMoreTokens()) {
					queryString.append(",\"" + st.nextToken().replaceAll("\"", "'") + "\"");
				}
				queryString.append("),");
				if (Id%100 == 0) {
					System.out.println("\n" + queryString.toString());
					stmt.executeUpdate(queryString.substring(0, queryString.length()-1));
					queryString = new StringBuilder();
					queryString.append("insert into esco2017.skill values ");
				}
				
				Id++;
				line = br.readLine();
			}
		}
		stmt.close();
		conn.close();
	}


	public static void ProcessRelationss(String filepath, int skip) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		int Id = 0;
		String relation= "";
		StringBuilder queryString = new StringBuilder();
		try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			String line = br.readLine();
			for (int j=0; j<skip; j++)
				line = br.readLine();
			relation = line.substring(0, line.indexOf("\t"));
			stmt.executeUpdate("truncate table asoc_hrdf." + relation);
			queryString.append("insert into asoc_hrdf." + relation + " values ");
			

			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "\t");
				if (!relation.equalsIgnoreCase(line.substring(0, line.indexOf("\t")))) {
					System.out.println("\n\t ---> " + Id + "  relations \n"+ queryString.substring(0, queryString.length()-1) + "\n");
					stmt.executeUpdate(queryString.substring(0, queryString.length()-1));
					
					relation = line.substring(0, line.indexOf("\t"));
					System.out.println("\n\t---> new relation : " + relation);
					stmt.executeUpdate("truncate table asoc_hrdf." + relation);
					queryString = new StringBuilder();
					queryString.append("insert into asoc_hrdf." + relation + " values ");
					Id = 0;
				}
				System.out.println(line);
				st.nextToken();					
				st.nextToken();					
				queryString.append("(\"" + st.nextToken().replaceAll("\"", "'") + "\",");
				st.nextToken();					
				queryString.append("\"" + st.nextToken().replaceAll("\"", "'") + "\"),");
				if (Id%100 == 0) {
					System.out.println("\n\t ---> " + Id + "  relations \n"+ queryString.substring(0, queryString.length()-1));
					stmt.executeUpdate(queryString.substring(0, queryString.length()-1));
					queryString = new StringBuilder();
					queryString.append("insert into asoc_hrdf." + relation + " values ");
				}
				
				Id++;
				line = br.readLine();
			}
		}
		stmt.close();
		conn.close();
	}

	public static void generateCSV(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
		String[][] relations = {
			{"occupation-essential-knowledge","SELECT distinct 'occupation', occupationURI, 'skill', skillURI FROM esco2017.occupationskills where SkillType='knowledge' and relationshipType= 'essential skill' and occupationURI in (select occupation from esco2017.occupation_en)"},
			{"occupation-optional-knowledge","SELECT distinct 'occupation', occupationURI, 'skill', skillURI FROM esco2017.occupationskills where SkillType='knowledge' and relationshipType= 'optional skill' and occupationURI in (select occupation from esco2017.occupation_en)"},
			{"occupation-essential-skill","SELECT distinct 'occupation', occupationURI, 'skill', skillURI FROM esco2017.occupationskills where SkillType='skill' and relationshipType= 'essential skill' and occupationURI in (select occupation from esco2017.occupation_en)"},
			{"occupation-optional-skill","SELECT distinct 'occupation', occupationURI, 'skill', skillURI FROM esco2017.occupationskills where SkillType='skill' and relationshipType= 'optional skill' and occupationURI in (select occupation from esco2017.occupation_en)"}
		};
		    writer = new BufferedWriter(new FileWriter(new File(output+"relations.csv")));
			System.out.println( "genetationg relations.csv: ");
			
			for (int index=0; index<relations.length; index++)
			{
				rs = stmt.executeQuery(relations[index][1]);
				int i = 0;
				for (; rs.next();)
				{
					if (i>0)
						writer.write("\n");
				    writer.write (relations[index][0] + "\t" + rs.getString(1) + "\t" + rs.getString(2) 
				    + "\t" + rs.getString(3) + "\t" + rs.getString(4) 
				    );
				    i++;
				}
				System.out.println("\t" + relations[index][0] + ": \t" + i + " data objects");
			}
		    writer.close();

	}
	public static void readProperties()
	{
		Properties prop = new Properties();
		InputStream input = null;

		try
		{

			input = new FileInputStream("utilities.properties");

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