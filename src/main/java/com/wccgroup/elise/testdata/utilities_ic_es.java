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

public class utilities_ic_es
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static String stopWords = "#a#ante#bajo#con#contra#de#desde#detrás#en#entre#hacia#hasta#para#por#según#sin#sobre#tras#la#lo#no#se#su#un#bar#cal#del#gas#las#ley#los#mar#oro#paz#pop#que#red#sus#vía#e#o#y#al#da#el#"; // prepositions and prefixes 
	

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
			System.out.println("\t- 3- Process Occupation Skills");
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
			ProcessOccupations("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\ESCO\\occupations.csv", 1);
			break;
		case "2":
			ProcessSkills("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\ESCO\\skill.csv", 1);
			break;
		case "3":
			ProcessOccupationSkills("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\ESCO\\relationships.csv", 1);
			break;
		case "4":
			ProcessTitles("esco_v0.occupation_ic", "SELECT distinct conceptURI, ConceptPT FROM esco_v0.occupation;");
			//ProcessTitles("esco2017.HiddenTitles", "SELECT distinct occupation, HiddenTitles FROM esco2017.occupation_en where HiddenTitles!='NA';");
			break;
		case "5":
			ProcessTitles("esco2017.alternativeskilltitles", "SELECT distinct skill, AlternativeTitles FROM esco2017.skill where alternativetitles!='NA';");
			ProcessTitles("esco2017.HiddenSkillTitles", "SELECT distinct skill, HiddenTitles FROM esco2017.skill where HiddenTitles!='NA';");
			break;
		case "6":
			generateJSON("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\niri\\IC\\spanish\\");
			generateMappings("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\niri\\IC\\spanish\\", "SELECT distinct code, concept FROM esco_v0.occupation_ic;");
			generateWordClusters("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\niri\\IC\\spanish\\", "SELECT distinct concept FROM esco_v0.occupation_ic;");
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
		StringBuilder queryString = new StringBuilder();
		queryString.append("insert into " + sqlTable + " values ");
		String gender="U";
		while (rs.next()) {
			StringTokenizer st = new StringTokenizer(rs.getString(2), " ");
			//queryString.append("insert into " + sqlTable + " values ");
			while (st.hasMoreTokens()) {
				gender = "U";
				String temp = st.nextToken().replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", "").trim();
				if (temp.contains("/a"))
				{
					queryString.append("("+ rs.getString(1) + ",\"" + temp.replace("/a", "") + "\", 'M'),");
					temp = temp.replace("e/a", "a");
					temp = temp.replace("o/a", "a");
					temp = temp.replace("/a", "a");
					gender = "F";

				}
				if (!stopWords.contains("#" + temp + "#"))
					queryString.append("("+ rs.getString(1) + ",\"" + temp + "\",'" + gender + "'),");
			}
			if (Id%200 == 0) {
				System.out.println(Id + "\t- " + queryString.substring(0, queryString.length()-1));
				stmt2.executeUpdate(queryString.substring(0, queryString.length()-1));
				queryString = new StringBuilder();
				queryString.append("insert into " + sqlTable + " values ");
			}
			Id++;
		}
		stmt2.executeUpdate(queryString.substring(0, queryString.length()-1));
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
		String query = "";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt.executeUpdate("truncate table esco2017.occupation");
		//stmt.executeUpdate("SET CHARACTER SET utf8");
		int Id = 1;
		try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			StringBuilder queryString = new StringBuilder();
			queryString.append("insert into esco2017.occupation values ");
			String line = br.readLine();
			for (int j=0; j<skip; j++)
				line = br.readLine();
			

			while (line != null) {
				//if (line.contains("@es")) {
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
					queryString.append("insert into esco2017.occupation values ");
				}
				
				Id++;
				//System.out.println("\t --> (" + queryString.toString() + ")");
				line = br.readLine();
			//}
			}
		}
		//query = "create table matchtest.matchOwn66 (SELECT job_id, sum(match_score)/count(candidate_id) avgMatchOwn, count(candidate_id) counts FROM matchtest.matching where match_type='MatchOwn' and elise_version= '6.6' group by job_id)";
		//stmt.executeUpdate(query);
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


	public static void ProcessOccupationSkills(String filepath, int skip) throws ClassNotFoundException, SQLException, IOException
	{
		String query = "";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt.executeUpdate("truncate table esco2017.occupationskills");
		int Id = 1;
		try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			StringBuilder queryString = new StringBuilder();
			queryString.append("insert into esco2017.occupationskills values ");
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
					queryString.append("insert into esco2017.occupationskills values ");
				}
				
				Id++;
				line = br.readLine();
			}
		}
		stmt.close();
		conn.close();
	}

	public static void generateJSON(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
		String query = "SELECT distinct conceptURI, conceptPT FROM esco_v0.occupation";
		    writer = new BufferedWriter(new FileWriter(new File(output+"SpanishTaxonomyData.json")));
			System.out.println( "genetationg " + output+"SpanishTaxonomyData.json");
			writer.write("[{\n\t\t\"version\" : \"" + new Date() + "\",\n\t\t\"id\" : \"activity\",\n\t\t\"name\" : \"Ocupaciones y habilidades\",\n\t\t\"nodes\" : []\n");
			writer.write("\t}, {\n\t\t\"version\" : \"" + new Date() + "\",\n\t\t\"id\" : \"Ocupaciones\",\n\t\t\"name\" : \"Ocupacion\",\n\t\t\"nodes\" : [\"");
			
				rs = stmt.executeQuery(query);
				int i = 0;
				for (; rs.next();)
				{
					if (i>0)
						writer.write(", ");
				    writer.write ("{\n\t\t\t\t\"code\" : \"" + rs.getString(1) + "\"," 
						+ "\n\t\t\t\t\"name\" : \"" + rs.getString(2) + "\","  
						+ "\n\t\t\t\t\"relations\" [],"  
						+ "\n\t\t\t\t\"properties\" [],"  
						+ "\n\t\t\t\t\"childNodes\" [],"  
						+ "\n\t\t\t}"
				    );
				    i++;
				}
				writer.write("]\n\t\t\t}\n\t\n]");
				System.out.println("\t" + i + " data objects");
		    writer.close();

	}

	public static void generateWordClusters(String output, String query) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
		    writer = new BufferedWriter(new FileWriter(new File(output+"SpanishWordClusters.json")));
			System.out.println( "genetationg " + output+"SpanishWordClusters.json");

			int i = 0;
			Map<String, StringBuilder> wordLemmas = new HashMap<String, StringBuilder>();
			rs = stmt.executeQuery("select distinct code,lemma from esco_v0.lemma");
			while (rs.next())
			{
					wordLemmas.computeIfAbsent(rs.getString("code").toLowerCase() , key -> new StringBuilder()).append( 
						rs.getString("lemma") + "\t" );
			}
			System.out.println("\t ---> " + wordLemmas.size() + " codes");
			
			rs = stmt.executeQuery("select distinct lemma, code from esco_v0.lemma where lemma not in (select code from esco_v0.lemma)");
			while (rs.next())
			{
					wordLemmas.computeIfAbsent(rs.getString("lemma").toLowerCase() , key -> new StringBuilder()).append( 
						rs.getString("code") + "\t" );
			}
			System.out.println("\t ---> " + wordLemmas.size() + " lemmas");
			System.out.println("\t ---> value for 'ocupaciones': '" + wordLemmas.get("ocupaciones") + "'");
			rs.close();


			writer.write("[");
			
				rs = stmt.executeQuery(query);
				i = 0;
				for (; rs.next();)
				{
					if (i>0)
						writer.write(", ");
				    writer.write ("\n\t{\"name\" : \"" + rs.getString(1) + "\",\"words\": [" );
					if (rs.getString(1).equalsIgnoreCase("ocupaciones")) {
						System.out.println("\t ---> value for '" + rs.getString(1) + "': '" + wordLemmas.get(rs.getString(1).toLowerCase()) + "'");
						System.out.println("\t ---> value for 'ocupaciones': '" + wordLemmas.get("ocupaciones") + "'");
					}
				    if (wordLemmas.get(rs.getString(1).toLowerCase())!=null) {
						String[] words = wordLemmas.get(rs.getString(1).toLowerCase()).toString().split("\t");
						
						for (String word : words)
						{
							writer.write ("\n\t\t{\"value\" : \"" + word + "\",\"wordType\":\"LEMMA\"}" );
							if (i>0)
								writer.write(", ");
					    	
						}
				    }
					writer.write("\n\t]}");
				    i++;
					if (i%100==0)
						System.out.println("\t ---> " + i + " data objects");
				}
				writer.write("\n]");
				System.out.println("\t" + i + " data objects (total)");
		    writer.close();
		    rs.close();
		    stmt.close();
		    conn.close();

	}

	public static void generateMappings(String output, String query) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
		    writer = new BufferedWriter(new FileWriter(new File(output+"SpanishOccupationMappings.json")));
			System.out.println( "genetationg " + output+"SpanishOccupationMappings.json");
			writer.write("[");
			
				rs = stmt.executeQuery(query);
				int i = 0;
				for (; rs.next();)
				{
					if (i>0)
						writer.write(",");
				    writer.write ("\n\t{\"listvalueCode\" : \"" + rs.getString(1) + "\",\"wordClusterName\": \"" + rs.getString(2) + "\"}"
				    );
				    i++;
				}
				writer.write("\n]");
				System.out.println("\t" + i + " data objects");
		    writer.close();

	}

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