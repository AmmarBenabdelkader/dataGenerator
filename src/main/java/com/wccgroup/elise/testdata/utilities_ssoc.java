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

public class utilities_ssoc
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
			System.out.println("\t- 1- Link SSOC occupations to ESCO");
			System.out.println("\t- 2- Matching Occupations: SSOC to ESCO");
			System.out.println("\t- 3- To be Matched Occupations: SSOC to ESCO");
			System.out.println("\t- 4- Occupation Alternative/Hidden Titles");
			System.out.println("\t- 5- Skill Alternative/Hidden Titles");
			System.out.println("\t- 6- Generate CSVs ESCO");
			System.out.println("\t- 7- Generate CSVs SSOC");
			System.out.println("\t- 8- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			SSOC_to_ESCO("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\SSOC\\data\\");
			break;
		case "2":
			//SSOC_ESCO_matching_occupations("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\SSOC2\\data\\");
			SSOC_ESCO_matching_occupations_html("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\SSOC2\\data\\");
			break;
		case "3":
			SSOC_ESCO_tobe_matched_occupations("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\SSOC2\\data\\");
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
			generateCSV_esco("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\ESCO_2017\\");
			break;
		case "7":
			generateCSV_ssoc("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\SSOC\\main\\");
			break;
		default:
			System.out.println("Bye"); // quit

		}

	}
	public static void SSOC_to_ESCO(String output) throws ClassNotFoundException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		try
		{
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    
		String[][] relations = {
			{"drop table if exists","drop table if exists ssoc2.ssoc_to_esco"},
			{"Exact job title match, same isco level 4","create table ssoc2.ssoc_to_esco (SELECT distinct ssoc ssoc_code, occupation esco_code, ssocTitle ssoc_title, literal esco_title, iscogroup, 'Exact job title match, same isco level 4' Remark FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b where left(ssoc,4)=iscogroup and length(ssoc)=5 and a.ssoctitle = b.literal )"},
			{"Adjusting table column Remark","ALTER TABLE ssoc2.ssoc_to_esco CHANGE COLUMN Remark Remark VARCHAR(255);"},
			{"Adjusting table column iscogroup","ALTER TABLE ssoc2.ssoc_to_esco CHANGE COLUMN iscogroup iscogroup VARCHAR(45);"},
			{"Exact job title match, different isco level 4","insert into ssoc2.ssoc_to_esco (SELECT distinct ssoc ssoc_code, occupation esco_code, ssocTitle ssoc_title, literal esco_title, iscogroup, 'Exact job title match, different isco level 4' Remark FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b where left(ssoc,4)!=iscogroup and length(ssoc)=5 and a.ssoctitle=b.literal and a.ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco))"},
			{"SSOC jobTitle as part of ESCO jobTitle, same isco level 4","insert into ssoc2.ssoc_to_esco (SELECT distinct ssoc ssoc_code, occupation esco_code, ssocTitle ssoc_title, literal esco_title, iscogroup, 'SSOC jobTitle as part of ESCO jobTitle, same isco level 4' Remark FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and length(ssoc)=5 and b.literal like concat('%', a.ssoctitle, '%') and length(isco_code)>3 and a.ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco))"},
			{"ESCO jobTitle as part of SSOC jobTitle, same isco level 4","insert into ssoc2.ssoc_to_esco (SELECT distinct ssoc ssoc_code, occupation esco_code, ssocTitle ssoc_title, literal esco_title, iscogroup, 'ESCO jobTitle as part of SSOC jobTitle, same isco level 4' Remark FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and length(ssoc)=5 and a.ssoctitle like concat('%', b.literal, '%') and length(isco_code)>3 and a.ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco))"}
		};
		
		System.out.println( "creating the link SSOC_to_ESCO with data:");
			
			for (int index=0; index<relations.length; index++)
			{
				System.out.println("\t" + relations[index][0] + ": \t" + stmt.executeUpdate(relations[index][1]) + " data objects");
			}
			stmt.close();
			conn.close();
		}
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

	}

	
	public static void SSOC_ESCO_matching_occupations_html(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
	    
		String query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like 'Exact job title match%' order by ssoc_title";
	    writer = new BufferedWriter(new FileWriter(new File(output+"exact-occupations.html")));
		System.out.println( "generating " + output+ "exact-matching-occupations.html: ");
	    writer.write ("<html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;font-family: calibri; font-size: 12pt;}</style></head>");
	    writer.write ("<body><table  style=\"width:100%\"><tr><th width=\"100%\" colspan=\"6\">Occupations automatically mapped </th></tr>");
	    writer.write ("<tr><th width=\"5%\">ISCO Group</th><th width=\"5%\">SSOC Code</th><th width=\"25%\">SSOC Title</th><th width=\"25%\">ESCO Title</th><th width=\"22%\"> Match Type</th><th width=\"18%\">ESCO Code</th></tr>");

		
			//System.out.println("\tRetrieving mat: \t" + stmt.executeQuery(query) + " data objects");
			ResultSet rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
			    writer.write ("<tr bgcolor=" + ((i%2==0)?"edede":"efede") + "><td><b>" + rs.getString(5) + "</b></td><td>");
			    writer.write (rs.getString(1) + "</td><td>" + rs.getString(3) + "</td><td>" + rs.getString(4) + "</td><td>" + rs.getString(6) + "</td><td>" + rs.getString(2) + "</td></tr>");
			    writer.write ("\n");
			    i++;
			}
			System.out.println("\t" + i + " data objects");
		
		writer.write ("</table><html>");
	    writer.close();
		query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like '%SSOC jobTitle%' order by ssoc_title, esco_title";
	    
	    writer = new BufferedWriter(new FileWriter(new File(output+"to-validate-occupations.html")));
		System.out.println( "generating " + output+ "to-validate-matching-occupations.html: ");
	    writer.write ("<html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;font-family: calibri; font-size: 12pt;}</style></head>");
	    writer.write ("<body><table  style=\"width:100%\"><tr><th width=\"100%\" colspan=\"6\">Occupations mapping to be validated </th></tr>");
	    writer.write ("<tr><th width=\"5%\">ISCO Group</th><th width=\"5%\">SSOC Code</th><th width=\"25%\">SSOC Title</th><th width=\"25%\">ESCO Title</th><th width=\"22%\"> Match Type</th><th width=\"18%\">ESCO Code</th></tr>");

		
			//System.out.println("\tRetrieving mat: \t" + stmt.executeQuery(query) + " data objects");
			rs = stmt.executeQuery(query);
			i = 0;
			for (; rs.next();)
			{
			    writer.write ("<tr bgcolor=" + ((i%2==0)?"edede":"efede") + "><td><b>" + rs.getString(5) + "</b></td><td>");
			    writer.write (rs.getString(1) + "</td><td>" + rs.getString(3) + "</td><td>" + rs.getString(4) + "</td><td>" + rs.getString(6) + "</td><td>" + rs.getString(2) + "</td></tr>");
			    writer.write ("\n");
			    i++;
			}
			System.out.println("\t" + i + " data objects");
		
		writer.write ("</table><html>");
	    writer.close();
	    stmt.close();
			conn.close();

	}

	public static void SSOC_ESCO_matching_occupations(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
	    
		String query= "SELECT concat(ssoc_code ,'\t', esco_code ,'\t', ssoc_title ,'\t', esco_title ,'\t', iscogroup ,'\t', Remark) line FROM ssoc2.ssoc_to_esco where remark like 'Exact job title match%'";
	    writer = new BufferedWriter(new FileWriter(new File(output+"exact-matching-occupations.csv")));
		System.out.println( "generating " + output+ "exact-occupations.csv: ");
	    writer.write ("soc_code\tesco_code\tssoc_title\tesco_title\tiscogroup\tRemark");
		
			//System.out.println("\tRetrieving mat: \t" + stmt.executeQuery(query) + " data objects");
			ResultSet rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1));
			    i++;
			}
			System.out.println("\t" + i + " data objects");
		
	    writer.close();

		query= "SELECT concat(ssoc_code ,'\t', esco_code ,'\t', ssoc_title ,'\t', esco_title ,'\t', iscogroup ,'\t', Remark) line FROM ssoc2.ssoc_to_esco where remark like '%SSOC jobTitle%'";
	    writer = new BufferedWriter(new FileWriter(new File(output+"to-validate-matching-occupations.csv")));
		System.out.println( "generating " + output+ "to-validate-occupations.csv: ");
	    writer.write ("soc_code\tesco_code\tssoc_title\tesco_title\tiscogroup\tRemark");
		
			//System.out.println("\tRetrieving mat: \t" + stmt.executeQuery(query) + " data objects");
			rs = stmt.executeQuery(query);
			i = 0;
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1));
			    i++;
			}
			System.out.println("\t" + i + " data objects");
		
	    writer.close();
	    stmt.close();
			conn.close();

	}

	
	public static void SSOC_ESCO_tobe_matched_occupations(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		Statement stmt2 = conn.createStatement();
		Statement stmt3 = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
	    
		String query= "SELECT distinct ssoc_code, ssoc_description FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by ssoc_code, ssoc_description;";
		    writer = new BufferedWriter(new FileWriter(new File(output+"to be matched-occupations.html")));
			System.out.println( "genetationg to be matched-occupations.html: ");
		    writer.write ("<html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;font-family: calibri; font-size: 12pt;}</style></head>");
		    writer.write ("<body><table  style=\"width:100%\"><tr><th width=\"100%\" colspan=\"4\">Occupations to be mapped manually</th></tr>");
		    writer.write ("<tr><th width=\"20%\">SSOC/ISCO Level 4</th><th width=\"40%\">SSOC Code & Title</th><th width=\"40%\">ESCO Code & Title</th></tr>");
			
			System.out.println("query: " + query);
				//System.out.println("\tRetrieving mat: \t" + stmt.executeQuery(query) + " data objects");
				ResultSet rs = stmt.executeQuery(query);
				ResultSet rs2, rs3;
				int i = 0;
				for (; rs.next();)
				{
				    writer.write ("<tr bgcolor=" + ((i%2==0)?"edede":"efede") + "><td><b>" + rs.getString(1) + "<br>" + rs.getString(2) + "</b></td><td>");
					query= "SELECT distinct ssoc, ssoctitle  FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(a.ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by ssoctitle";
					//System.out.println("\tquery: " + query);
					rs2 = stmt2.executeQuery(query);
					while (rs2.next()) 
					    writer.write (rs2.getString(1) + ": " + rs2.getString(2) + "<br>");
				    writer.write ("</td><td>");
					
					query= "SELECT distinct occupation,literal FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by literal";
					//System.out.println("\tquery: " + query);
					rs3 = stmt3.executeQuery(query);
					while (rs3.next()) 
					    writer.write (rs3.getString(2) + "<br>");
				    	//writer.write (rs3.getString(1) + ": " + rs3.getString(2) + "<br>");
				    writer.write ("</td></tr>");
						
				    rs2.close();
				    rs3.close();
				    i++;
				}
				writer.write ("</table><html>");
				System.out.println("\t" + i + " ISCO occupations");
			
		    writer.close();
		    rs.close();
			stmt.close();
			stmt2.close();
			conn.close();

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

	public static void generateCSV_esco(String output) throws ClassNotFoundException, SQLException, IOException
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
	public static void generateCSV_ssoc(String output) throws ClassNotFoundException, SQLException, IOException
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

			input = new FileInputStream("ssoc.properties");

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