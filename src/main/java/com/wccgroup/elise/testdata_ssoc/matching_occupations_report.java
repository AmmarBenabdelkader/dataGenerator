/**
 * @author abenabdelkader
 *
 * matching_occupations_report.java
 * Sep 4, 2017
 */
package com.wccgroup.elise.testdata_ssoc;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * @author abenabdelkader
 *
 */
public class matching_occupations_report
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
		Map<String, String> matchedTitles = new HashMap<String, String>();
		
		generateFullReport_v2("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\SSOC2\\data\\");
		//generateFullReport("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\SSOC2\\data\\");
		//generateFullReport_ssocActonomy("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\SSOC2\\data\\");
		
/*		matchedTitles = matchedJobTitles("") ;
		for (Map.Entry<String, String> entry : matchedTitles.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
			
		}
		
		Map<String, String> toBeValidatedTitles = new HashMap<String, String>();
		toBeValidatedTitles = toBeValidatedJobTitles("") ;
		for (Map.Entry<String, String> entry : toBeValidatedTitles.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
			
		}
		
		Map<String, String> toBeMatchedTitles = new HashMap<String, String>();
		toBeMatchedTitles = toBematchedJobTitles("") ;
		for (Map.Entry<String, String> entry : toBeMatchedTitles.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
			
		}
*/	}
	public static void generateFullReport(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output+"full_report3.html")));
		System.out.println( "genetationg to be matched-occupations.html: ");
	    writer.write ("<html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;font-family: calibri; font-size: 12pt;}</style></head>");
	    writer.write ("<body><table  style=\"width:100%\"><tr><th width=\"100%\" colspan=\"4\">Occupations Mapping <br> SSOC v.s ESCO</th></tr>");
	    writer.write ("<tr><td width=\"100%\" colspan=\"4\">Legend: <ol><li>Green: automatic mapping <li> Orange: to be validated <li> Black: to be manually mapped </ol></td></tr>");
	    writer.write ("<tr><th width=\"20%\">SSOC/ISCO Level 4</th><th width=\"40%\">SSOC Code & Title</th><th width=\"40%\">ESCO Title</th></tr>");
		
	    
		System.out.println( "generating " + output+ "full report.html: ");
		// STEP 1: Exact mapping
		String query= "SELECT code, name FROM ssoc.occupation where length(code)=4 order by code";
		ResultSet rs = stmt.executeQuery(query);
		int green = 0; 
		int brown = 0;
		int black = 0;
			int i = 0;
			for (; rs.next();)
			{
			    writer.write ("<tr bgcolor=" + ((i%2==0)?"edede":"efede") + "><td><b>" + rs.getString(1) + "<br>" + rs.getString(2) + "</b></td>");
				query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like 'Exact job title match%' and iscogroup='" + rs.getString(1) + "' order by ssoc_title";
				ResultSet rs2 = stmt2.executeQuery(query);
				String column1="<font color=green>";
				String column2="<font color=green>";
				int j=1;
				
				for (; rs2.next();)
				{
					column1 += rs2.getString(1) + " ## " + rs2.getString(3) + " --> " + j +"<br>";
					column2 += j + "- " + rs2.getString(4) + " ## " + rs2.getString(2) + "<br>";
					j++;
					green++;
				}
				column1 += "</font>";
				column2 += "</font>";
				
				// STEP 2: Partial mapping (to be validated)
				query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like '%SSOC jobTitle%' and iscogroup='" + rs.getString(1) + "' order by ssoc_title, esco_title";
				rs2 = stmt2.executeQuery(query);
				column1+="<font color=brown>";
				column2+="<font color=brown>";
				
				for (; rs2.next();)
				{
					column1 += rs2.getString(1) + " ## " + rs2.getString(3) + " --> " + j +"<br>";
					column2 += j + "- " + rs2.getString(4) + " ## " + rs2.getString(2) + "<br>";
					j++;
					brown++;
				}
				column1 += "</font>";
				column2 += "</font>";
				
				// STEP 3: to be mapped (manually)
				query= "SELECT distinct ssoc, ssoctitle  FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(a.ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by ssoctitle";
				rs2 = stmt2.executeQuery(query);
				
				for (; rs2.next();)
				{
					column1 += rs2.getString(1) + " ## " + rs2.getString(2) + "<br>";
					black++;
				}
				
				query= "SELECT distinct occupation,literal FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by literal";
				rs2 = stmt2.executeQuery(query);
				
				for (; rs2.next();)
				{
					column2 += rs2.getString(2) + ": " + rs2.getString(1) + "<br>";
				}
				
				writer.write ("<td colspan=2 valing=top>" + column1 + "</td>");
				writer.write ("<td colspan=2 valing=top>" + column2 + "</td>");
			    writer.write ("</tr>");
			    i++;
			}
		    writer.write ("<tr><td width=\"100%\" colspan=\"4\">Summary: <ol><li>Green: automatic mapping --> " + green + "<li> Orange: to be validated --> " + brown + "<li> Black: to be manually mapped --> " + black + "</ol></td></tr><");
			writer.write ("</table></html>");
			System.out.println("\t" + i + " data objects");
		
	    writer.close();
			    stmt2.close();
	    stmt.close();
		conn.close();

	}

	public static void generateFullReport_v2(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		Statement stmt3 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		stmt3 = conn.createStatement();
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output+"full_report3.html")));
		System.out.println( "genetationg to be matched-occupations.html: ");
		writer.write ("<html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;font-family: calibri; font-size: 12pt;}</style></head>");
		writer.write ("<body><table  style=\"width:100%\"><tr><th width=\"100%\" colspan=6>Occupations Mapping <br> SSOC v.s ESCO</th></tr>");
		writer.write ("<tr><td width=\"100%\" colspan=6>Legend: <ol><li>Green: automatic mapping <li> Orange: to be validated <li> Black: to be manually mapped </ol></td></tr>");
		writer.write ("<tr><th width=\"20%\" colspan=2 >ISCO Level 4 code & Title</th><th width=\"30%\" colspan=2>SSOC Level 5 Code & Title </th><th width=\"40%\" colspan=2>ESCO level 5 code & Title</th></tr>");


		System.out.println( "generating " + output+ "full report.html: ");
		String query= "SELECT code, name FROM ssoc.occupation where length(code)=4 order by code";
		ResultSet rs = stmt.executeQuery(query);
		int green = 0; 
		int brown = 0;
		int black = 0;
		int i = 0;
		for (; rs.next();)
		{
			String header = "<tr bgcolor=" + ((i%2==0)?"edede":"efede") + "><td><b>" + rs.getString(1) + "</td><td>" + rs.getString(2) + "</b></td>";
			// STEP 1: Exact mapping
			query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like 'Exact job title match%' and iscogroup='" + rs.getString(1) + "' order by ssoc_title";
			ResultSet rs2 = stmt2.executeQuery(query);
			String column1=""; //<font color=green>";
			String column2=""; //<font color=green>";
			int j=1;

			for (; rs2.next();)
			{
				column1 += header + "<td>" + rs2.getString(1) + "</td><td><font color=green>" + rs2.getString(3) + "</font></td>" +
				 "<td><font color=green>" + rs2.getString(4) + "</td><td><font color=green> " + rs2.getString(2) + "</font></td></tr>";
				j++;
				green++;
			}
			//column1 += "</font>";
			//column2 += "</font>";

			// STEP 2: Partial mapping (to be validated)
			query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like '%SSOC jobTitle%' and iscogroup='" + rs.getString(1) + "' order by ssoc_title, esco_title";
			rs2 = stmt2.executeQuery(query);
			//column1+="<font color=brown>";
			//column2+="<font color=brown>";

			for (; rs2.next();)
			{
				column1 += header + "<td>" + rs2.getString(1) + "</td><td><font color=brown>" + rs2.getString(3) + "</font></td>" +
					"<td><font color=brown>" + rs2.getString(4) + "</td><td><font color=brown>" + rs2.getString(2) + "</font></td></tr>";
				j++;
				brown++;
			}
			//column1 += "</font>";
			//column2 += "</font>";

			// STEP 3: to be mapped (manually)
			String column3 = "";
			query= "SELECT distinct ssoc, ssoctitle  FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(a.ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by ssoctitle";
			rs2 = stmt2.executeQuery(query);

			for (; rs2.next();)
			{
				//column1 += header + "<td>" + rs2.getString(1) + "</td><td><font color=black>" + rs2.getString(2) + "</font></td>";
				black++;

			query= "SELECT distinct occupation,literal FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by literal";
			ResultSet rs3 = stmt3.executeQuery(query);

			for (; rs3.next();)
			{
				column2 += header + "<td>" + rs2.getString(1) + "</td><td><font color=black>" + rs2.getString(2) + "</font></td>" + 
					"<td>" + rs3.getString(2) + "</td><td><font color=black>" + rs3.getString(1) + "</font></td></tr>";
			}
			}

/**/
			writer.write (column1);
			writer.write (column2);
			//writer.write ("<td valing=top>" + column1 + "</td>");
			//writer.write ("<td valing=top>" + column2 + "</td>");
			writer.write ("</tr>");
			i++;
		}
		writer.write ("<tr><td width=\"100%\" colspan=\"4\">Summary: <ol><li>Green: automatic mapping --> " + green + "<li> Orange: to be validated --> " + brown + "<li> Black: to be manually mapped --> " + black + "</ol></td></tr><");
		writer.write ("</table></html>");
		System.out.println("\t" + i + " data objects");

		writer.close();
		stmt3.close();
		stmt2.close();
		stmt.close();
		conn.close();

	}

	public static void generateFullReport_ssocActonomy(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(output+"fullReport_ssoc-ontology2.html")));
		System.out.println( "genetationg to be matched-occupations.html: ");
	    writer.write ("<html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;font-family: calibri; font-size: 12pt;}</style></head>");
	    writer.write ("<body><table  style=\"width:100%\"><tr><th width=\"100%\" colspan=\"4\">Occupations Mapping <br> SSOC v.s Ontology</th></tr>");
	    writer.write ("<tr><td width=\"100%\" colspan=\"4\">Legend: <ol><li>black: automaticaly mapped <li> orange: to be approved  <li> red: not mapped </ol></td></tr>");
	    writer.write ("<tr><th width=\"20%\">SSOC Level 4</th><th width=\"40%\">SSOC Code & Title</th><th width=\"40%\">Ontology Title</th></tr>");
		
	    
		System.out.println( "generating " + output+ "full report.html: ");
		// STEP 1: Exact mapping
		String query= "SELECT code, name FROM ssoc.occupation where length(code)=4 order by code";
		ResultSet rs = stmt.executeQuery(query);
		int black = 0;
		int orange = 0;
		int red = 0; 
			int i = 0;
			for (; rs.next();)
			{
			    writer.write ("<tr bgcolor=" + ((i%2==0)?"edede":"efede") + "><td><b>" + rs.getString(1) + "<br>" + rs.getString(2) + "</b></td>");
				query= "SELECT a.ssoc_code, c.name, a.actonomy_code, b.name, a.score FROM ssoc2.ssoc_actonomy_occupation a, ssoc2.actonomy_terms b , ssoc.occupation c "
					+ "where a.ssoc_code=c.code and a.actonomy_code=b.code and a.score>=95 and b.parent='Function' "
					+ "and a.ssoc_code like '" + rs.getString(1) + "%'";
				//System.out.println(query);
				ResultSet rs2 = stmt2.executeQuery(query);
				String column1="<font color=black>";
				String column2="<font color=black>";
				int j=1;
				
				for (; rs2.next();)
				{
					column1 += rs2.getString(1) + ": " + rs2.getString(2) + " --> " + j + " (" + (rs2.getString(2).equalsIgnoreCase(rs2.getString(4))?"100":rs2.getString(5)) + "%)<br>";
					column2 += j + "- " + rs2.getString(4) + "<br>";
					j++;
					black++;
				}
				column1 += "</font>";
				column2 += "</font>";
				
				// STEP 2: Partial mapping (to be validated)
				query= "SELECT a.ssoc_code, c.name, a.actonomy_code, b.name, a.score FROM ssoc2.ssoc_actonomy_occupation a, ssoc2.actonomy_terms b , ssoc.occupation c "
					+ "where a.ssoc_code=c.code and a.actonomy_code=b.code and a.score<95 and b.parent='Function' "
					+ "and a.ssoc_code like '" + rs.getString(1) + "%'";
				rs2 = stmt2.executeQuery(query);
				column1+="<font color=orange>";
				column2+="<font color=orange>";
				
				for (; rs2.next();)
				{
					column1 += rs2.getString(1) + ": " + rs2.getString(2) + " --> " + j + " (" + rs2.getString(5) + "%)<br>";
					column2 += j + "- " + rs2.getString(4) + "<br>";
					j++;
					orange++;
				}
				column1 += "</font>";
				column2 += "</font>";
				
				// STEP 2: to be manually mapped
				query= "SELECT code, name FROM ssoc.occupation where code not in (select distinct ssoc_code from ssoc2.ssoc_actonomy_occupation) "
					+ "and code like '" + rs.getString(1) + "%' and length(code)>4";
				rs2 = stmt2.executeQuery(query);
				column1+="<font color=red>";
				column2+="<font color=red>";
				
				for (; rs2.next();)
				{
					column1 += rs2.getString(1) + ": " + rs2.getString(2) + "<br>";
					//column2 += j + "- " + rs2.getString(4) + "<br>";
					j++;
					red++;
				}
				column1 += "</font>";
				column2 += "</font>";
				
				writer.write ("<td valing=top>" + column1 + "</td>");
				writer.write ("<td valing=top>" + column2 + "</td>");
			    writer.write ("</tr>");
			    i++;
			}
		    writer.write ("<tr><td width=\"100%\" colspan=\"4\">Summary: <ol><li>black: automaticaly mapped --> " + black + "<li> orange: to be approved --> " + orange + "<li> red: not mapped --> " + red + "</ol></td></tr><");
			writer.write ("</table></html>");
			System.out.println("\t" + i + " data objects");
		
	    writer.close();
			    stmt2.close();
	    stmt.close();
		conn.close();

	}

	public static Map<String, String> matchedJobTitles(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		Map<String, String> matchedTitles = new HashMap<String, String>();
	    //BufferedWriter writer;
	    
		String query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like 'Exact job title match%' order by ssoc_title";
	    //writer = new BufferedWriter(new FileWriter(new File(output+"exact-occupations.html")));
		System.out.println( "generating " + output+ "exact-matching-occupations.html: ");
	    //writer.write ("<html><head><style>table, th, td {border: 1px solid black;border-collapse: collapse;font-family: calibri; font-size: 12pt;}</style></head>");
	    //writer.write ("<body><table  style=\"width:100%\"><tr><th width=\"100%\" colspan=\"6\">Occupations automatically mapped </th></tr>");
	    //writer.write ("<tr><th width=\"5%\">ISCO Group</th><th width=\"5%\">SSOC Code</th><th width=\"25%\">SSOC Title</th><th width=\"25%\">ESCO Title</th><th width=\"22%\"> Match Type</th><th width=\"18%\">ESCO Code</th></tr>");

		
			ResultSet rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
				//String value = "<tr bgcolor=" + ((i%2==0)?"edede":"efede") + "><td><b>" + rs.getString(5) + "</b></td><td>" + rs.getString(1) + "</td><td>" + rs.getString(3) + "</td><td>" + rs.getString(4) + "</td><td>" + rs.getString(6) + "</td><td>" + rs.getString(2) + "</td></tr>\n";
				String value = rs.getString(5) + "_#_" + rs.getString(3) + "_#_" + rs.getString(4) + "_#_" + rs.getString(6) + "_#_" + rs.getString(2);
				matchedTitles.put(rs.getString(1), value);
			    //writer.write (value);
			    i++;
			}
			System.out.println("\t" + i + " data objects");
		
		//writer.write ("</table><html>");
	    //writer.close();
	    stmt.close();
		conn.close();
		return matchedTitles;

	}

	public static Map<String, String> toBeValidatedJobTitles(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		Map<String, String> jobTitles = new HashMap<String, String>();
		String query= "SELECT ssoc_code ,esco_code ,ssoc_title ,esco_title ,iscogroup ,Remark FROM ssoc2.ssoc_to_esco where remark like '%SSOC jobTitle%' order by ssoc_title, esco_title";
	    
		System.out.println( "generating " + output+ "to-validate-matching-occupations.html: ");
		ResultSet rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
				String value = rs.getString(5) + "_#_" + rs.getString(3) + "_#_" + rs.getString(4) + "_#_" + rs.getString(6) + "_#_" + rs.getString(2);
				jobTitles.put(rs.getString(1) + "_" + i, value);
			    i++;
			}
			System.out.println("\t" + i + " data objects");
		
	    stmt.close();
		conn.close();
		return jobTitles;

	}

	public static Map<String, String> toBematchedJobTitles(String output) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		Statement stmt2 = conn.createStatement();
		Statement stmt3 = conn.createStatement();
		Map<String, String> jobTitles = new HashMap<String, String>();
		String query= "SELECT distinct ssoc_code, ssoc_description FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by ssoc_code, ssoc_description;";
		ResultSet rs = stmt.executeQuery(query);
		ResultSet rs2, rs3;
		int i = 0;
		System.out.println( "generating " + output+ "to-be-matched-occupations.html: ");
		for (; rs.next();)
		{
			query= "SELECT distinct ssoc, ssoctitle  FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(a.ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by ssoctitle";
			String value = rs.getString(2) + "_#_";
			rs2 = stmt2.executeQuery(query);
			while (rs2.next()) 
				value += (rs2.getString(1) + ": " + rs2.getString(2) + "_#_");
			
			query= "SELECT distinct occupation,literal FROM ssoc2.ssoc_2015 a, esco2017.occupation_en b, ssoc.ssoc2015_isco08 c where left(ssoc,4)=c.ssoc_code and iscogroup=isco_code and left(a.ssoc,4)='" + rs.getString(1) + "' and length(ssoc)=5 and ssoc not in (SELECT ssoc_code FROM ssoc2.ssoc_to_esco) order by literal";
			//System.out.println("\tquery: " + query);
			rs3 = stmt3.executeQuery(query);
			while (rs3.next()) 
				value += (rs3.getString(2) + "_##_");
			jobTitles.put(rs.getString(1) + "_" + i, value);
			
		    rs2.close();
		    rs3.close();
		    i++;
		}
	    
		System.out.println("\t" + i + " data objects");
		
	    stmt.close();
	    stmt2.close();
	    stmt3.close();
		conn.close();
		return jobTitles;

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
