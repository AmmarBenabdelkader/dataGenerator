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
import org.xml.sax.helpers.*;
import java.util.*;
import java.io.*;
import java.sql.*;

public class Niri_OccupSkillValidator extends DefaultHandler 
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Map<String, Boolean> elmtStaus = new HashMap<String, Boolean>();
	static int counter;
	static String jobTitle;
	static String jobId;
	static String[] batchQueries = new String[10];
	static 	List<String> words = new ArrayList<String>();

	static public void main(String[] args) throws Exception {
		readProperties();
		removeInvalidSkills();
		removeLessOccuringSkills(1);

	}
	/* Remove skills with length <=2 or length >= 40 
	 */
	public static void removeInvalidSkills() throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		query = "delete from niri.job_skills where length(skill_id)<=2 or length(skill_id)>40";
		System.out.println(stmt.executeUpdate(query) + " Invalid skills have beeing deleted");
		stmt.close();
		conn.close();
	}
	/* Assigns skill level based on keywords within the harvested skills (e.g. good, excellent, fluent, etc.)
	 * Cleans the level keywords from the skill name (skill_id)
	 */
	public static void removeLessOccuringSkills(int occurance) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		query = "create table niri.tmp (SELECT skill_id, count(job_id) counts FROM niri.job_skills group by skill_id having counts<=" + occurance + ")";
		System.out.println(stmt.executeUpdate(query) + " skills to be deleted");
		query = "delete from niri.job_skills where skill_id in (SELECT skill_id from niri.tmp)";
		System.out.println(stmt.executeUpdate(query) + " less occuring skills have beeing deleted");
		query = "drop table niri.tmp";
		stmt.executeUpdate(query);
		stmt.close();
		conn.close();
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
