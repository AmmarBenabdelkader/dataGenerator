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
import java.io.*;
import java.sql.*;

public class Niri_SkillValidator 
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
		String[][] skillLevels = {
			{"Good in","Good"},
			{"Good","Good"},
			{"Very good","Excellent"},
			{"Very strong","Excellent"},
			{"Excellent in","Excellent"},
			{"Excellent","Excellent"},
			{"Strong","Strong"},
			{"Demonstrate strong","Strong"},
			{"Fluency in","fluent"},
			{"Fluent in","Fluent"},
			{"Fluent","Fluent"},
			{"Proficiency in","Proficiency"},
			{"Proficient with","Proficient"},
			{"Proficient in","Proficient"},
			{"Advance in","Advanced"},
			{"Advance","Advanced"},
			{"Advanced in","Advanced"},
			{"Advanced","Advanced"},
			{"Basic","Basic"},
			{"Effective","Excellent"},
			{"High level of","Excellent"},
			{"High level","Excellent"},
		};
		String[] preStopwords = {
			"command in","command of","command","background in","background",
			"knowledge in","knowledge of","Knowledge",
			"level in","level of","level","skills in","verbal and written","written and spoken","oral and written","written and oral",
			"written and verbal","written and oral"};
		String[] postStopwords = {
			"skills","writing and reading","speaking", "skill", "is a must", "is required", "language", "languages",
			"(written and oral)", "is a plus", "is essential", "is highly", "is an advantage", "is desirable",
			"would be advantageous"};
		String[][] skillSynonyms = {
			{"Computer literate","Computer literacy"},
			{"Computer","Computer literacy"},
			{"PC literate","Computer literacy"},
			{"PC","Computer literacy"},
			{"MS Office","Microsoft Office"},
			{"Microsoft office programs","Microsoft Office"},
			{"Microsoft office skills","Microsoft Office"},
			{"Microsoft office software","Microsoft Office"},
			{"Microsoft office suite","Microsoft Office"},
			{"MS applications","Microsoft Applications"},
			{"MS Excel","Microsoft Excel"},
			{"MS project","Microsoft Project"},
			{"","Microsoft Office"},
			{"MS SQL","Microsoft SQL Server"},
			{"MS Windows","Microsoft Windows"},
			{"MS Visual Studio 2010","Microsoft Visual Studio"},
			{"office software","Microsoft Office"},
			{"office","Microsoft Office"},
			{"English and Arabic","Arabic and English"},
			{"MIS","Management Information Systems"},
			{"Arabic speaker","Arabic"},
			{"communication skills, both verbal and written","communication"},
			{"communication (written and oral)","communication"},
			{"communications","communication"},
			{"Team Leadership","leadership"},
			{"Bilingual (Arabic/English)","Arabic and English"},
			{"Auto CAD","AUTOCAD"},
			{"Administration","Administrative"},
			{"Organizing","Organizational"},
			{"report writing","Reporting"},
		};
		String[] toSplit_and = {
			"Arabic and English","communication and interpersonal","communication and presentation", "leadership and management", 
			"planning and organizing", "leadership and communication", "interpersonal and communication","Management and Leadership",
			"Influencing and negotiating","report writing and communication","Accounting and finance", "Accounting and Financial principles and procedures",
			"XML and XML Schema","leadership and a strategic thinker","Planning and Organizational","administrative and organizational","interpersonal and leadership",
			"interpersonal and negotiation"};
				
		String[] toSplit_comma = {
			"interpersonal, communication and presentation", "Must be able to react, analyse",
			"English, Arabic and Hindi","English, Arabic","word processing, spreadsheet",
			"PC, LAN/WAN","interpersonal, communication and organizational",
			"analytic, communication","Data Entry Management, General Math","Accounting, Corporate Finance"};
		String[] toSplit_slash = {
			"Arabic/English","English/Arabic","LAN/WAN", "Java / J2EE technology"};
		String[] toClean = {
			"(written and oral)","(written and spoken)","(read, write, speak)","(both written and verbal)",
			"(written and oral)"};
		readProperties();
		cleanSkills(toClean);
		assignSkillLevel(skillLevels);
		cleanSkillPreStopwords(preStopwords);
		cleanSkillPostStopwords(postStopwords);
		resolveSkillSynonyms(skillSynonyms);
		
		splitSkills(toSplit_and, " and ");
		splitSkills(toSplit_comma, ",");
		splitSkills(toSplit_slash, "/");
		splitSkills(toSplit_and, " and ");

	}
	/* Assigns skill level based on keywords within the harvested skills (e.g. good, excellent, fluent, etc.)
	 * Cleans the level keywords from the skill name (skill_id)
	 */
	public static void assignSkillLevel(String[][] skills) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		
		for (int i=0; i<skills.length; i++) {
			System.out.print("\nAssigning skills level: '" + skills[i][0] + "' ... ");
			query = "update niri.job_skills set skill_id=right(skill_id_original,length(skill_id_original) - " + (skills[i][0].length()+1) + ") where skill_level is null and skill_id_original like \"" + skills[i][0] + " %\"";
			System.out.print(stmt.executeUpdate(query) + " / ");
			query = "update niri.job_skills set skill_level='" + skills[i][1] + "' where skill_level is null and skill_id_original like \"" + skills[i][0] + " %\"";
			System.out.print(stmt.executeUpdate(query));
		}
		stmt.close();
		conn.close();
	}

	/* Cleans pre-stopwords within the harvested skills (e.g. good, excellent, fluent, etc.)
	 */
	public static void cleanSkillPreStopwords(String[] preStopwords) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		for (int i=0; i<preStopwords.length; i++) {
			System.out.print("\nRemoving pre-stopwords: '" + preStopwords[i] + "' ... ");
			query = "update niri.job_skills set skill_id=right(skill_id,length(skill_id) - " + (preStopwords[i].length()+1) + ") where skill_id like \"" + preStopwords[i] + " %\"";
			System.out.print(stmt.executeUpdate(query));
		}
		stmt.close();
		conn.close();
	}

	/* Cleans post-stopwords within the harvested skills (e.g. ' skills, etc.)
	 */
	public static void cleanSkillPostStopwords(String[] postStopwords) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		for (int i=0; i<postStopwords.length; i++) {
			System.out.print("\nRemoving post-stopwords: '" + postStopwords[i] + "' ... ");
			query = "update niri.job_skills set skill_id=left(skill_id,length(skill_id) - " + (postStopwords[i].length()+1) + ") where skill_id like \"% " + postStopwords[i] + "\"";
			System.out.print(stmt.executeUpdate(query));
		}
		stmt.close();
		conn.close();
	}

	/* Resolves Skill synonyms by replacing the common skills with the most adopted skill in the taxonomy
	 * 
	 */
	public static void resolveSkillSynonyms(String[][] synonyms) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();

		for (int i=0; i<synonyms.length; i++) {
			System.out.print("\nResolving '" + synonyms[i][0] + "' TO '" + synonyms[i][1] + "' ... ");
				query = "update niri.job_skills set skill_id=\"" + synonyms[i][1] + "\" where skill_id=\"" + synonyms[i][0] + "\"";
				System.out.print(stmt.executeUpdate(query) + " records updated");

		}
		stmt.close();
		conn.close();
	}

	/* Splits composed skills (e.g. Arabic and English, etc.)
	 */
	public static void splitSkills(String[] toSplit, String spliter) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		ResultSet rs=null;
		
		System.out.println("\n *********** Spliting composed skills ************");
		
		for (int i=0; i<toSplit.length; i++) {
			query = "select * from niri.job_skills where skill_id=\"" + toSplit[i] + "\"";
			//System.out.println(query);

			rs = stmt.executeQuery(query);
			while (rs.next()) {
				query = "update niri.job_skills set skill_id='" + toSplit[i].substring(0, toSplit[i].indexOf(spliter)) 
					+ "' where job_id='" +  rs.getString("job_id") + "' and skill_id='" +  toSplit[i] + "'";
				System.out.print(stmt2.executeUpdate(query)+ "/");
				query = "insert into niri.job_skills values ('" +  rs.getString("job_id") + "',\"" +  rs.getString("skill_id_original") + "\",'"
					+ toSplit[i].substring(toSplit[i].indexOf(spliter)+spliter.length(),toSplit[i].length()).trim()  + "','" +  rs.getString("skill_level") + "')";
				System.out.print(stmt2.executeUpdate(query)+ " ");
			}
		}
		rs.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/* Cleans skills (stopwords inside the skill_id)
	 */
	public static void cleanSkills(String[] toClean) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="", skill="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		ResultSet rs=null;
		int idx=0;
		
		System.out.println("\n *********** Cleaning inside stopwords ************");

		// re-initialize the skills table
		query = "drop table niri.job_skills";
		stmt.executeUpdate(query);
		query = "create table niri.job_skills (SELECT * FROM niri.job_skills_copy)";
		stmt.executeUpdate(query);

		// make a copy of the original skill_id
		query = "update niri.job_skills set skill_id=skill_id_original where skill_level is null";
		System.out.println(stmt.executeUpdate(query) + " original records preserved.");
		
		// Initialize skill_level to null
		query = "update niri.job_skills set skill_level=null";
		System.out.println(stmt.executeUpdate(query) + " skill levels reset to null.");
		
		
		for (int i=0; i<toClean.length; i++) {
			query = "select * from niri.job_skills where skill_id like\"%" + toClean[i] + "%\"";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			while (rs.next() && rs.getString("skill_id").length()>3) {
				skill = rs.getString("skill_id");
				idx = skill.indexOf(toClean[i]);
				skill = skill.substring(0, idx) + skill.substring(idx+toClean[i].length(), skill.length());
				skill = skill.replaceAll("  ", " ").trim();
				
				query = "update niri.job_skills set skill_id='" + skill 
					+ "' where job_id='" +  rs.getString("job_id") + "' and skill_id='" +  rs.getString("skill_id") + "'";
				System.out.print(stmt2.executeUpdate(query)+ ".");
			}
		}
		rs.close();
		stmt.close();
		stmt2.close();
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
