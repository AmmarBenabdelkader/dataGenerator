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
import java.io.*;
import java.sql.*;

public class Niri_SkillsClassifier 
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
			{"ability to","Good"},
		};
		String[] preStopwords = {
			"command in","command of","command","background in","background",
			"knowledge in","knowledge of","Knowledge",
			"level in","level of","level","skills in","verbal and written","written and spoken","oral and written","written and oral",
			"written and verbal","written and oral", "ability to"};
		String[] postStopwords = {
			"skills","writing and reading","speaking", "skill", "is a must", "is required", "language", "languages",
			"(written and oral)", "is a plus", "is essential", "is highly", "is an advantage", "is desirable",
			"would be advantageous"};
		String[][] skillSynonyms = {
			{"Computer literate","Computer literacy"},
			{"Computer","Computer literacy"},
			{"PC literate","Computer literacy"},
			{"PC","Computer literacy"},
			//{"MS Office","Microsoft Office"},
			{"Microsoft office programs","MS Office"},
			{"Microsoft office skills","MS Office"},
			{"Microsoft office software","MS Office"},
			{"Microsoft office suite","MS Office"},
			{"MS applications","MS Office"},
			//{"MS Excel","Microsoft Excel"},
			//{"MS project","Microsoft Project"},
			{"MS SQL","Microsoft SQL Server"},
			{"MS Windows","Windows"},
			{"MS Visual Studio 2010","Visual Studio"},
			{"office software","MS Office"},
			{"office","MS Office"},
			{"MIS","MIS - Management Information Systems"},
			{"Arabic speaker","Arabic"},
			{"communication skills, both verbal and written","communication"},
			{"communication (written and oral)","communication"},
			{"communications","communication"},
			{"Team Leadership","leadership"},
			{"Bilingual (Arabic/English)","Arabic and English"},
			{"Auto CAD","AUTOCAD"},
			{"Administration","HR Administration"},
			//{"Organizing","Organizational"},
		};
		readProperties();
		assignSkillLevel(skillLevels);
		resolveSkillSynonyms(skillSynonyms);
		cleanSkillPreStopwords(preStopwords);
		cleanSkillPostStopwords(postStopwords);
		validateSkills_asoc();
		validateSkills_esco();
		//resolveSkillSynonyms(skillSynonyms);
		validateSkills_bis();
		//explainSkillsValidation1_bis("135405");
		//explainSkillsValidation2_bis("412001 - 03");
		//splitSkills_bis("#_");
		//splitSkills_bis("$_");
		//skillsTreeBuilder();
		
		//exploreJobSkills("135931"); //135405 //	190533//190651//190653//
/*		218308
		190415
		208611
		190655
		203104
		208596
		224654
*/		
		//exploreCommonJobSkills("412001 - 03");
/*241101 - 03
412001 - 01
332207 - 03
121901 - 02
122108 - 02
122108 - 01
214201 - 03
221201 - 01
231006 - 03
132106 - 01
412001 - 03
122106 - 02
216603 - 03
*/	}
	public static void exploreJobSkills(String job) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		System.out.print("JOB - ");
		query = "SELECT distinct job_id, job_title, job_code FROM niri.job where job_id='" + job + "' order by prediction_code";
		rs = stmt.executeQuery(query);
		if (rs.next())
		{
			System.out.println(rs.getString(1) + ": " + rs.getString(2) + (rs.getString(3)!=null?" (" + rs.getString(3) + ")":"") + "\n  ***** ORIGINAL Skills ****");

			query = "SELECT distinct skill_id_original FROM niri.job_skills where job_id='" + job + "'";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t  - " + rs2.getString(1));
			}
			
			System.out.println("\n  **** PROCESSED Skills ****");

			query = "SELECT distinct skill_id FROM niri.job_skills_extended where job_id='" + job + "'";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t  - " + rs2.getString(1));
			}
			rs2.close();
		}
		else 
			System.out.println("\t -> No job with id '" + job + "' have been found");
			
		rs.close();

		stmt.close();
		stmt2.close();
		conn.close();
	}

	public static void exploreCommonJobSkills(String job) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		System.out.print("\nJOB - ");
		query = "SELECT distinct job_code, job_title, count(job_id) FROM niri.job where job_code='" + job + "' group by job_code order by prediction_code";
		//System.out.println(query);
		rs = stmt.executeQuery(query);
		if (rs.next())
		{
			System.out.println(rs.getString(1) + ": " + rs.getString(2) + " (" + rs.getString(3) + ")\n  ***** ORIGINAL Skills ****");

			query = "SELECT distinct skill_id_original, count(job_id) Counts FROM niri.job_skills where job_id in (select job_id from niri.job where job_code='" + job + "') group by skill_id_original order by Counts desc";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t  - " + rs2.getString(1) + " (" + rs2.getString(2) + ")");
			}
			
			System.out.println("\n  **** PROCESSED Skills ****");

			query = "SELECT distinct skill_id, count(job_id) Counts FROM niri.job_skills_extended where job_id in (select job_id from niri.job where job_code='" + job + "') group by skill_id order by Counts desc";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t  - " + rs2.getString(1) + " (" + rs2.getString(2) + ")");
			}
			rs2.close();
		}
		else 
			System.out.println("\t -> No Occupation with id '" + job + "' have been found");
		rs.close();
		stmt.close();
		stmt2.close();
		conn.close();
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
		
		// make a copy of the original skill_id
		query = "update niri.job_skills_classified set skill_id=skill_id_original";
		System.out.println(stmt.executeUpdate(query) + " original records preserved.");
		
		// Initialize skill_level to null
		query = "update niri.job_skills_classified set skill_level=null";
		System.out.println(stmt.executeUpdate(query) + " skill levels reset to null.");
		
		
		for (int i=0; i<skills.length; i++) {
			System.out.print("\nAssigning skills level: '" + skills[i][0] + "' ... ");
			query = "update niri.job_skills_classified set skill_id=right(skill_id_original,length(skill_id_original) - " + (skills[i][0].length()+1) + ") where skill_level is null and skill_id_original like \"" + skills[i][0] + " %\"";
			System.out.print(stmt.executeUpdate(query) + " / ");
			query = "update niri.job_skills_classified set skill_level='" + skills[i][1] + "' where skill_level is null and skill_id_original like \"" + skills[i][0] + " %\"";
			System.out.print(stmt.executeUpdate(query));
		}
		stmt.close();
		conn.close();
	}

	public static void validateSkills_asoc() throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		List<String> words = new ArrayList<String>();
		
		String INPUT = "";
		String tmp = "";

		query = "update niri.job_skills_classified set skill_id_asoc=skill_id";
		stmt2.executeUpdate(query);
		
		query = "update niri.job_skills_classified set validated_skills_asoc=null";
		stmt2.executeUpdate(query);
		
		
		query = "SELECT distinct skill_title_en FROM asoc.skills order by length(skill_title_en) desc";
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next())
		{
				words.add(rs.getString("skill_title_en").trim());
		}

		query = "SELECT distinct skill_id_asoc FROM niri.job_skills_classified";
		rs = stmt.executeQuery(query);
		String skill = "", skills = "";
		while (rs.next()) // && j < 5)
		{
			INPUT = rs.getString("skill_id_asoc").trim();
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
			for (int i = 0; i < words.size(); i++)
			{
				Pattern p = Pattern.compile("\\b" + words.get(i) + "\\b", Pattern.CASE_INSENSITIVE);
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						skills += skill + "#_";
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						}
			        }     
			}
			if (skills.length()>1) {
				System.out.print(INPUT + "\n\t- " + tmp + "\tSkills:" + skills);
				query = "update niri.job_skills_classified set validated_skills_asoc=\"" + skills 
				+ "\" where skill_id_asoc=\"" +  rs.getString("skill_id_asoc") + "\"";
				System.out.print(stmt2.executeUpdate(query)+ "/");
				query = "update niri.job_skills_classified set skill_id_asoc=\"" + tmp 
				+ "\" where skill_id_asoc=\"" +  rs.getString("skill_id_asoc") + "\"";
				System.out.println(stmt2.executeUpdate(query));
						
			}


		}
		stmt.close();
		stmt2.close();
		conn.close();
	}

	public static void validateSkills_esco() throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		List<String> words = new ArrayList<String>();
		
		String INPUT = "";
		String tmp = "";

		query = "update niri.job_skills_classified set skill_id_esco=skill_id";
		System.out.print(stmt2.executeUpdate(query)+ "/");
		
		query = "update niri.job_skills_classified set validated_skills_esco=null";
		System.out.println(stmt2.executeUpdate(query));
		
		query = "SELECT distinct conceptpt FROM escoskos.skills where length(conceptpt)>1 order by length(conceptpt) desc";
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next())
		{
				words.add(rs.getString("conceptpt").trim());
		}

/*		for (int i = 0; i < words.size(); i++)
            System.out.println( i + "- '" + words.get(i) + "'");
*/
		query = "SELECT distinct skill_id_esco FROM niri.job_skills_classified";
		rs = stmt.executeQuery(query);
		String skill = "", skills = "";
		while (rs.next()) // && j < 5)
		{
			INPUT = rs.getString("skill_id_esco").trim();
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
			for (int i = 0; i < words.size(); i++)
			{
				Pattern p = Pattern.compile("\\b" + words.get(i) + "\\b", Pattern.CASE_INSENSITIVE);
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
/*			            System.out.println(INPUT + "\n\t- " + words.get(i) + " -- > found: " + count + " : "
			                    + matcher.start() + " - " + matcher.end());
*/			            
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						skills += skill + "#_";
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						}
			        }     
			}
			if (skills.length()>1) {
				System.out.print(INPUT + "\n\t- " + skills);
				query = "update niri.job_skills_classified set validated_skills_esco=\"" + skills 
				+ "\" where skill_id_esco=\"" +  rs.getString("skill_id_esco") + "\"";
				System.out.print(stmt2.executeUpdate(query)+ "/");
				query = "update niri.job_skills_classified set skill_id_esco=\"" + tmp 
				+ "\" where skill_id_esco=\"" +  rs.getString("skill_id_esco") + "\"";
				System.out.println(stmt2.executeUpdate(query));
						
			}


		}
		stmt.close();
		stmt2.close();
		conn.close();
	}

	public static void validateSkills_bis() throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		List<String> words = new ArrayList<String>();
		
		String INPUT = "";
		String tmp = "";

		query = "update niri.job_skills_classified set skill_id_bis=skill_id";
		System.out.print(stmt2.executeUpdate(query)+ "/");
		
		query = "update niri.job_skills_classified set validated_skills_bis=null";
		System.out.println(stmt2.executeUpdate(query));
		
		//query = "SELECT distinct skill_name FROM niri.skills_uni where length(skill_name)>0 and skill_name like '%communication%' order by length(skill_name) desc";
		query = "SELECT distinct skill_name FROM niri.skills_uni where length(skill_name)>0 order by length(skill_name) desc";
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next())
		{
				words.add(rs.getString("skill_name").trim());
		}

		query = "SELECT distinct skill_id_bis FROM niri.job_skills_classified";
		rs = stmt.executeQuery(query);
		String skill = "", skills = "";
		while (rs.next()) // && j < 5)
		{
			INPUT = rs.getString("skill_id_bis").trim();
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
			for (int i = 0; i < words.size(); i++)
			{
				Pattern p = Pattern.compile("\\b" + words.get(i) + "\\b", Pattern.CASE_INSENSITIVE);
				if (words.get(i).length()<5)
					p = Pattern.compile("\\b" + words.get(i) + "\\b");
				
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						skills += skill + "#_";
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						}
			        }     
			}
			if (skills.length()>1) {
				System.out.print(INPUT + "\n\t- " + skills);
				query = "update niri.job_skills_classified set validated_skills_bis=\"" + skills 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\"";
				System.out.print(stmt2.executeUpdate(query)+ "/");
				query = "update niri.job_skills_classified set skill_id_bis=\"" + tmp 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\"";
				System.out.println(stmt2.executeUpdate(query));
						
			}


		}
		
		/*
		 * PART 2: based on the skills
		 */
		query = "SELECT distinct skill_name, synonym_name FROM niri.skills_uni a, niri.skill_synonyms b where a.skill_id=b.skill_id order by length(synonym_name) desc";
		rs = stmt.executeQuery(query);
		Map<String, String> synonyms = new HashMap<String, String>();
		for (int i=0; rs.next(); i++) {
			synonyms.put(rs.getString("synonym_name").trim(), rs.getString("skill_name").trim());			
		}
		query = "SELECT distinct skill_id_bis FROM niri.job_skills_classified";
		rs = stmt.executeQuery(query);
		skill = ""; skills = "";
		while (rs.next()) // && j < 5)
		{
			INPUT = rs.getString("skill_id_bis").trim();
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
	        for(String key: synonyms.keySet())
			{
				Pattern p = Pattern.compile("\\b" + key + "\\b", Pattern.CASE_INSENSITIVE);
				if (key.length()<5)
					p = Pattern.compile("\\b" + key + "\\b");
				
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						skills += synonyms.get(key) + "$_";
						}
			        }     
			}
			if (skills.length()>1) {
				System.out.print(INPUT + "\n\t- " + skills);
				query = "update niri.job_skills_classified set validated_skills_bis=\"" + skills 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\"";
				System.out.print(stmt2.executeUpdate(query)+ "/");
				query = "update niri.job_skills_classified set skill_id_bis=\"" + tmp 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\"";
				System.out.println(stmt2.executeUpdate(query));
						
			}


		}
		stmt.close();
		stmt2.close();
		conn.close();
	}
	public static void explainSkillsValidation1_bis(String job) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		List<String> words = new ArrayList<String>();
		
		String INPUT = "";
		String tmp = "";

		query = "update niri.job_skills_classified set skill_id_bis=skill_id where job_id='" + job + "'";
		System.out.print(stmt2.executeUpdate(query)+ " out of ");
		
		query = "update niri.job_skills_classified set validated_skills_bis=null where job_id='" + job + "'";
		System.out.println(stmt2.executeUpdate(query) + " Skills for job: " + job);
		
		System.out.println("A- Based on SKILLS");

		query = "SELECT distinct skill_name FROM niri.skills_uni where length(skill_name)>0 order by length(skill_name) desc";
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next())
		{
				words.add(rs.getString("skill_name").trim());
		}

		query = "SELECT distinct skill_id_bis FROM niri.job_skills_classified where job_id='" + job + "'";
		rs = stmt.executeQuery(query);
		String skill = "", skills = "";
		while (rs.next()) // && j < 5)
		{
			INPUT = rs.getString("skill_id_bis").trim();
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
			for (int i = 0; i < words.size(); i++)
			{
				Pattern p = Pattern.compile("\\b" + words.get(i) + "\\b", Pattern.CASE_INSENSITIVE);
				if (words.get(i).length()<5)
					p = Pattern.compile("\\b" + words.get(i) + "\\b");
				
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						skills += skill + "#_";
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						}
			        }     
			}
			System.out.println("  - " + INPUT + " ---> " + skills);
			if (skills.length()>1) {
				query = "update niri.job_skills_classified set validated_skills_bis=\"" + skills 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\" and job_id='" + job + "'";
				stmt2.executeUpdate(query);
				query = "update niri.job_skills_classified set skill_id_bis=\"" + tmp 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\"  and job_id='" + job + "'";
				stmt2.executeUpdate(query);
						
			}


		}
		
		/*
		 * PART 2: based on the skills
		 */
		System.out.println("\nB- Based on Skill Synonyms");
		query = "SELECT distinct skill_name, synonym_name FROM niri.skills_uni a, niri.skill_synonyms b where a.skill_id=b.skill_id order by length(synonym_name) desc";
		rs = stmt.executeQuery(query);
		Map<String, String> synonyms = new HashMap<String, String>();
		for (int i=0; rs.next(); i++) {
			synonyms.put(rs.getString("synonym_name").trim(), rs.getString("skill_name").trim());			
		}
		query = "SELECT distinct skill_id_bis FROM niri.job_skills_classified where job_id='" + job + "'";
		rs = stmt.executeQuery(query);
		skill = ""; skills = "";
		while (rs.next()) // && j < 5)
		{
			INPUT = rs.getString("skill_id_bis").trim();
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
	        for(String key: synonyms.keySet())
			{
				Pattern p = Pattern.compile("\\b" + key + "\\b", Pattern.CASE_INSENSITIVE);
				if (key.length()<5)
					p = Pattern.compile("\\b" + key + "\\b");
				
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						//skills += synonyms.get(key) + "$_";
						skills += synonyms.get(key) + "$_(" + key + ")";
						}
			        }     
			}
			System.out.println("  - " + INPUT + " ---> " + skills);
			if (skills.length()>1) {
				query = "update niri.job_skills_classified set validated_skills_bis=\"" + skills 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\" and job_id='" + job + "'";
				System.out.print(stmt2.executeUpdate(query)+ "/");
				query = "update niri.job_skills_classified set skill_id_bis=\"" + tmp 
				+ "\" where skill_id_bis=\"" +  rs.getString("skill_id_bis") + "\" and job_id='" + job + "'";
				System.out.println(stmt2.executeUpdate(query));
						
			}


		}
		stmt.close();
		stmt2.close();
		conn.close();
	}
	public static void explainSkillsValidation2_bis(String job) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		List<String> words = new ArrayList<String>();
		List<String> inputSkills = new ArrayList<String>();
		
		String INPUT = "";
		String tmp = "";

		System.out.println("Explain skills validation for job: " + job);
		
		query = "select distinct skill_id from niri.job_skills_classified where job_id in (SELECT job_id FROM niri.job where job_code='" + job + "')";
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next())
		{
			inputSkills.add(rs.getString("skill_id"));
		}

		System.out.println("A- Based on SKILLS");
		query = "SELECT distinct skill_name FROM niri.skills_uni where length(skill_name)>0 order by length(skill_name) desc";
		rs = stmt.executeQuery(query);
		while (rs.next())
		{
				words.add(rs.getString("skill_name").trim());
		}

		query = "SELECT distinct skill_id_bis FROM niri.job_skills_classified where job_id in (SELECT job_id FROM niri.job where job_code='" + job + "')";
		rs = stmt.executeQuery(query);
		String skill = "", skills = "";
		for (int s=0; s<inputSkills.size(); s++)
		{
			INPUT = inputSkills.get(s);
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
			for (int i = 0; i < words.size(); i++)
			{
				Pattern p = Pattern.compile("\\b" + words.get(i) + "\\b", Pattern.CASE_INSENSITIVE);
				if (words.get(i).length()<5)
					p = Pattern.compile("\\b" + words.get(i) + "\\b");
				
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						skills += skill + "#_";
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						}
			        }     
			}
			System.out.println("  - " + INPUT + " ---> " + skills);
			if (skills.length()>1) {
				inputSkills.set(s, tmp);						
			}


		}
		
		/*
		 * PART 2: based on the skill synonyms
		 */
		System.out.println("\nB- Based on Skill Synonyms");
		query = "SELECT distinct skill_name, synonym_name FROM niri.skills_uni a, niri.skill_synonyms b where a.skill_id=b.skill_id order by length(synonym_name) desc";
		rs = stmt.executeQuery(query);
		Map<String, String> synonyms = new HashMap<String, String>();
		for (; rs.next(); ) {
			synonyms.put(rs.getString("synonym_name").trim(), rs.getString("skill_name").trim());			
		}
		skill = ""; skills = "";
		for (int s=0; s<inputSkills.size(); s++)
		{
			INPUT = inputSkills.get(s);
			skills = "";
			tmp = INPUT.replaceAll("  ", " ");
	        for(String key: synonyms.keySet())
			{
				Pattern p = Pattern.compile("\\b" + key + "\\b", Pattern.CASE_INSENSITIVE);
				if (key.length()<5)
					p = Pattern.compile("\\b" + key + "\\b");
				
				Matcher matcher = p.matcher(INPUT);

			        while(matcher.find()) {
						skill = INPUT.substring(matcher.start(), matcher.end());
						if (tmp.contains(skill)) {
						tmp = tmp.substring(0, tmp.indexOf(skill)) + tmp.substring(tmp.indexOf(skill)+skill.length(), tmp.length()) ;
						//skills += synonyms.get(key) + "$_";
						skills += synonyms.get(key) + "$_(" + key + ")";
						}
			        }     
			}
	        if (INPUT.length()>1)
	        	System.out.println("  - " + INPUT + " ---> " + skills);

		}
		stmt.close();
		stmt2.close();
		conn.close();
	}
	public static void splitSkills_bis(String sep) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		Statement stmt2 = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		
		String skill = "";

		query = "delete from niri.job_skills_extended";
		//System.out.print(stmt2.executeUpdate(query)+ "/");
		
		query = "SELECT * FROM niri.job_skills_classified where validated_skills_bis like '%" + sep +"%'";
		ResultSet rs = stmt.executeQuery(query);
		skill = ""; 
		while (rs.next()) 
		{
			System.out.println(rs.getString("validated_skills_bis"));
			StringTokenizer st = new StringTokenizer(rs.getString("validated_skills_bis"), sep);
			while (st.hasMoreTokens()) {
				skill = st.nextToken();
				System.out.println("\t- " + skill);
				query = "insert into niri.job_skills_extended values ('" + rs.getString("job_id") + "', '"
					+  rs.getString("skill_level") + "', \"" +  rs.getString("skill_id_original") + "\", \""
					+  rs.getString("skill_id_bis") + "\", \"" +  skill + "\", \""
					+  rs.getString("validated_skills_bis") + "\")";
				System.out.print(stmt2.executeUpdate(query)+ "/");
						
			}


		}
		stmt.close();
		stmt2.close();
		conn.close();
	}

	public static void cleanSkillPreStopwords(String[] preStopwords) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		for (int i=0; i<preStopwords.length; i++) {
			System.out.print("\nRemoving pre-stopwords: '" + preStopwords[i] + "' ... ");
			query = "update niri.job_skills_classified set skill_id=right(skill_id,length(skill_id) - " + (preStopwords[i].length()+1) + ") where skill_id like \"" + preStopwords[i] + " %\"";
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
			query = "update niri.job_skills_classified set skill_id=left(skill_id,length(skill_id) - " + (postStopwords[i].length()+1) + ") where skill_id like \"% " + postStopwords[i] + "\"";
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
				query = "update niri.job_skills_classified set skill_id=\"" + synonyms[i][1] + "\" where skill_id=\"" + synonyms[i][0] + "\"";
				System.out.print(stmt.executeUpdate(query) + " records updated");

		}
		stmt.close();
		conn.close();
	}

	/* Resolves Skill synonyms by replacing the common skills with the most adopted skill in the taxonomy
	 * 
	 */
	public static void resolveSkillSynonyms_bis(String[][] synonyms) throws ClassNotFoundException, SQLException
	{
		Statement stmt = null;
		String query ="";
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();

		//Map<String, String> words = new HashMap<String, String>();
		String[][] words = null;
		
		query = "SELECT distinct skill_name, synonym_name FROM niri.skills_uni a, niri.skill_synonyms b where a.skill_id=b.skill_id order by length(synonym_name) desc";
		ResultSet rs = stmt.executeQuery(query);
		for (int i=0; i<words.length; i++) {
			words[i][0] =rs.getString("synonym_name").trim();
			words[i][1] =rs.getString("skill_name").trim();
			
		}

		for (int i=0; i<words.length; i++) {
			System.out.print("\nResolving '" + words[i][0] + "' TO '" + words[i][1] + "' ... ");
			query = "update niri.job_skills_classified set skill_id=right(skill_id,length(skill_id) - " + (words[i][0].length()+1) + ") where skill_id like \"" + words[i][0] + " %\"";
			System.out.print(stmt.executeUpdate(query));
		}
		stmt.close();
		conn.close();
	}
/*
 * This method builds a tree out of the skills, it uses an input SQL table in a form of parent-child structure.
 * This example is based on an adjusted version of BIS Skills 
 */

	public static void skillsTreeBuilder() throws ClassNotFoundException, SQLException, FileNotFoundException, UnsupportedEncodingException
	{
		Connection conn = null;
		Statement stmt = null, stmt1 = null;
		ResultSet rs = null, rs1 =  null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt1 = conn.createStatement();
		String query;
		int l = 1;
		PrintWriter writer = new PrintWriter("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\AMS\\SkillsTree\\SkillsTree_gr.html","UTF-8");
		query = "SELECT distinct skill_id FROM niri.skills_uni where skill_parent is null";

		//System.out.println(query);
		rs1 = stmt1.executeQuery(query);
		System.out.println("<ul>");
		String parent = "", child1 = "", child2 = "", child3 = "", buffer="";
		while (rs1.next()) {

			query = "SELECT aa.skill_id Id, aa.skill_name_gr Parent, bb.skill_id Id1, bb.skill_name_gr Child1, cc.skill_id Id2, cc.skill_name_gr Child2, dd.skill_id Id3, dd.skill_name_gr Child3"
				+ " FROM niri.skills_uni aa"
				+ " LEFT OUTER JOIN niri.skills_uni bb"
				+ " ON aa.skill_id=bb.skill_parent"
				+ " LEFT OUTER JOIN niri.skills_uni cc"
				+ " ON bb.skill_id=cc.skill_parent"
				+ " LEFT OUTER JOIN niri.skills_uni dd"
				+ " ON cc.skill_id=dd.skill_parent where aa.skill_id ='" + rs1.getString("skill_id") + "'"
				+ " order by Parent, Child1, Child2, Child3";

			//System.out.println(query);
			rs = stmt.executeQuery(query);
			parent = ""; child1 = ""; child2 = ""; child3 = ""; buffer="";
			buffer = "";

			for (; rs.next();)
			{
				if (!parent.equalsIgnoreCase(rs.getString("Parent"))){
					if (l==0)
						buffer += "</li>\n";
					buffer += "  <li id=" + rs.getString("Id") + ">" + rs.getString("Parent"); // + " 0->" + l);
					l=0;
				}
				parent = rs.getString("Parent");
				if (rs.getString("Child1") != null)
				{
					if (!child1.equalsIgnoreCase(rs.getString("Child1"))) {
						if (l==0)
							buffer += "\n\t<ul>\n";
						if (l==1)
							buffer += "</li>\n";
						if (l==2)
							buffer += "\n\t  </ul>\n";
						if (l==3)
							buffer += "\t\t</ul>\n\t  </ul>\n";
						buffer += "\t  <li id=" + rs.getString("id1") + ">" + rs.getString("Child1");// + " 1->" + l);
						l = 1;
						child1 = rs.getString("Child1");
					}				

					if (rs.getString("Child2") != null)
					{
						if (!child2.equalsIgnoreCase(rs.getString("Child2"))) {
							if (l==1)
								buffer += "\n\t  <ul>";
							if (l==3)
								buffer += "\t\t</ul>";
							buffer += "\n\t    <li id=" + rs.getString("id2") + ">" + rs.getString("Child2");// + " 2->" + l);
							l = 2;						
							child2 = rs.getString("Child2");
						}
						if (rs.getString("Child3") != null){
							if (!child3.equalsIgnoreCase(rs.getString("Child3"))) {
								if (l==2)
									buffer += "\n\t\t<ul>\n";
								buffer += "\t\t  <li id=" + rs.getString("id3") + ">" + rs.getString("Child3");//  + " 3->" + l);
								if (l<=3)
									buffer += "</li>\n";
								l=3;
								child3 = rs.getString("Child3");
							}
						}
					}
				}

			}
			if (l==3)
				buffer += "\n\t\t</ul>\n\t  </ul>\n\t</ul>\n";
			if (l==2)
				buffer += "\n\t  </ul>\n\t</ul>\n";
			if (l==1)
				buffer += "\n\t</ul>\n";

			System.out.print(buffer);
			writer.print(buffer);
		}
		System.out.println("<ul>");

		rs1.close();
		rs.close();
		stmt.close();
		stmt1.close();
		conn.close();
		writer.close();
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
