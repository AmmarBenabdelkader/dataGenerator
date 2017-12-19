
/**
 * @author abenabdelkader
 *
 * dataGenerator_ams.java
 * September 7, 2016
 */
package com.wccgroup.elise.testdata;

import java.io.*;
//STEP 1. Import required packages
import java.sql.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/*
 * TEST Data generator for jobs and jobseekers based on ASOC MOL list of occupations
 * data is generated in mixed mode in Arabic and English and the data distribution 
 * is based on statistics that are specific to HRDF project
 */
public class dataGenerator_ams
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Map<String, String> jobsDic = new HashMap<String, String>();
	static Map<Integer, String> zipcodes = new HashMap<Integer, String>();
	static String[] batchQueries = new String[10];
	String[][] options = {{"0","occupation_ar",""},{"1","occupation",""}};
	int option=0;
	static String jobCode; // two columns to hold jobCode and occupation code
	static String occupCode;
	static String occupSRC;
	static String exclude="('0','9')"; // list of occupations to exclude from the data generation; 0: military, 9: low level
	static int from_=0; // from-to values for specific occupations
	static int to_=100;
	static int maxSpecific = 500;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException
	{

		readProperties(); //load dataGerator properties
		String input = "";
		ArrayList<String> options = new ArrayList<>();
		options.add("1");
		options.add("2");
		options.add("3");

		while (true)
		{
			System.out.printf("Please select the action to perform from the following:\n");
			System.out.println("\t- 1- update data for AMS (age)");
			System.out.println("\t- 2- Statistics");
			System.out.println("\t- 3- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			updateAMSdata(100); // generate test data for HRDF based on ASOC MOL
			break;
		case "2":
			fullStatistics(); // perform statistics on the data
			break;
		case "3":
		default:
			System.out.println("Bye"); // quit

		}

	}

	/*
	 * TEST Data generator for jobs and jobseekers based on BIS list of occupations
	 * the data distribution is based on statistics that are specific to AMS project
	 */
	public static void updateAMSdata(int counts) throws SQLException, ClassNotFoundException
	{
		Date date = new Date();

		String[][] languages = {{"263180","89", "Englisch"},{"258830", "18", "Französisch"},{"258831", "10", "Italienisch"},{"272654","8", "Serbisch"},{"258840","7", "Türkisch"},{"258838","6", "Spanisch"},{"261172","6", "Kroatisch"},{"258835","4", "Russisch"},{"262880","3", "Bosnisch"}};
		//enrichLanguages(12,languages);
		String[][] mobility = {{"BUS","20", "Bus"},{"RKW", "35", "Car"},{"Zug", "45", "Train"}};
		//enrichJSMobility(15,mobility);
		
		String[][] education = {{"SECSCHOOL","49", "SECSCHOOL"},{"APPRENTICE","27", "APPRENTICE"},{"COMPSCHOOL","11", "COMPSCHOOL"},{"COLBACUNIV", "6", "COLBACUNIV"},{"MASTERCERT", "1", "master"}};
		enrichJSEducation(100,education);
		
		
/*
		generateAddresses_sa("Home");
		generateAddresses_sa("Work");

		generateJobEducation();
		generateJobLanguages();
		
		//generateEducation();
		//GenCandidateSkills();
*/
		System.out.println("\n\tStarting at: " + date);
		System.out.println("\tFinished at: " + new Date());
	}

	public static void enrichJSEducation(int ratioage, String[][] distribution) throws SQLException
	{
		Date date = new Date();
		String query = "";
		int total=0;
		String education_code="";
		String education_id="";

		for (int i=0; i<distribution.length; i++)
			total = total + Integer.parseInt(distribution[i][1]);

		Map<String, String> education_list1 = loadItems("SELECT distinct cast(RECORDID as varchar(200)) FROM LEHRBERUF where deleted=0 AND webstatusext_noteid IN(2, 4)");
		Map<String, String> education_list2 = loadItems("select distinct trim(code) from CODETABLEITEM where trim(TABLENAME) in ('CEMSEducationUniversity' , 'CEMSEducationType' ,  'CEMSEducationMiddleSchool' ,  'CEMSEducationSecSchool') and ISENABLED=1");
		int index_1 = education_list1.size();
		int index_2 = education_list2.size();
		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = null;
			ResultSet rs2 = null;

			String val = "";
			String profile_id="";
			long i=1;
			long profiles=0;
			long jobseekers=0;
			int j=0;
			rs = stmt.executeQuery("select max(PROFILEWORKLOCATIONID)+1 as recordid from CEMSPROFILEWORKLOC");
			if (rs.next())
				i = rs.getLong("recordid");
			if (i==0)
				i++;
			long start = i;
			
			rs = stmt.executeQuery(
					"SELECT count(distinct a.PROFILEID) profiles, count(distinct a.CONCERNROLEID) jobseekers "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			if (rs.next()) 
			{
				profiles = rs.getLong("profiles");
				jobseekers = rs.getLong("jobseekers");
			}


			System.out.println("\n *** updating jobseeker education  ***  ");

			rs = stmt.executeQuery(
					"SELECT Distinct a.CONCERNROLEID as js_id "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			Random rn = new Random();
			while (rs.next())
			{
				
				query = "insert into CEMSEDUCATION (EDUCATIONID, RELATEDID,  EDUCATIONTAXONOMYID, EDUCATIONTYPE, EDUCATIONCODEVALUE, RELATEDTYPE, APPROVEDFORPUBLICATION, RECORDSTATUS, VERSIONNO, LASTWRITTEN) values ("
						+ "#i#,#profile_id#, #education_id#, '#education_type#', '#education_code#', 'JSP', 'YES', 'RST1', 1, CURRENT TIMESTAMP)" ;
				
					rs2 = stmt2.executeQuery(
						"SELECT Distinct a.PROFILEID as profile_id "
								+ "FROM CEMSPROFILE a "
								+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
								+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
								+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
	                            + "AND a.PROFILETYPE = 'JOBSEEKER' "
	 							+ "AND a.CONCERNROLEID=" + rs.getString("js_id")
						); 
					System.out.println((j++) + "- jobseeker_id: " + rs.getString("js_id")); 
					val = pickValue(distribution, rn.nextInt(total) + 1);
					System.out.println((j++) + "\t- education: '" + val + "'"); 
					if (val.equalsIgnoreCase("APPRENTICE") || val.equalsIgnoreCase("MASTERCERT")) {
						education_id = education_list1.get(String.valueOf(rn.nextInt(index_1)+1));
						education_code="";
					}
					else {
						education_code = education_list2.get(String.valueOf(rn.nextInt(index_2)+1));
						education_id=null;
					}

					
					while (rs2.next())
					{
								profile_id = rs2.getString("profile_id");
								query=query.replace("#i#", String.valueOf(i)).replace("#profile_id#",profile_id);
								query =query.replace("#education_type#", val);
								query =query.replace("#education_id#", education_id);
								query =query.replace("#education_code#", education_code);
								
								//query = "insert into CEMSEDUCATION (EDUCATIONID, RELATEDID,  EDUCATIONTYPE, RELATEDTYPE, APPROVEDFORPUBLICATION, RECORDSTATUS, VERSIONNO, LASTWRITTEN) values ("
								//	+ i + "," + profile_id + ", '" +  val + "', 'JSP', 'YES', 'RST1', 1, CURRENT TIMESTAMP)" ;
								System.out.println(query); // + "\neducation_id" + education_id + "\neducation_code" + education_code);
								//stmt3.executeUpdate(query);
								i++;
					}
			}
			i--;
			rs2.close();
			stmt2.close();
			conn.close();
			String report = "\n\t****  Data generation report  **** \ndatabase: " + DB_URL
					+ "\nTiming:\n\tStart at: " + date + "\n\tEnd at:  " + new Date() + "\n"
					+ "Porperty: Jobseeker languages\nGenerated records: " + (i-start+1) 
					+ "\n\tfrom: " + start + "\n\tto: " + i + ")\n"
					+ "Out of:\n\t" + profiles + " profiles\n\t" + jobseekers + " jobseekers"
					+ "\nRatio: " + ratioage + " (actual: " + (i-start+1)*100/profiles + "% )\n" 
					+ "To delete the generated data, you need to run the query:\n\tdelete from CEMSEDUCATION where EDUCATIONID>=" + start + " and EDUCATIONID<= " + i ;
;
			System.out.println(report);
			try(FileWriter fw = new FileWriter("c:\\AMS\\workspace\\dataGeneration_AMSreport.txt", true);
				    BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw))
				{
				    out.println(report);
				    out.close();
				    bw.close();
				    fw.close();
				} catch (IOException e) {
				    //exception handling left as an exercise for the reader
				}
			
		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
	}//end

	public static void enrichJSMobility(int ratioage, String[][] mobility) throws SQLException
	{
		Date date = new Date();
		String query = "";
		int total=0;

		for (int i=0; i<mobility.length; i++)
			total = total + Integer.parseInt(mobility[i][1]);

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = null;
			ResultSet rs2 = null;

			String val = "";
			String profile_id="";
			long i=1;
			long profiles=0;
			long jobseekers=0;
			long new_ratioage=0;
			int j=0;
			rs = stmt.executeQuery("select max(PROFILEWORKLOCATIONID)+1 as recordid from CEMSPROFILEWORKLOC");
			if (rs.next())
				i = rs.getLong("recordid");
			if (i==0)
				i++;
			long start = i;
			
			rs = stmt.executeQuery(
					"SELECT count(distinct a.PROFILEID) profiles, count(distinct a.CONCERNROLEID) jobseekers "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			if (rs.next()) 
			{
				profiles = rs.getLong("profiles");
				jobseekers = rs.getLong("jobseekers");
				new_ratioage = ratioage * (int) Math.round(rs.getDouble("profiles")/rs.getDouble("jobseekers"));
			}


			System.out.println("\n *** updating candidates  ***  ");

			rs = stmt.executeQuery(
					"SELECT Distinct a.CONCERNROLEID as js_id "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			Random rn = new Random();
			while (rs.next())
			{
				
				if (rn.nextInt(100)>ratioage) 
					;
				else
				{
					rs2 = stmt2.executeQuery(
						"SELECT Distinct a.PROFILEID as profile_id "
								+ "FROM CEMSPROFILE a "
								+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
								+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
								+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
	                            + "AND a.PROFILETYPE = 'JOBSEEKER' "
	 							+ "AND a.CONCERNROLEID=" + rs.getString("js_id")
						); 
					System.out.println((j++) + "- jobseeker_id: " + rs.getString("js_id")); 
					val = pickValue(mobility, rn.nextInt(total) + 1);
					while (rs2.next())
					{
								profile_id = rs2.getString("profile_id");
			
								query = "insert into CEMSPROFILEWORKLOC (PROFILEWORKLOCATIONID, PROFILEID,  WILLINGNESSTOTRAVEL, MOBILITY, WEIGHTING, RECORDSTATUS, VERSIONNO, LASTWRITTEN) values ("
									+ i + "," + profile_id + ", 'YES', '" +  val + "', 0, 'RST1', 1, CURRENT TIMESTAMP)" ;
								System.out.println(query);
								stmt3.executeUpdate(query);
								i++;
					}
				}
			}
			i--;
			rs2.close();
			stmt2.close();
			conn.close();
			String report = "\n\t****  Data generation report  **** \ndatabase: " + DB_URL
					+ "\nTiming:\n\tStart at: " + date + "\n\tEnd at:  " + new Date() + "\n"
					+ "Porperty: Jobseeker languages\nGenerated records: " + (i-start+1) 
					+ "\n\tfrom: " + start + "\n\tto: " + i + ")\n"
					+ "Out of:\n\t" + profiles + " profiles\n\t" + jobseekers + " jobseekers"
					+ "\nRatio: " + ratioage + " (actual: " + (i-start+1)*100/profiles + "% - adjusted: " + new_ratioage + " )\n" 
					+ "To delete the generated data, you need to run the query:\n\tdelete from CEMSPROFILEWORKLOC where PROFILEWORKLOCATIONID>=" + start + " and PROFILECOMPETENCYID<= " + i ;
;
			System.out.println(report);
			try(FileWriter fw = new FileWriter("c:\\AMS\\workspace\\dataGeneration_AMSreport.txt", true);
				    BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw))
				{
				    out.println(report);
				    out.close();
				    bw.close();
				    fw.close();
				} catch (IOException e) {
				    //exception handling left as an exercise for the reader
				}
			
		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
	}//end

	public static void enrichLanguages(int ratioage, String[][] languages) throws SQLException
	{
		Date date = new Date();
		String query = "";
		int total=0;

		for (int i=0; i<languages.length; i++)
			total = total + Integer.parseInt(languages[i][1]);

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = null;
			ResultSet rs2 = null;

			String lang = "";
			String profile_id="";
			long i=1;
			long profiles=0;
			long jobseekers=0;
			long new_ratioage=0;
			int j=0;
			rs = stmt.executeQuery("select max(PROFILECOMPETENCYID)+1 as recordid from CEMSPROFILECOMPETENCY");
			if (rs.next())
				i = rs.getLong("recordid");
			long start = i;
			
			rs = stmt.executeQuery(
					"SELECT count(distinct a.PROFILEID) profiles, count(distinct a.CONCERNROLEID) jobseekers "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			if (rs.next()) 
			{
				profiles = rs.getLong("profiles");
				jobseekers = rs.getLong("jobseekers");
				new_ratioage = ratioage * (int) Math.round(rs.getDouble("profiles")/rs.getDouble("jobseekers"));
			}


			System.out.println("\n *** updating candidates  ***  ");

			rs = stmt.executeQuery(
					"SELECT Distinct a.CONCERNROLEID as js_id "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			Random rn = new Random();
			while (rs.next())
			{
				
				if (rn.nextInt(100)>ratioage) 
					;
				else
				{
					rs2 = stmt2.executeQuery(
						"SELECT Distinct a.PROFILEID as profile_id "
								+ "FROM CEMSPROFILE a "
								+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
								+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
								+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
	                            + "AND a.PROFILETYPE = 'JOBSEEKER' "
	 							+ "AND a.CONCERNROLEID=" + rs.getString("js_id")
						); 
					System.out.println((j++) + "- jobseeker_id: " + rs.getString("js_id")); 
					lang = pickValue(languages, rn.nextInt(total) + 1);
					while (rs2.next())
					{
								profile_id = rs2.getString("profile_id");
			
								query = "insert into CEMSPROFILECOMPETENCY (PROFILECOMPETENCYID, RELATEDID,  COMPETENCYTAXONOMYID, COMPETENCYTYPE, WEIGHTINGABILITY, WEIGHTINGIMPORTANCE,  RECORDSTATUS, NEGATIVESEARCHPARAM, APPROVEDFORPUBLICATION, RELATEDTYPE, VERSIONNO, LASTWRITTEN) values ("
									+ i + "," + profile_id + "," +  lang + ", 'LANGUAGE', 0, 1, 'RST1', 0, 0, 'JSP', 1, CURRENT TIMESTAMP)" ;
								System.out.println(query);
								//stmt3.executeUpdate(query);
								i++;
					}
				}
			}
			i--;
			rs2.close();
			stmt2.close();
			conn.close();
			String report = "\n\t****  Data generation report  **** \ndatabase: " + DB_URL
					+ "\nTiming:\n\tStart at: " + date + "\n\tEnd at:  " + new Date() + "\n"
					+ "Porperty: Jobseeker languages\nGenerated records: " + (i-start+1) 
					+ "\n\tfrom: " + start + "\n\tto: " + i + ")\n"
					+ "Out of:\n\t" + profiles + " profiles\n\t" + jobseekers + " jobseekers"
					+ "\nRatio: " + ratioage + " (actual: " + (i-start+1)*100/profiles + "% - adjusted: " + new_ratioage + " )\n" 
					+ "To delete the generated data, you need to run the query:\n\tdelete from CEMSPROFILECOMPETENCY where PROFILECOMPETENCYID>=" + start + " and PROFILECOMPETENCYID<= " + i ;
;
			System.out.println(report);
			try(FileWriter fw = new FileWriter("c:\\AMS\\workspace\\dataGeneration_AMSreport.txt", true);
				    BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw))
				{
				    out.println(report);
				    out.close();
				    bw.close();
				    fw.close();
				} catch (IOException e) {
				    //exception handling left as an exercise for the reader
				}
			
		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
	}//end

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

	/* Picks a random value from an array
	 * 
	 */
	public static String pickValue(String[][] values, int myNumber)
	{
		int begin = 0, end = 0;
		for(int c = 0; c < values.length; c++){
			end = begin +  Integer.parseInt(values[c][1]);
			if (myNumber>begin && myNumber<=end)
				return values[c][0];
			begin = end;
		}
		return null;
	}
	


	public static void generateEducation()
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = null, rs2 = null;
			String query;
			int age = 0, niv = 0, cId = 0;
			query = "select candidate_id, birth_date, timestampdiff(year,birth_date, curdate()) age FROM Employment.candidate where candidate_id not in (select distinct candidate_id from Employment.candidate_education)";
			//System.out.println("\t- " + query);
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			while (rs.next())
			{
				age = rs.getInt("age");
				cId = rs.getInt("candidate_id");
				if (age > 30)
				{
					niv = rn.nextInt(10) + 1;
				}
				else if (age > 27)
				{
					niv = rn.nextInt(9) + 1;
				}
				else if (age > 25)
				{
					niv = rn.nextInt(8) + 1;
				}
				else if (age > 22)
				{
					niv = rn.nextInt(3) + 1;
				}
				System.out.println("\tAge " + age + " --> Niv.: " + niv);
				if (niv == 0)
				{
					continue;
				}
				for (int i = niv; i <= 10; i++)
				{
					query = "SELECT * from escoskos.education where education_niveau=" + i + " order by niveau_code";
					//System.out.println("\t- " + query);
					rs2 = stmt2.executeQuery(query);
					if (rs2.next())
					{
						query = "insert into Employment.candidate_education (candidate_id, education_name, education_level) values ("
							+ cId
							+ ", '"
							+ rs2.getString("education_name")
							+ "', '"
							+ rs2.getString("niveau_code")
							+ "')";
						System.out.println("\t\t- " + query);
						stmt3.executeUpdate(query);

					}
					if (i > 3 && i < 9)
					{
						i = 9;
					}

				}
				rs2.close();
			}
			rs.close();

			//STEP 6: Clean-up environment
			stmt.close();
			stmt2.close();
			conn.close();

		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
		finally
		{
			//finally block used to close resources
			try
			{
				if (stmt != null)
				{
					stmt.close();
				}
			}
			catch (SQLException se2)
			{
			} // nothing we can do
			try
			{
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
		} //end try
	}//end

	/*
	 * Generate companies for the job offering, companies names and sectors are based on the 
	 * top 200 companies in KSA, companies locations are randomly distributed over the most populated cities 
	 * in KSA
	 */
	public static void GenerateJobCompanies_ar()
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);
			String[][] cities = new String[200][4];

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			ResultSet rs = null;
			String query = "SELECT count(*) from Employment.employer";
			rs = stmt.executeQuery(query);
			if (rs.next() && !rs.getString(1).equalsIgnoreCase("0"))
				return;

			//System.out.println (new Date());
			query = "SELECT * FROM taxonomies.geoname where feature_class='P' and population>10000";
			rs = stmt.executeQuery(query);
			int i = 0;
			while (rs.next())
			{
				//rowIterator.next();
				//For each row, iterate through all the columns
				cities[i][0] = rs.getString("asciname"); // city name
				cities[i][1] = rs.getString("latitude");
				cities[i][2] = rs.getString("longitude");
				cities[i][3] = rs.getString("country");
				i++;

				//writer.println(Names[i][0]);
			}
			query = "SELECT * from taxonomies.companies";
			rs = stmt.executeQuery(query);
			Random rn = new Random();
			int nbre = 0;
			int j = 1;
			double shift1, shift2;
			while (rs.next())
			{
				nbre = rn.nextInt(i);
				if (rn.nextInt(10) < 5)
				{
					shift1 = rn.nextDouble() / 100;
					shift2 = rn.nextDouble() / 100;
				}
				else
				{
					shift1 = -rn.nextDouble() / 100;
					shift2 = -rn.nextDouble() / 100;
				}
				query = "insert into Employment.employer (employer_id, company_name, industry_sector, city, country, latitude, longitude) values ("
					+ j++
					+ ", \""
					+ rs.getString("name")
					+ "\", '"
					+ rs.getString("sector")
					+ "', \""
					+ cities[nbre][0]
					+ "\", '"
					+ cities[nbre][3]
					+ "', "
					+ (Double.parseDouble(cities[nbre][1]) + shift1)
					+ ", "
					+ (Double.parseDouble(cities[nbre][2]) + shift2)
					+ ")";
				//System.out.println (query + "\n" + shift1 + shift2);
				stmt2.executeUpdate(query);
			}
			System.out.println(new Date());

			System.out.println("\n *** Generating companies/employers (" + j + ") ***  ");
			rs.close();
			stmt.close();
			stmt2.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/*
	 * Assigns random employers/companies to job posting
	 * takes into account the sector industry 
	 * This method can be improved by assigning employers from sector that fits the occupation
	 */
	public static void assignJobCompany_ar()
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		try
		{

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			ResultSet rs = null;
			System.out.println(new Date());
			
			//Create dictionary of sectors with companies ids
			Map<String, String> EmpDic = new HashMap<String, String>();
			
			// change the sector (in Arabic) to English so that we can perform the assignments
			String query = "update employment.job a set job_sector = (select sector from taxonomies.sector_ar b where a.job_occupation=b.occup_code) where language='ar'";
			stmt2.executeUpdate(query);

			query = "SELECT distinct industry_sector FROM employment.employer";
			rs = stmt.executeQuery(query);
			while (rs.next())
				EmpDic.put(rs.getString("industry_sector"), "");
			
			query = "SELECT employer_id, industry_sector FROM employment.employer order by industry_sector, employer_id";
			rs = stmt.executeQuery(query);
			//String strValue;
			while (rs.next()) {
				//strValue = EmpDic.get(rs.getString("job_sector"));
				EmpDic.put(rs.getString("industry_sector"), EmpDic.get(rs.getString("industry_sector")) + " " + rs.getString("employer_id"));
			}
			
			Random rn = new Random();

			query = "SELECT count(*) from Employment.employer";
			rs = stmt.executeQuery(query);
			int counts = 0;
			if (rs.next())
			{
				counts = rs.getInt(1);
			}

			String employers[] = null;
			query = "SELECT job_id, job_title, job_sector from Employment.job";
			rs = stmt.executeQuery(query);
			rn = new Random();
			String strValue = "";
			while (rs.next())
			{
				if (EmpDic.get(rs.getString("job_sector"))!=null)
					strValue = EmpDic.get(rs.getString("job_sector"));
				else
					strValue = EmpDic.get("Cross Sector");
				
				strValue = strValue.substring(1, strValue.length());
				employers = strValue.split(" ");
					
				query = "update  Employment.job set employer_id='"
					+ (employers[rn.nextInt(employers.length)])
					//+ (rn.nextInt(counts) + 1)
					+ "' where job_id='"
					+ rs.getString("job_id")
					+ "'";
				//System.out.println (query + rs.getString("job_sector"));
				stmt2.executeUpdate(query);
			}
			// change the sector back (to Arabic) for job titles in Arabic
			query = "update employment.job a set job_sector = (select sector_ar from taxonomies.sector_ar b where a.job_occupation=b.occup_code) where language='ar'";
			stmt2.executeUpdate(query);

			System.out.println("\n *** Assigned JOB companies/employers (" + counts + ") ***  ");
			rs.close();
			stmt.close();
			stmt2.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void generateJobLanguages() throws ClassNotFoundException
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null;
			String query;
			System.out.println("\n *** Generating job languages ***\t  ");

			query = "SELECT job_code, disc_title_en, proficiency_title_en from asoc.occup_languages, Employment.job where left(job_occupation,5)=left(occup_code,5)";
			rs = stmt.executeQuery(query);

			while (rs.next())
			{
				query = "insert into Employment.job_languages (job_code, language_name, language_level) values ('"
					+ rs.getString(1)
					+ "', '"
					+ rs.getString(2)
					+ "', '"
					+ rs.getString(3)
					+ "')";
				//System.out.println("\t\t- " + query);
				System.out.print(".");
				stmt2.executeUpdate(query);

			}

			rs.close();

			stmt2.executeUpdate("update employment.job_languages set language_level='Beginner' where language_level='Basic'");
			stmt2.executeUpdate("update employment.job_languages set language_level='Advanced' where language_level='Expert'");
			stmt.close();
			stmt2.close();
			conn.close();

		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
	}//end try

	public static void generateJobEducation() throws ClassNotFoundException
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null;
			String query;
			System.out.println("\n *** Generating job educations ***\t  ");

			query = "SELECT job_code, a.edu_code, edu_title_en FROM asoc.occupation_education a, asoc.education b where a.edu_code=b.edu_code";
			rs = stmt.executeQuery(query);

			while (rs.next())
			{
				query = "insert into Employment.job_education (job_id, education_level, education_name) values ('"
					+ rs.getString(1)
					+ "', '"
					+ rs.getString(2)
					+ "', '"
					+ rs.getString(3)
					+ "')";
				//System.out.println("\t\t- " + query);
				System.out.print(".");
				stmt2.executeUpdate(query);

			}
			String[][] edu = { { "EL_0", "LES" }, { "EL_1", "ESH" }, { "EL_2", "MSH" }, { "EL_3", "HSH" }, { "EL_4", "TCD" }, { "EL_5", "HTC" }, { "EL_6", "BDG" }, { "EL_7", "MDG" } , { "EL_8", "DDG" } };

			//Generate specific ~100 occupations for main demo
			for (int i = 0; i < edu.length; i++)
			{
				stmt2.executeUpdate("update Employment.job_education set education_level='" + edu[i][1] + "' where education_level='" + edu[i][0] + "'");
			}
			rs.close();

			stmt2.executeUpdate("update employment.job_education set edu_discipline=(select distinct disc_title_en from asoc.discipline where occup_code=job_id limit 1)");
			stmt2.executeUpdate("update employment.job_education set edu_field=(select distinct field_title_en from asoc.discipline where occup_code=job_id limit 1)");
			stmt.close();
			stmt2.close();
			conn.close();

		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
	}//end try


	/*
	 * Generates address information including exact latitude and longitude 
	 * based on lookup data for the Dutch zip codes 
	 */
	public static void generateZipCodes(Map<String, Integer> cities)
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;
			Iterator<Map.Entry<String, Integer>> it = cities.entrySet().iterator();
			String query;
			int nbre = 0, i = 1, j = 1;

			System.out.print("Generating Addresses ...\t");
			while (it.hasNext())
			{
				Map.Entry<String, Integer> entry = it.next();
				nbre = entry.getValue();
				System.out.print(entry.getKey() + "(" + nbre + "), ");
				query = "select * from escoskos.dutchpostalcodelookup where city = \"" + entry.getKey() + "\"";
				rs = stmt.executeQuery(query);
				for (i = 1; rs.next() && i <= nbre; j++, i++)
				{
					//System.out.println("\t" + j + ". " + zipCode + " " + city + ", " + country);
					zipcodes.put(
						j,
						rs.getString(1)
							+ "##"
							+ rs.getString(3)
							+ "##"
							+ rs.getString(5)
							+ "##"
							+ rs.getString(6)
							+ "##"
							+ rs.getString(8)
							+ "##"
							+ rs.getString(9));

				}

			}
			System.out.println("\n\tTotal of \t" + j);

			query = "SELECT candidate_id from Employment.candidate where candidate_id not in (select distinct candidate_id from Employment.candidate_locations)";
			//query = "SELECT candidate_id from Employment.candidate where zipcode is null and geo_latitude is null";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			for (; rs.next();)
			{
				String[] addresses = zipcodes.get(rn.nextInt(j - 1) + 1).split("##");
				query = "insert into Employment.candidate_locations values ('"
					+ rs.getInt(1)
					+ "','"
					+ addresses[0]
					+ "', \""
					+ addresses[1]
					+ "\", \""
					+ addresses[3]
					+ "\", '"
					+ addresses[2]
					+ "', '"
					+ addresses[4]
					+ "', '"
					+ addresses[5]
					+ "', 'Home',"
					+ (rn.nextInt(10) * 5 + 25)
					+ ",'Netherlands')";
				System.out.print(".");
				stmtS.executeUpdate(query);
			}
			rs.close();

			//STEP 6: Clean-up environment
			stmt.close();
			stmtS.close();
			conn.close();

		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
		finally
		{
			//finally block used to close resources
			try
			{
				if (stmt != null)
				{
					stmt.close();
				}
			}
			catch (SQLException se2)
			{
			} // nothing we can do
			try
			{
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
		} //end try
	}//end

	/*
	 * Generates approximate latitude and longitude for the job seekers in KSA
	 * job seekers locations are spread over the major cities in KSA 
	 */
	public static void generateAddresses_sa(String addressType)
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;
			String[][] cities = new String[200][4];
			String query = "SELECT * FROM taxonomies.geoname where feature_class='P' and population>10000";
			rs = stmt.executeQuery(query);
			int i = 0;
			while (rs.next())
			{
				//rowIterator.next();
				//For each row, iterate through all the columns
				cities[i][0] = rs.getString("asciname"); // city name
				cities[i][1] = rs.getString("latitude");
				cities[i][2] = rs.getString("longitude");
				cities[i][3] = rs.getString("country");
				i++;

				//writer.println(Names[i][0]);
			}
			Random rn = new Random();
			int nbre = 0;
			double shift1, shift2;

			System.out.print("Generating Addresses ...\t");
			query = "SELECT candidate_id from Employment.candidate where candidate_id not in (select distinct candidate_id from Employment.candidate_locations where address_type='" + addressType + "')";
			//query = "SELECT candidate_id from Employment.candidate where zipcode is null and geo_latitude is null";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				nbre = rn.nextInt(i);
				if (rn.nextInt(10) < 5)
				{
					shift1 = rn.nextDouble() / 50;
					shift2 = rn.nextDouble() / 50;
				}
				else
				{
					shift1 = -rn.nextDouble() / 50;
					shift2 = -rn.nextDouble() / 50;
				}
				query = "insert into Employment.candidate_locations (candidate_id, city, country, geo_latitude, geo_longitude, address_type, distance) values ('"
					+ rs.getInt(1)
					+ "',\""
					+ cities[nbre][0]
					+ "\", '"
					+ cities[nbre][3]
					+ "', "
					+ (Double.parseDouble(cities[nbre][1]) + shift1)
					+ ", "
					+ (Double.parseDouble(cities[nbre][2]) + shift2)
					+ ", '" + addressType + "',"
					+ (rn.nextInt(10) * 5 + 25)
					+ ")";
				System.out.print(".");
				stmtS.executeUpdate(query);
			}
			rs.close();

			//STEP 6: Clean-up environment
			stmt.close();
			stmtS.close();
			conn.close();

		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
	}//end

	public static void generateZipCodes_old(Map<String, Integer> cities)
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;
			Iterator<Map.Entry<String, Integer>> it = cities.entrySet().iterator();
			String city = "", zipCode, query;
			int nbre = 0, from_ = 0, to_ = 0;
			int j = 1;

			while (it.hasNext())
			{
				Map.Entry<String, Integer> entry = it.next();
				nbre = entry.getValue();
				query = "select from_, to_, city, country from escoskos.zip_code where city like '" + entry.getKey() + "%'";
				//System.out.println("\t- " + query);
				rs = stmt.executeQuery(query);
				if (rs.next())
				{
					from_ = rs.getInt(1);
					to_ = rs.getInt(2);
					city = rs.getString(3);
					//System.out.println(from_ + "-" + to_ + ": " + entry.getKey() + "-" + city);
					if (city.indexOf(" ") > 0)
					{
						city = city.substring(0, city.indexOf(" "));
					}
					if (city.indexOf("-") > 0)
					{
						city = city.substring(0, city.indexOf("-"));
					}
					if (city.indexOf(",") > 0)
					{
						city = city.substring(0, city.indexOf(","));
					}

					Random rn = new Random();
					for (int i = 0; i < nbre; i++)
					{
						if (to_ == from_)
						{
							zipCode = String.valueOf(from_) + " " + (char)(rn.nextInt(26) + 65) + (char)(rn.nextInt(26) + 65);
						}
						else
						{
							zipCode = String.valueOf(rn.nextInt(to_ - from_) + from_)
								+ (char)(rn.nextInt(26) + 65)
								+ (char)(rn.nextInt(26) + 65);
						}

						//System.out.println("\t" + j + ". " + zipCode + " " + city + ", " + country);
						zipcodes.put(j, "" + zipCode + " " + city);
						j++;
					}

				}

			}

			query = "SELECT candidate_id from Employment.candidate where zipcode is null";
			//query = "SELECT candidate_id from Employment.candidate where zipcode is null and geo_latitude is null";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			for (; rs.next();)
			{
				String[] addresses = zipcodes.get(rn.nextInt(j - 1) + 1).split(" ");
				System.out.println(addresses[0]);
				query = "update Employment.candidate set zipcode ='"
					+ addresses[0]
					+ "', city='"
					+ addresses[1]
					+ "' where candidate_id="
					+ rs.getInt(1);
				stmtS.executeUpdate(query);
			}
			rs.close();

			//STEP 6: Clean-up environment
			stmt.close();
			stmtS.close();
			conn.close();

		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
		finally
		{
			//finally block used to close resources
			try
			{
				if (stmt != null)
				{
					stmt.close();
				}
			}
			catch (SQLException se2)
			{
			} // nothing we can do
			try
			{
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
		} //end try
	}//end
	

	public static void enrichLanguages_fast(int ratioage, String[][] languages) throws SQLException
	{
		Date date = new Date();
		String query = "";
		int total=0;

		for (int i=0; i<languages.length; i++)
			total = total + Integer.parseInt(languages[i][1]);

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null;
			ResultSet rs2 = null;

			String lang = "";
			String profile_id="";
			String js_id="";
			long i=1;
			long profiles=0;
			long jobseekers=0;
			long new_ratioage=0;
			int j=0;
			rs = stmt.executeQuery("select max(PROFILECOMPETENCYID)+1 as recordid from CEMSPROFILECOMPETENCY");
			if (rs.next())
				i = rs.getLong("recordid");
			long start = i;
			
			rs = stmt.executeQuery(
					"SELECT count(distinct a.PROFILEID) profiles, count(distinct a.CONCERNROLEID) jobseekers "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			if (rs.next()) 
			{
				profiles = rs.getLong("profiles");
				jobseekers = rs.getLong("jobseekers");
				new_ratioage = ratioage * (int) Math.round(rs.getDouble("profiles")/rs.getDouble("jobseekers"));
			}

			rs.close();
			stmt.close();

			System.out.println("\n *** updating candidates  ***  ");

			rs2 = stmt2.executeQuery(
					"SELECT Distinct a.CONCERNROLEID as js_id, a.PROFILEID as profile_id "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
 							+ "order by a.CONCERNROLEID"
					); 
			Random rn = new Random();
			while (rs2.getRow()< profiles && rs2.next())
			{
				js_id = rs2.getString("js_id");
				if (rn.nextInt(100)>new_ratioage) {
					while (rs2.getRow()< profiles && js_id.equalsIgnoreCase(rs2.getString("js_id")))
						rs2.next();
				}
				else
				{
					if (rs2.getRow()>= profiles)
						break;
					js_id = rs2.getString("js_id");
					System.out.println((j++) + "- jobseeker_id: " + js_id ); 
					while (js_id.equalsIgnoreCase(rs2.getString("js_id")))
					{
						js_id = rs2.getString("js_id");
						profile_id = rs2.getString("profile_id");
						lang = pickValue(languages, rn.nextInt(total) + 1);
	
						query = "insert into CEMSPROFILECOMPETENCY (PROFILECOMPETENCYID, RELATEDID,  COMPETENCYTAXONOMYID, COMPETENCYTYPE, WEIGHTINGABILITY, WEIGHTINGIMPORTANCE,  RECORDSTATUS, NEGATIVESEARCHPARAM, APPROVEDFORPUBLICATION, RELATEDTYPE, VERSIONNO, LASTWRITTEN) values ("
							+ i + "," + profile_id + "," +  lang + ", 'LANGUAGE', 0, 1, 'RST1', 0, 0, 'JSP', 1, CURRENT TIMESTAMP)" ;
						System.out.println(query);
						//stmt2.executeUpdate(query);
						i++;
						if (rs2.getRow()>= profiles)
							continue;
					
						rs2.next();
					}
				}
			}
			i--;
			rs2.close();
			stmt2.close();
			conn.close();
			String report = "\n\t****  Data generation report  **** \ndatabase: " + DB_URL
					+ "\nTiming:\n\tStart at: " + date + "\n\tEnd at:  " + new Date() + "\n"
					+ "Porperty: Jobseeker languages\nGenerated records: " + (i-start+1) 
					+ "\n\tfrom: " + start + "\n\tto: " + i + ")\n"
					+ "Out of:\n\t" + profiles + " profiles\n\t" + jobseekers + " jobseekers"
					+ "\nRatio: " + ratioage + " (actual: " + (i-start+1)*100/profiles + "% - adjusted: " + new_ratioage + " )\n" 
					+ "To delete the generated data, you need to run the query:\n\tdelete from CEMSPROFILECOMPETENCY where PROFILECOMPETENCYID>=" + start + " and PROFILECOMPETENCYID<= " + i ;
;
			System.out.println(report);
			try(FileWriter fw = new FileWriter("c:\\AMS\\workspace\\dataGeneration_AMSreport.txt", true);
				    BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw))
				{
				    out.println(report);
				    out.close();
				    bw.close();
				    fw.close();
				} catch (IOException e) {
				    //exception handling left as an exercise for the reader
				}
			
		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
	}//end

	/*
	 *  The generateCandidates method generates candidates with random values for occupation, driving license, gender, marital status, and disabilities
	 *  It also generates working experience for the candidate based on his/her age and based on current occupation
	 */

	public static void updateCandidates2(int counts) throws SQLException
	{
		int jump = 3;
		Date date = new Date();
		int salary = 9000; // yearly salary
		int hours = 0; //availability hours
		String jobFP = ""; // job full/part time
		String jobShift = ""; // job Shift Type
		String query = "", jobTitle, ocId;
		int empCounts = 0; //counts of employers
		int ratioage=12;
		
		String[][] languages = {{"264992","89", "Englisch"},{"260642", "18", "Französisch"},{"260643", "10", "Italienisch"},{"274466","8", "Serbisch"},{"260652","7", "Türkisch"},{"260650","6", "Spanisch"},{"262984","6", "Kroatisch"},{"260647","4", "Russisch"},{"264692","3", "Bosnisch"}};
		String[][] genders = {{"Male","6"},{"Female","4"}};
		String[][] driving = {{"Yes","7"},{"No","3"}};
		String[][] M_Statuses = {{"Married","80"},{"Single","17"},{"Divorced","2"},{"Widowed","1"}};
		String[][] statuses = {{"Active","8"},{"Inactive","2"}};
		String[][] nationalities = {{"SA","100"}}; //,{"SD","5"},{"BH","5"},{"OM","4"},{"PK","4"},{"IN","4"},{"QA","4"},{"ID","5"},{"IR","4"},{"PH","5"}};
		String[][] jobFPs = {{"Full-time","65"},{"Part-time","35"}};
		String[][] contractType = {{"Contract","65"},{"Permanent","35"}};
		String[][] jobType = {{"Regular Job","65"},{"Internship","15"},{"Summer Job","10"},{"Mini Job","10"}};
		String[][] jobShifts = {{"Day","65"},{"Night","20"},{"Two Shifts","15"}};
		String[][] jobScheduleP = {{"08:00 - 12:00","20"},{"12:00 - 17:00","20"},{"09:00 - 12:00","15"},{"13:00 - 17:00","15"},{"12:00 - 16:00","10"},{"17:00 - 22:00","10"},{"18:00 - 24:00","10"}};
		String[][] jobScheduleF = {{"09:00 - 17:00","20"},{"09:00 - 17:00","20"},{"08:00 - 16:00","15"},{"08:00 - 16:00","15"},{"16:00 - 24:00","10"},{"18:00 - 02:00","10"},{"20:00 - 04:00","10"}};
		String[][] travel = {{"0%","40"},{"25%","20"},{"50%","15"},{"75%","12"},{"100%","8"}};
		String[][] disabilities = {{"","96"},{"Attention-Deficit","1"},{"Hyperactivity Disorders","1"},{"Blindness","1"},{"Low Vision","1"},{"Blindness","1"},
			{"Brain Injuries","1"},{"Deaf","1"},{"Hard-of-Hearing","1"},{"Learning Disabilities","1"},{"Medical Disabilities","1"},
			{"Speech Disabilities","1"},{"Language Disabilities","1"}};
		String[][] telework = {{"Yes","2"},{"No","8"}};
		String[][] commute_distance = {{"25","50"},{"50","30"},{"75","10"},{"100","10"}};
		String[][] ordering = {{"","20"},{" order by ConceptPT","20"},{" order by ConceptPT desc","20"},{" order by length(ConceptPT)","20"},{" order by length(ConceptPT) desc","20"}};

		int[] totals = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}; // to hold the totals for each array
									// 0: M_Statuses
		for (int i=0; i<languages.length; i++)
			totals[0] = totals[0] + Integer.parseInt(languages[i][1]);
		
		for (int i=0; i<genders.length; i++)
			totals[1] = totals[1] + Integer.parseInt(genders[i][1]);
		
		for (int i=0; i<driving.length; i++)
			totals[2] = totals[2] + Integer.parseInt(driving[i][1]);
		
		for (int i=0; i<M_Statuses.length; i++)
			totals[3] = totals[3] + Integer.parseInt(M_Statuses[i][1]);
		
		for (int i=0; i<statuses.length; i++)
			totals[4] = totals[4] + Integer.parseInt(statuses[i][1]);
		
		for (int i=0; i<nationalities.length; i++)
			totals[5] = totals[5] + Integer.parseInt(nationalities[i][1]);
		
		for (int i=0; i<jobFPs.length; i++)
			totals[6] = totals[6] + Integer.parseInt(jobFPs[i][1]);
		
		for (int i=0; i<contractType.length; i++)
			totals[7] = totals[7] + Integer.parseInt(contractType[i][1]);
		
		for (int i=0; i<jobType.length; i++)
			totals[8] = totals[8] + Integer.parseInt(jobType[i][1]);
		
		for (int i=0; i<jobShifts.length; i++)
			totals[9] = totals[9] + Integer.parseInt(jobShifts[i][1]);
		
		for (int i=0; i<jobScheduleP.length; i++)
			totals[10] = totals[10] + Integer.parseInt(jobScheduleP[i][1]);
		
		for (int i=0; i<jobScheduleF.length; i++)
			totals[11] = totals[11] + Integer.parseInt(jobScheduleF[i][1]);
		
		for (int i=0; i<travel.length; i++)
			totals[12] = totals[12] + Integer.parseInt(travel[i][1]);
		
		for (int i=0; i<disabilities.length; i++)
			totals[13] = totals[13] + Integer.parseInt(disabilities[i][1]);
		
		for (int i=0; i<telework.length; i++)
			totals[14] = totals[14] + Integer.parseInt(telework[i][1]);
		
		for (int i=0; i<commute_distance.length; i++)
			totals[15] = totals[15] + Integer.parseInt(commute_distance[i][1]);
		
		for (int i=0; i<ordering.length; i++)
			totals[16] = totals[16] + Integer.parseInt(ordering[i][1]);
		

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = null;
			ResultSet rs2 = null;

			String lang = "";
			String profile_id="";
			String js_id="";
			long i=1;
			long profiles=0;
			long new_ratioage=0;
			rs = stmt.executeQuery("select max(PROFILECOMPETENCYID)+1 as recordid from CEMSPROFILECOMPETENCY");
			if (rs.next())
				i = rs.getLong("recordid");
			long start = i;
			
			rs = stmt.executeQuery(
					"SELECT count(distinct a.PROFILEID) profiles, count(distinct a.CONCERNROLEID) jobseekers "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
					); 
			if (rs.next()) 
			{
				profiles = rs.getLong("profiles");
				new_ratioage = ratioage * (int) Math.round(rs.getDouble("profiles")/rs.getDouble("jobseekers"));
			}



			System.out.println("\n *** updating candidates  ***  ");

			rs = stmt.executeQuery(
					"SELECT Distinct a.CONCERNROLEID as js_id, a.PROFILEID as profile_id "
							+ "FROM CEMSPROFILE a "
							+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
							+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
							+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                            + "AND a.PROFILETYPE = 'JOBSEEKER'"
 							+ "order by a.CONCERNROLEID"
					); 
					// and a.PROFILEID not in (
					//select RelatedID from CEMSPROFILECOMPETENCY where COMPETENCYTAXONOMYID!=256685 and COMPETENCYTYPE='LANGUAGE' 
					//and RELATEDTYPE='JSP' and RECORDSTATUS='RST1')
				// add where clause when candidate doesn't have a language
			while (rs.getRow()< profiles && rs.next())
			{
				Random rn = new Random();
				js_id = rs.getString("js_id");
				if (rn.nextInt(100)>ratioage) {
					while (rs.getRow()< profiles && rs.next()  && js_id.equalsIgnoreCase(rs.getString("js_id")))
						;
				}
				if (rs.getRow()>= profiles)
					break;
				js_id = rs.getString("js_id");
				System.out.println("jobseeker_id: " + js_id ); 
				while (js_id.equalsIgnoreCase(rs.getString("js_id")))
				{
/*					rs2 = stmt2.executeQuery(
							"SELECT Distinct a.PROFILEID as profile_id "
									+ "FROM CEMSPROFILE a "
									+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
									+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
									+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
		                            + "AND a.PROFILETYPE = 'JOBSEEKER' "
		 							+ "and a.CONCERNROLEID = " + js_id ); 
					
					System.out.println("jobseeker_id: " + js_id); 
					lang = pickValue(languages, rn.nextInt(totals[0]) + 1);
					while (rs2.next())
					{
*/						profile_id = rs.getString("profile_id");
						lang = pickValue(languages, rn.nextInt(totals[0]) + 1);

		
		
		
		/*
		 * 				jobShift = "Day";
						// generating gender type
						String gender = pickValue(genders, rn.nextInt(totals[1]) + 1);
		
						jobFP = pickValue(jobFPs, rn.nextInt(totals[6]) + 1);
						hours = jobFP.equalsIgnoreCase("Full-time")?getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]):getHours(jobScheduleP[rn.nextInt(jobScheduleP.length)][0]);
		
		
							System.out.println((i++) + "- js_id" + js_id + 
								"\n\t- gender: " + gender +
								"\n\t- salary: " + salary +
								"\n\t- contractType: " + pickValue(contractType, rn.nextInt(totals[7]) + 1) +
								"\n\t- jobType: " + pickValue(jobType, rn.nextInt(totals[8]) + 1) +
								"\n\t- jobFPs: " + pickValue(jobFPs, rn.nextInt(totals[6]) + 1) +
								"\n\t- hours: " + hours +
								"\n\t- travel: " + pickValue(travel, rn.nextInt(totals[12]) + 1) +
								"\n\t- jobShifts: " + pickValue(jobShifts, rn.nextInt(totals[9]) + 1) +
								"\n\t- telework: " + pickValue(telework, rn.nextInt(totals[14]) + 1) +
								"\n\t- commute_distance: " + pickValue(commute_distance, rn.nextInt(totals[15]) + 1) +
								"\n\t- language: " + lang
							);
		
		*/				
						query = "insert into CEMSPROFILECOMPETENCY (PROFILECOMPETENCYID, RELATEDID,  COMPETENCYTAXONOMYID, COMPETENCYTYPE, WEIGHTINGABILITY, WEIGHTINGIMPORTANCE,  RECORDSTATUS, NEGATIVESEARCHPARAM, APPROVEDFORPUBLICATION, RELATEDTYPE, VERSIONNO, LASTWRITTEN) values ("
							+ i + "," + profile_id + "," +  lang + ", 'LANGUAGE', 0, 1, 'RST1', 0, 0, 'JSP', 1, CURRENT TIMESTAMP)" ;
						System.out.println(query);
						//stmt3.executeUpdate(query);
						i++;
						if (rs.getRow()>= profiles)
							break;
						else
							rs.next();
					}
				//}

			}
			i--;
			//rs2.close();
			//stmt2.close();
			stmt3.close();
			String report = "\n\t****  Data generation report  **** \ndatabase: " + DB_URL
					+ "\nStart at: " + date + "\nEnd at:  " + new Date() + "\n"
					+ "Porperty: Jobseeker languages\nGenerated records: " + (i-start+1) 
					+ "\nRatio: " + ratioage + " (actual: " + ratioage*profiles/(i-start+1) + ")" 
					+ " (from: " + start + " to: " + i + ")\n"
					+ "To delete the generated data, you need to run the query:\n\tdelete from CEMSPROFILECOMPETENCY where PROFILECOMPETENCYID>=" + start + " and PROFILECOMPETENCYID<= " + i ;
;
			System.out.println(report);
			try(FileWriter fw = new FileWriter("c:\\AMS\\workspace\\dataGeneration_AMSreport.txt", true);
				    BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw))
				{
				    out.println(report);
				    out.close();
				    bw.close();
				    fw.close();
				} catch (IOException e) {
				    //exception handling left as an exercise for the reader
				}
			
		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
		finally
		{
			//finally block used to close resources
			try
			{
				if (stmt != null)
				{
					stmt.close();
				}
			}
			catch (SQLException se2)
			{
			} // nothing we can do
			try
			{
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
		} //end try
	}//end

	public static void GenCandidateSkills()
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmtC = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmtC = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs = null, rsC;
			int i = 0;
			int c = 0;

			String query = "SELECT candidate_id, job_title from Employment.candidate_ambitions where candidate_id not in (select distinct candidate_id from Employment.candidate_skills)";
			//System.out.println(query);
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();

				System.out.print("\nSimilar occupations for " + rsC.getString(2) + ": \n\t-");
				// retrieve list of skills for jobs, to create candidate skills
				query = "SELECT ConceptURI, ConceptPT FROM skills where ConceptURI in (select hasRelatedConcept from occupation_skill where isRelatedConcept=(SELECT ConceptURI FROM escoskos.occupations where conceptPT = \""
					+ rsC.getString(2)
					+ "\"))";
				//System.out.println(query);
				rs = stmt2.executeQuery(query);
				String skillId = "";
				for (; rs.next();)
				{
					skillId = rs.getString(1);
					skillId = skillId.substring(skillId.lastIndexOf("/") + 1, skillId.length());
					skills.put(skillId, rs.getString(2));
					System.out.print(rs.getString(2) + ", ");
					i++;
					//query = "insert into Employment.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');";
					//System.out.println("\t" + query);
					//stmt.executeUpdate(query);
				}
				rs.close();
				if (skills.size() >= 25)
				{
					i = 12;
				}
				if (skills.size() < 25 && skills.size() >= 20)
				{
					i = 10;
				}
				if (skills.size() <= 15 && skills.size() > 8)
				{
					i = 8;
				}
				if (skills.size() <= 8 && skills.size() > 4)
				{
					i = 5;
				}
				if (skills.size() <= 4)
				{
					i = skills.size();
				}

				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();

				c = 0;

				while (it.hasNext() && c < i)
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into Employment.candidate_skills (candidate_id, skill_id, skill_name) values ("
						+ rsC.getString(1)
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue());
					System.out.print(entry.getValue() + ", ");
					stmt.executeUpdate(query);
					c++;

				}
				i = 0;
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
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
		finally
		{
			//finally block used to close resources
			try
			{
				if (stmt != null)
				{
					stmt.close();
				}
			}
			catch (SQLException se2)
			{
			} // nothing we can do
			try
			{
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
		} //end try
	}//end function

	public static void GenCandidateSkills2()
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmtC = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmtC = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs = null, rs2, rsC;
			int i = 0;
			int c = 0;

			String query = "SELECT candidate_id, job_title from Employment.candidate_ambitions where candidate_id not in (select distinct candidate_id from Employment.candidate_skills)";
			//System.out.println(query);
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();
				// retrieve list of similar occupations, to create work experience
				query = "SELECT occupation, similar FROM occupation_similars where ConceptPT=\"" + rsC.getString(2) + "\"";
				//System.out.println(query);
				rs = stmt.executeQuery(query);
				//Map<Integer, String> similars = new HashMap<Integer, String>();
				//similars.put(0, rsC.getString(2));
				String ocId = "";
				//System.out.print("\n" + rsC.getString(1) + ": " + rsC.getString(2) +"\n\t -");
				System.out.println("\nSimilar occupations for " + rsC.getString(2) + ": ");
				for (; rs.next();)
				{
					//similars.put(j, rs.getString(2));
					ocId = rs.getString(1);
					System.out.print("\n\t - " + rs.getString(2) + "\n\t\tSkills: ");

					// retrieve list of skills for jobs, to create candidate skills
					query = "SELECT ConceptURI, ConceptPT FROM skills where ConceptURI in (select hasRelatedConcept from occupation_skill where isRelatedConcept='"
						+ ocId
						+ "')";
					//System.out.println(query);
					rs2 = stmt2.executeQuery(query);
					String skillId = "";
					for (; rs2.next();)
					{
						skillId = rs2.getString(1);
						skillId = skillId.substring(skillId.lastIndexOf("/") + 1, skillId.length());
						skills.put(skillId, rs2.getString(2));
						System.out.print(rs2.getString(2) + ", ");
						//query = "insert into Employment.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');";
						//System.out.println("\t" + query);
						//stmt.executeUpdate(query);
					}
					rs2.close();
				}
				rs.close();
				if (skills.size() >= 25)
				{
					i = 12;
				}
				if (skills.size() < 25 && skills.size() >= 20)
				{
					i = 10;
				}
				if (skills.size() <= 15 && skills.size() > 8)
				{
					i = 8;
				}
				if (skills.size() <= 8 && skills.size() > 4)
				{
					i = 5;
				}
				if (skills.size() <= 4)
				{
					i = skills.size();
				}

				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();

				while (it.hasNext() && c < i)
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into Employment.candidate_skills (candidate_id, skill_id, skill_name) values ("
						+ rsC.getString(1)
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue());
					System.out.print(entry.getValue() + ", ");
					//stmt.executeUpdate(query);
					c++;

				}
				i = 0;
			}

			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			conn.close();
		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
		finally
		{
			//finally block used to close resources
			try
			{
				if (stmt != null)
				{
					stmt.close();
				}
			}
			catch (SQLException se2)
			{
			} // nothing we can do
			try
			{
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
		} //end try
	}//end function

	/*
	 * This function loads the a list of items into a dictionary, so that it becomes easy to randomly pick random items when automatically generating the data
	 * @param trg_occup if a target occupation is provided, this modules only loads similar jobs to the specified occupation (level 5)
	 * if occupation is not specified (null), all the jobs in the taxonomy will be loaded
	 * @param occupSRC: source of taxonomy occupations
	 * @return void
	 */
	public static Map<String, String> loadItems(String query)
	{
		Map<String, String> itemsDic = new HashMap<String, String>();
		Connection conn = null;
		Statement stmt = null;
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			ResultSet rs;
			System.out.println(query);
/*			if (1==1)
				return;
*/			rs = stmt.executeQuery(query);

			int i = 0;
			itemsDic.clear();
			while (rs.next())
			{
				itemsDic.put(String.valueOf(i++), rs.getString(1));
			}
			System.out.println("size: " + itemsDic.size());
			rs.close();
			stmt.close();
			conn.close();
		}
		catch (SQLException se)
		{
			se.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return itemsDic;
	}//end function

	/*
	 * This function automatically generate jobs
	 * @param trg_occup if an occupation is provided, only similar jobs to the specified occupation (level 5) are created
	 * if no occupation is specified (null), jobs will be created by selecting random jobs from the totality of jobs in the taxonomy
	 * @param counts number of jobs to create
	 * @return void
	*/
	public static void generateJobs(int trg_occup, int counts, String tableName)
	{
		if (counts==0)
			return;
		Connection conn = null;
		Statement stmt = null;
		Statement stmtS = null;
		Statement stmtQ = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmtS = conn.createStatement();
			stmtQ = conn.createStatement();
			ResultSet rs;

			rs = stmt.executeQuery("select max(job_id) from Employment.job");
			int start = 1;
			if (rs.next())
			{
				start = rs.getInt(1) + 1;
			}
			//loadJobs(trg_occup, tableName);
			
			if (start<maxSpecific && from_>maxSpecific)
				start = from_;
			
			int salary = 0;

			System.out.println("\n *** Generating Job data (" + counts + ") " + ((trg_occup!=0)?"occupation: " + trg_occup:"") + " ***  ");

			String[][] languages = {{"en","6"},{"ar","4"}};
			String[][] genders = {{"Male","6"},{"Female","4"}};
			String[][] driving = {{"Yes","7"},{"No","3"}};
			String[][] statuses = {{"Active","8"},{"Inactive","2"}};
			String[][] contractType = {{"Contract","65"},{"Permanent","35"}};
			String[][] jobType = {{"Regular-job","65"},{"Internship","25"},{"Summer-job","10"}};
			String[][] travel = {{"0%","40"},{"25%","20"},{"50%","15"},{"75%","12"},{"100%","8"}};
			String[][] jobShifts = {{"Day","65"},{"Night","20"},{"Two Shifts","15"}};
			String[][] jobScheduleP = {{"08:00 - 12:00","20"},{"12:00 - 17:00","20"},{"09:00 - 12:00","15"},{"13:00 - 17:00","15"},{"12:00 - 16:00","10"},{"17:00 - 22:00","10"},{"18:00 - 24:00","10"}};
			String[][] jobScheduleF = {{"09:00 - 17:00","20"},{"09:00 - 17:00","20"},{"08:00 - 16:00","15"},{"08:00 - 16:00","15"},{"16:00 - 24:00","10"},{"18:00 - 02:00","10"},{"20:00 - 04:00","10"}};
			String[][] jobFPs = {{"Full-time","65"},{"Part-time","35"}};
			String[][] ordering = {{"","20"},{" order by ConceptPT","20"},{" order by ConceptPT desc","20"},{" order by length(ConceptPT)","20"},{" order by length(ConceptPT) desc","20"}};
			String[][] telework = {{"Yes","2"},{"No","8"}};
			String[][] disabilities = {{"96",""},{"Attention-Deficit","1"},{"Hyperactivity Disorders","1"},{"Blindness","1"},{"Low Vision","1"},{"Blindness","1"},
				{"Brain Injuries","1"},{"Deaf","1"},{"Hard-of-Hearing","1"},{"Learning Disabilities","1"},{"Medical Disabilities","1"},
				{"Speech Disabilities","1"},{"Language Disabilities","1"}};
		


			int[] totals = {0,0,0,0,0,0,0,0,0,0,0,0,0,0}; // to hold the totals for each array
			// 0: M_Statuses
			for (int i=0; i<languages.length; i++)
			totals[0] = totals[0] + Integer.parseInt(languages[i][1]);
			
			for (int i=0; i<genders.length; i++)
			totals[1] = totals[1] + Integer.parseInt(genders[i][1]);
			
			for (int i=0; i<driving.length; i++)
			totals[2] = totals[2] + Integer.parseInt(driving[i][1]);
			
			for (int i=0; i<statuses.length; i++)
			totals[3] = totals[3] + Integer.parseInt(statuses[i][1]);
			
			for (int i=0; i<contractType.length; i++)
			totals[4] = totals[4] + Integer.parseInt(contractType[i][1]);
			
			for (int i=0; i<jobType.length; i++)
			totals[5] = totals[5] + Integer.parseInt(jobType[i][1]);
			
			for (int i=0; i<travel.length; i++)
			totals[6] = totals[6] + Integer.parseInt(travel[i][1]);
			
			for (int i=0; i<jobShifts.length; i++)
			totals[7] = totals[7] + Integer.parseInt(jobShifts[i][1]);
			
			for (int i=0; i<jobScheduleP.length; i++)
			totals[8] = totals[8] + Integer.parseInt(jobScheduleP[i][1]);
			
			for (int i=0; i<jobScheduleF.length; i++)
			totals[9] = totals[9] + Integer.parseInt(jobScheduleF[i][1]);
			
			for (int i=0; i<jobFPs.length; i++)
			totals[10] = totals[10] + Integer.parseInt(jobFPs[i][1]);
			
			for (int i=0; i<ordering.length; i++)
			totals[11] = totals[11] + Integer.parseInt(ordering[i][1]);
			
			for (int i=0; i<telework.length; i++)
			totals[12] = totals[12] + Integer.parseInt(telework[i][1]);
			
/*			for (int i=0; i<disabilities.length; i++)
			totals[13] = totals[13] + Integer.parseInt(disabilities[i][1]);
*/
			String occup, order, part_full_time, jobSchedule = "", jobShift = "Day", job_code, isco_code;
			int hours=0;
			System.out.print("\t- ");
			int j = 1;
			Random oc = new Random();

			int index = jobsDic.size();
			while (j <= counts && index > 0)
			{
				if (j % 51 == 0)
				{
					System.out.print("\n\t- " + index);
				}
				occup = jobsDic.get(String.valueOf(oc.nextInt(index)));
				job_code = occup.substring(0, occup.indexOf("##"));
				isco_code = occup.substring(occup.indexOf("##") + 2, occup.length());

				Random rn = new Random();

				part_full_time = pickValue(jobFPs, rn.nextInt(totals[10]) + 1);
				order = pickValue(ordering, rn.nextInt(totals[11]) + 1);
				jobShift = pickValue(jobShifts, rn.nextInt(totals[7]) + 1);

				jobSchedule = part_full_time.equalsIgnoreCase("Full-time")?pickValue(jobScheduleF, rn.nextInt(totals[9]) + 1):pickValue(jobScheduleP, rn.nextInt(totals[8]) + 1);
				hours = getHours(jobSchedule);
				//hours = part_full_time.equalsIgnoreCase("Full-time")?getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]):getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]);


				switch (isco_code.charAt(0))
				{
				case '0':
					salary = (rn.nextInt(60) + 30) * 375;
					break; // Armed forces occupations
				case '1':
					salary = (rn.nextInt(80) + 40) * 375;
					break; // Managers
				case '2':
					salary = (rn.nextInt(60) + 40) * 375;
					break; // Professionals
				case '3':
					salary = (rn.nextInt(30) + 40) * 375;
					break; // Technicians and associate professionals
				case '4':
					salary = (rn.nextInt(25) + 20) * 375;
					break; // Clerical support workers
				case '5':
					salary = (rn.nextInt(30) + 30) * 375;
					break; // Service and sales workers
				case '6':
					salary = (rn.nextInt(30) + 30) * 375;
					break; // Skilled agricultural, forestry and fishery workers
				case '7':
					salary = (rn.nextInt(20) + 30) * 375;
					break; // Craft and related trades workers
				case '8':
					salary = (rn.nextInt(25) + 30) * 375;
					break; // Plant and machine operators and assemblers
				case '9':
					salary = (rn.nextInt(20) + 20) * 375;
					break; // Elementary occupations
				default:
					salary = 0;
					break; // a
				}

				//System.out.println(i++ + ". " + skillId + " - " + jobType + " (" + jobSchedule + ")");
				String query = "insert into Employment.job (job_id, job_code, full_part_time, job_schedule, job_occupation, salary, salary_period, "
					+ "salary_currency, start_date, telework, job_status, job_type, job_contract_type, travel_to_work, job_shift_type, desired_gender, "
					+ "job_hours, hours_period, language) values ("
					+ start
					+ ", '"
					+ job_code
					+ "', '"
					+ part_full_time
					+ "', '"
					+ jobSchedule
					+ "', '"
					+ isco_code
					+ "', '"
					+ salary
					+ "', 'Month', 'SAR', date_add(curdate(), interval "
					+ rn.nextInt(6 * 30)
					+ " day), '"
					+ pickValue(telework, rn.nextInt(totals[12]) + 1)
					+ "', '"
					+ pickValue(statuses, rn.nextInt(totals[3]) + 1)
					+ "', '"
					+ pickValue(jobType, rn.nextInt(totals[5]) + 1)
					+ "', '"
					+ pickValue(contractType, rn.nextInt(totals[4]) + 1)
					+ "', '"
					+ pickValue(travel, rn.nextInt(totals[6]) + 1)
					+ "', '"
					+ jobShift
					+ "', '"
					+ pickValue(genders, rn.nextInt(totals[1]) + 1)
					+ "', '"
					+ hours
					+ "', '"
					+ "Day"
					+ "', '"
					+ pickValue(languages, rn.nextInt(totals[0]) + 1)
					+ "');";
				start++;
				j++;
				//stmtS.executeQuery("SET NAMES 'utf8'");
				//System.out.println(query);
				System.out.print(job_code + " | ");
				stmtS.executeUpdate(query);
				/*
				query = "SELECT ConceptURI, ConceptPT FROM escoskos.skills where ConceptURI in (select hasRelatedConcept from escoskos.occupation_skill where isRelatedConcept='" + ocId + "')" + order ;
				System.out.println(query);
						    	rsS = stmtS.executeQuery(query);
						    	int j = 0;
				while(rsS.next() && j++<(r+2)){
				    skillId = rsS.getString(1);
				    skillId = skillId.substring(skillId.lastIndexOf("/")+1, skillId.length());
				    //System.out.println("\t" + rsS.getString(2));
				    query = "insert into Employment.job_skills (job_id, skill_id) values (" + i + ", '" +skillId + "');";
				    System.out.println("\t" + query);
				    stmtQ.executeUpdate(query);
				}
				rsS.close();
				*/

			}
			j--;
			// updating job title, description, and sector from the taxonomy database
			for (int q=0; batchQueries[q]!=null ; q++)
				stmtQ.executeUpdate(batchQueries[q]);
			rs.close();
			stmt.close();
			stmtS.close();
			stmtQ.close();
			conn.close();
		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
		catch (Exception e)
		{
			//Handle errors for Class.forName
			e.printStackTrace();
		}
		finally
		{
			//finally block used to close resources
			try
			{
				if (stmt != null)
				{
					stmt.close();
				}
			}
			catch (SQLException se2)
			{
			} // nothing we can do
			try
			{
				if (conn != null)
				{
					conn.close();
				}
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
		} //end try
	}//end function
	
	
	/*
	 * Provide statistics about the distribution of the data in the staging area (MySQL)
	 */
	public static void fullStatistics() throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		double counts=0;
		DecimalFormat df = new DecimalFormat("#0.00"); 

		System.out.println(" *** JOBS *** "); 
		rs = stmt.executeQuery("SELECT count(distinct b.JOBPROFILEID)"
				+ " FROM CEMSJOBPROFILE b "
				+ "LEFT JOIN CEMSPROFILESTATUS k ON k.PROFILEID=b.JOBPROFILEID and k.RECORDSTATUS='RST1' "
				+ "and (trim(k.STATUS)='ACTIVE' OR trim(k.STATUS)='NOTSEARCH') "
				);
		for (;rs.next();) {
			counts = rs.getDouble(1);
			System.out.println("Total number of jobs: " + rs.getString(1)); 
		}
		
/*		System.out.println(" - Languages"); 
		rs = stmt.executeQuery("SELECT COMPETENCYTAXONOMYID, count(COMPETENCYTAXONOMYID) counts "
				+ "FROM CEMSPROFILECOMPETENCY "
				+ "WHERE RECORDSTATUS= 'RST1' AND COMPETENCYTAXONOMYID IS NOT NULL "
				+ "AND trim(COMPETENCYTYPE) = 'LANGUAGE' AND RELATEDTYPE = 'JP' "
				+ "group by COMPETENCYTAXONOMYID "
				+ "having count(COMPETENCYTAXONOMYID) >=120 "
				+ "order by counts desc"
				);
		for (;rs.next();) {
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
		}
		
		System.out.println(" - Shift Type"); 
		rs = stmt.executeQuery("d.WORKTIMEMODEL, count(d.WORKTIMEMODEL) counts "
				+ "ROM CEMSPROFILEWORKTIMING d "
				+ "JOIN CEMSJOBPROFILE a ON a.JOBPROFILEID = d.PROFILEID "
				+ "LEFT JOIN CEMSPROFILESTATUS k ON k.PROFILEID=d.PROFILEID and k.RECORDSTATUS='RST1' "
				+ "and (trim(k.STATUS)='ACTIVE' OR trim(k.STATUS)='NOTSEARCH') "
				+ "group by d.WORKTIMEMODEL "
				+ "order by counts desc"
				);
		for (;rs.next();) {
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
		}
		
		if (1==1)
			return;
		System.out.println(" - telework"); 
		rs = stmt.executeQuery("SELECT  telework, COUNT(telework), concat(FORMAT(((COUNT(telework) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(telework) AS iCount FROM employment.job) newJobs GROUP BY telework");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - full_part_time"); 
		rs = stmt.executeQuery("SELECT  full_part_time, COUNT(full_part_time), concat(FORMAT(((COUNT(full_part_time) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(full_part_time) AS iCount FROM employment.job) newJobs GROUP BY full_part_time");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - job_shift_type"); 
		rs = stmt.executeQuery("SELECT  job_shift_type, COUNT(job_shift_type), concat(FORMAT(((COUNT(job_shift_type) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(job_shift_type) AS iCount FROM employment.job) newJobs GROUP BY job_shift_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - telework"); 
		rs = stmt.executeQuery("SELECT  telework, COUNT(telework), concat(FORMAT(((COUNT(telework) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(telework) AS iCount FROM employment.job) newJobs GROUP BY telework");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - full_part_time"); 
		rs = stmt.executeQuery("SELECT  full_part_time, COUNT(full_part_time), concat(FORMAT(((COUNT(full_part_time) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(full_part_time) AS iCount FROM employment.job) newJobs GROUP BY full_part_time");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - job_shift_type"); 
		rs = stmt.executeQuery("SELECT  job_shift_type, COUNT(job_shift_type), concat(FORMAT(((COUNT(job_shift_type) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(job_shift_type) AS iCount FROM employment.job) newJobs GROUP BY job_shift_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - job_contract_type"); 
		rs = stmt.executeQuery("SELECT  job_contract_type, COUNT(job_contract_type), concat(FORMAT(((COUNT(job_contract_type) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(job_contract_type) AS iCount FROM employment.job) newJobs GROUP BY job_contract_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - travel_to_work"); 
		rs = stmt.executeQuery("SELECT  travel_to_work, COUNT(travel_to_work), concat(FORMAT(((COUNT(travel_to_work) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(travel_to_work) AS iCount FROM employment.job) newJobs GROUP BY travel_to_work");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - desired_gender"); 
		rs = stmt.executeQuery("SELECT  desired_gender, COUNT(desired_gender), concat(FORMAT(((COUNT(desired_gender) * 100) / newJobs.iCount),2),'%') FROM   employment.job, (SELECT COUNT(desired_gender) AS iCount FROM employment.job) newJobs GROUP BY desired_gender");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
*/		/************** Job Seeker ***************************/
		System.out.println("\n\n *** Job Seeker *** "); 
		rs = stmt.executeQuery("SELECT count(distinct a.PROFILEID) profiles, count(distinct a.CONCERNROLEID) jobseekers "
				+ "FROM CEMSPROFILE a "
				+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID "
				+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus "
				+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') "
                + "AND a.PROFILETYPE = 'JOBSEEKER'"
				);
		
		for (;rs.next();) {
			counts = rs.getDouble(1);
			System.out.println("Total number of jobseekers: " + rs.getString(1) + " (" + rs.getString(2) + " profiles)"); 
		}
		System.out.println(" - Languages"); 
		rs = stmt.executeQuery("SELECT COMPETENCYTAXONOMYID, COUNT(Relatedid) Counts FROM CEMSPROFILECOMPETENCY where COMPETENCYTYPE='LANGUAGE' and RELATEDTYPE='JSP' GROUP BY COMPETENCYTAXONOMYID order by counts desc");
		for (;rs.next();) 
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
				
		System.out.println(" - gender"); 
		rs = stmt.executeQuery("SELECT trim(b.GENDER) as GENDER, count(b.CONCERNROLEID) " 
					+ "FROM PERSON b " 
					+ "JOIN CEMSPROFILE a on b.CONCERNROLEID=a.CONCERNROLEID " 
             		+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID " 
					+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus " 
					+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') " 
					+ "AND a.PROFILETYPE = 'JOBSEEKER' " 
					+ "group by trim(b.GENDER)"
					);
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
		
		System.out.println(" - desired job"); 
		rs = stmt.executeQuery("SELECT count(a.OCCUPATIONID) " 
					+ "FROM CEMSPROFILE a " 
             		+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID " 
					+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus " 
					+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') " 
					+ "AND a.PROFILETYPE = 'JOBSEEKER' " 
					);
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + " (" 
					+ df.format(rs.getDouble(1)/counts*100.0) + "%)"); 
		
		System.out.println(" - employment status"); 
		rs = stmt.executeQuery("SELECT a.EMPLSTATUSTYPE, count(a.PROFILEID) " 
					+ "FROM PERSON b " 
					+ "JOIN CEMSPROFILE a on b.CONCERNROLEID=a.CONCERNROLEID " 
             		+ "JOIN CEMSPROFILESTATUS z ON a.PROFILEID = z.PROFILEID " 
					+ "AND z.RECORDSTATUS='RST1' and z.status = a.profilestatus " 
					+ "AND (trim(a.PROFILESTATUS)='ACTIVE' or trim(a.PROFILESTATUS)='NOTSEARCH') " 
					+ "AND a.PROFILETYPE = 'JOBSEEKER' " 
					+ "group by a.EMPLSTATUSTYPE"
					);
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
		
		System.out.println(" - Mobility"); 
		rs = stmt.executeQuery("select d.Mobility, count(d.Mobility) counts "
				+ "FROM CEMSPROFILEWORKLOC d "
				+ "JOIN CEMSJOBSEEKERPROFILE a ON a.JOBSEEKERPROFILEID = d.PROFILEID "
				+ "LEFT JOIN CEMSPROFILESTATUS k ON k.PROFILEID=d.PROFILEID and k.RECORDSTATUS='RST1' "
				+ "and (trim(k.STATUS)='ACTIVE' OR trim(k.STATUS)='NOTSEARCH') "
				+ "group by d.Mobility "
				+ "order by counts desc"
				);
		for (;rs.next();) {
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
		}
		
		System.out.println(" - Shift Type"); 
		rs = stmt.executeQuery("select trim(d.WORKTIMEMODEL), count(d.WORKTIMEMODEL) counts "
				+ "FROM CEMSPROFILEWORKTIMING d "
				+ "JOIN CEMSJOBSEEKERPROFILE a ON a.JOBSEEKERPROFILEID = d.PROFILEID "
				+ "LEFT JOIN CEMSPROFILESTATUS k ON k.PROFILEID=d.PROFILEID and k.RECORDSTATUS='RST1' "
				+ "and (trim(k.STATUS)='ACTIVE' OR trim(k.STATUS)='NOTSEARCH') "
				+ "group by d.WORKTIMEMODEL "
				+ "order by counts desc"
				);
		for (;rs.next();) {
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
		}

		System.out.println(" - Education"); 
		rs = stmt.executeQuery("select d.EDUCATIONTYPE, count(d.EDUCATIONTYPE) counts "
				+ "FROM CEMSEDUCATION d "
				+ "JOIN CEMSJOBSEEKERPROFILE a ON a.JOBSEEKERPROFILEID = d.RELATEDID "
				+ "LEFT JOIN CEMSPROFILESTATUS k ON k.PROFILEID=d.RELATEDID and k.RECORDSTATUS='RST1' "
				+ "and (trim(k.STATUS)='ACTIVE' OR trim(k.STATUS)='NOTSEARCH') "
				+ "where d.RECORDSTATUS = 'RST1' AND d.RELATEDTYPE = 'JSP' "
				+ "group by d.EDUCATIONTYPE "
				+ "order by counts desc"
				);
		for (;rs.next();) {
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " profiles (" 
					+ df.format(rs.getDouble(2)/counts*100.0) + "%)"); 
		}

		rs.close();
		stmt.close();
		conn.close();
	}


	/* Picks a random value from an array
	 * 
	 */
	public static int getHours(String value) throws ParseException
	{
		if (value.length()<10)
			return 0;
		SimpleDateFormat format = new SimpleDateFormat("HH:mm");
		int l = (int) (format.parse(value.substring(8,13)).getTime() - format.parse(value.substring(0,5)).getTime())/3600000%24;
		return (l>=0?l:l+24);
	}


}