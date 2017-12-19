/**
 * @author abenabdelkader
 *
 * dataGenerator_ar.java
 * Oct 7, 2015
 */
package com.wccgroup.elise.testdata;

import java.io.*;
//STEP 1. Import required packages
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/*
 * TEST Data generator for jobseekers (Robert Half) based on ESCO occupations and skills
 * data is generated in English and the data distribution 
 * is based on some defauls statistics
 */
public class dataGenerator_gn
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static String trx_table = "";
	static String taxonomyDB = "";
	static String targetDB = "";
	static Map<String, String> jobsDic = new HashMap<String, String>();
	static Map<String, String> jobsLang = new HashMap<String, String>();
	static String[] batchQueries = new String[10];
	static String jobCode; // two columns to hold jobCode and occupation code
	static String occupCode;
	static String occupSRC;
	static String exclude="('0','9')"; // list of occupations to exclude from the data generation; 0: military, 9: low level
	//static int start=1; // start db counter for candidates
	static String country = "US";
	static int population=10000; // max population for cities to include when generating geo coordinates
	static Map<Integer, String> names = new HashMap<Integer, String>();
	public static void main(String[] args) throws SQLException, IOException, InterruptedException
	{
		if (args.length!=3) {
			System.out.println("wrong arguments:\n\tUsage: java -jar dataGenerator_gn.jar NbreOfObjs steps start");
			System.out.println("\t\tNbreOfObjs: number of objects to generate");
			System.out.println("\t\tsteps: number of steps for generating 'NbreOfObjs'");
			System.out.println("\t\tstart: starting number (database start id)");
			return;
		}

		Date date = new Date();
		int jsCounts = Integer.parseInt(args[0]);
		int steps = Integer.parseInt(args[1]);
		int start=Integer.parseInt(args[2]); // start db counter for candidates 

		try
		{
			readProperties(); //load dataGerator properties
			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			String query = "SELECT * FROM " + trx_table + " where (status!=3 and status!=9)";
			System.out.println("\t*****\tGenerating " +  jsCounts + " candidates in " + steps + " Step(s)\t*****");
			System.out.println("\t\tStaging database: " +  DB_URL.substring(0, DB_URL.lastIndexOf("/")));
			generateEmployers(country, population, conn);
			/*			generateData(1000, 1, conn);
			generateAddresses(conn);
			if (1==1)
				return;
			 */			boolean indicator = true;
			 for (int s=1; s<=steps; s++) {
				 Date d = new Date();
				 System.out.println("Step " +  s + "\t(" + jsCounts/steps + " candidates)");
				 fullClean(conn); // cleans the full test data
				 assignNames(conn,jsCounts);
				 generateData(jsCounts/steps, start, conn); // generate test data for jobseeker based on ESCO occupations
				 generateExtraData(conn);
				 System.out.println("\t+++ data generation time: " + (new Date().getTime() - d.getTime())/1000 + "s");
				 d = new Date();
				 while (indicator && s< steps) {
					 ResultSet rs = stmt.executeQuery(query);
					 System.out.println("\t\t waiting for EDR to fully replicate the data .... ");
					 if (rs.next())
						 TimeUnit.SECONDS.sleep(10);
					 else						
						 indicator=false;					
				 }
				 indicator = true;
				 System.out.println("\t+++ EDR transition time: " + (new Date().getTime() - d.getTime())/1000 + "s\n");
				 start += jsCounts/steps;
			 }
			 conn.close();
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		}



		System.out.println("\n\tStarting at: " + date);
		System.out.println("\tFinished at: " + new Date() + "(" + (new Date().getTime() - date.getTime())/1000 + "s)");

		//fullStatistics(); // perform statistics on the data


	}

	public static void generateExtraData(Connection conn) throws ClassNotFoundException
	{
		System.out.println("\t +++ Additonal Data:");
		createCandidateContacts(conn);
		generateAddresses(conn);
		generateCandidateLanguages(conn);
		assignNames(conn,2000);
		generateCandidateEducation(conn);
		GenCandidateSkills(conn);
		GenCandidateCertificates(conn);
		readBatchQueries("jquery_rh");
		executeFinalUpdates();

	}

	public static void generateData(int canCounts, int start, Connection conn) throws SQLException, ClassNotFoundException
	{
		//Date date = new Date();

		jobCode = "job_code";
		occupCode= "occup_code";
		occupSRC = "" + taxonomyDB + ".occupation";
		readBatchQueries("jquery_rh");

		//Generate specific occupations for main demo
		//start = 1;
		/***Generate more generic occupations (usually thousands) ***/
		loadJobs(occupSRC);

		//Generate generic candidates (usually thousands) // based on unemployed people
		start = generateCandidates(15, 29, (int)(canCounts*34/100), start,conn);  // 34% of JS aged between 15 and 29 years
		start = generateCandidates(30, 34, (int)(canCounts*13/100), start,conn);
		start = generateCandidates(35, 39, (int)(canCounts*15/100), start,conn);
		start = generateCandidates(40, 44, (int)(canCounts*13/100), start,conn);
		start = generateCandidates(45, 49, (int)(canCounts*10/100), start,conn);
		start = generateCandidates(50, 54, (int)(canCounts*7/100), start,conn);
		start = generateCandidates(55, 59, (int)(canCounts*5/100), start,conn);
		start = generateCandidates(60, 64, (int)(canCounts*3/100), start,conn);

		//executeFinalUpdates();

		//System.out.println("\n\tStarting at: " + date);
		//System.out.println("\tFinished at: " + new Date());
	}

	/*
	 * This function executes the final updates using pre-defined queries from batchQueries.proprieties
	 */
	public static void executeFinalUpdates()
	{
		Connection conn = null;
		Statement stmt = null;
		System.out.println("\t +++ Applying database batch updates " );
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			for (int q=0; batchQueries[q]!=null ; q++) {
				Date date = new Date();
				System.out.println("\t\t + batch " + (q+1) + ": " +
					stmt.executeUpdate(batchQueries[q]) + " updates (" + (new Date().getTime() - date.getTime())/1000 + "s)");
			}
			conn.close();
			stmt.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/*
	 * This function assigns names to the list of job seekers generated by the program
	 * data is generated in mixed mode in Arabic and English and the data distribution 
	 * is around 50/50. 
	 */
	public static void assignNames(Connection conn, int maxinput)
	{
		Date date = new Date();
		Statement stmt = null;
		try
		{

			maxinput++;
			String[][] Names = new String[maxinput][5];
			stmt = conn.createStatement();

			String query = "";
			query = "SELECT * from " + taxonomyDB + ".name_us";
			ResultSet rs = null;
			rs = stmt.executeQuery(query);
			int i = 0;
			int j = maxinput-1;
			while (i<maxinput/2) {
				while (rs.next())
				{
					if (rs.getString("gender").equalsIgnoreCase("Male"))
					{
						Names[i][0] = rs.getString(2);
						Names[i][1] = rs.getString(3);
						Names[i][2] = rs.getString(4);
						Names[i][3] = rs.getString(5);
						i++;
	
					}
					else
					{
						Names[j][0] = rs.getString(2);
						Names[j][1] = rs.getString(3);
						Names[j][2] = rs.getString(4);
						Names[j][3] = rs.getString(5);
						j--;
	
					}
				}
				rs.beforeFirst();
			}
			i--;
			j++;
			System.out.print("\t\t+ Assing Names using " + (i+j) + " references\t--> " );
			rs.close();
			stmt.close();
			Random rn = new Random();
			int k=0;
			int idx1 =0, idx2 =0;
			while (k<maxinput)
			{
				idx1 = rn.nextInt(maxinput);
				idx2 = rn.nextInt(maxinput);
				names.put(k, "\""+ Names[idx1][0]
					+ "\",\""
					+ Names[idx2][1]
						+ "\", \""
						+ Names[idx1][0] + "." + Names[idx2][1] + "@gmail.com\", '"
						+ Names[idx1][2] + "'");
				k++;

			}
			System.out.println(k + " contacts(" + (new Date().getTime() - date.getTime())/1000 + "s)");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


	/*
	 * This function creates contacts/references for each job seeker
	 */
	public static void createCandidateContacts(Connection conn)
	{
		Date date = new Date();
		try
		{
			Statement stmt = null;
			int maxinput=2000;
			String[][] Names = new String[maxinput][3];
			stmt = conn.createStatement();

			StringBuilder squery = new StringBuilder();
			squery.append("insert into " + targetDB + ".candidate_contacts (first_name, last_name, gender, candidate_id) values ");

			String query = "";
			query = "SELECT * from " + taxonomyDB + ".name_us order by firstname";
			ResultSet rs = null;
			rs = stmt.executeQuery(query);
			int i = 0;
			while (rs.next())
			{
				Names[i][0] = rs.getString("firstname");
				Names[i][1] = rs.getString("lastname");
				Names[i][2] = rs.getString("gender");
				i++;
			}
			i--;
			System.out.print("\t\t+ Contacts using " + i + " references\t--> " );
			Random rn = new Random();
			query = "SELECT candidate_id FROM " + targetDB + ".candidate where candidate_id not in (select candidate_id from " + targetDB + ".candidate_contacts)";
			//String query = "SELECT distinct job_title FROM test.job";
			rs = stmt.executeQuery(query);
			int j=0, k=0;
			while (rs.next())
			{
				j= rn.nextInt(i);
				squery.append("(\"" + Names[j][0] + "\", \"" + Names[j][1] + "\", \""
					+ Names[j][2] + "\", " + rs.getString("candidate_id") + "),");
				k++;
			}
			stmt.executeUpdate(squery.substring(0,squery.length()-1));
			System.out.println(k + " contacts(" + (new Date().getTime() - date.getTime())/1000 + "s)");

			rs.close();
			stmt.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


	/*
	 * Generate companies for the job offering, companies names and sectors are based on the 
	 * top 200 companies in KSA, companies locations are randomly distributed over the most populated cities 
	 * in KSA
	 */
	public static void generateEmployers(String country, int population, Connection conn)
	{
		Date date = new Date();
		Statement stmt = null, stmt2 = null;
		try
		{
			List<String[]> cities = new ArrayList<String[]>();

			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			ResultSet rs = null;
			String query = "SELECT count(*) from " + targetDB + ".employer";
			rs = stmt.executeQuery(query);
			if (rs.next() && !rs.getString(1).equalsIgnoreCase("0"))
				return;

			query = "SELECT * FROM " + taxonomyDB + ".geodata where country='" + country + "' and feature_class='P' and population>" + population;
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			int i = 0;
			while (rs.next())
			{
				cities.add(new String[] { rs.getString("asciname"), rs.getString("latitude"), rs.getString("longitude"), rs.getString("country")});

				i++;
			}
			StringBuilder squery = new StringBuilder();
			squery.append("insert into " + targetDB + ".employer (employer_id, company_name, industry_sector, city, country, latitude, longitude) values ");
			query = "SELECT * from " + taxonomyDB + ".companies where country='" + country + "'";
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
				squery.append("(" + j++ + ", \"" + rs.getString("name") + "\", '"
					+ rs.getString("sector_id_asoc") + "', \""
					+ cities.get(nbre)[0] + "\", '"
					+ cities.get(nbre)[3] + "', "
					+ (Double.parseDouble(cities.get(nbre)[1]) + shift1) + ", "
					+ (Double.parseDouble(cities.get(nbre)[2]) + shift2) + "),");
			}
			//System.out.println(squery);
			stmt2.executeUpdate(squery.substring(0,squery.length()-1));
			System.out.println("\t**\t" + j + " Employers from " + i + " cities (" + (new Date().getTime() - date.getTime())/1000 + "s)");
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
	 * Generates approximate latitude and longitude for the job seekers in KSA
	 * job seekers locations are spread over the major cities in KSA 
	 */
	public static void generateAddresses(Connection conn)
	{
		Date date = new Date();
		Statement stmt = null;
		try
		{
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;
			String[] addressType = {"Home", "Work"};
			List<String[]> cities = new ArrayList<String[]>();


			String query = "SELECT * FROM " + taxonomyDB + ".geodata where country='" + country + "' and feature_class='P' and population>" + population;
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			int i = 0;
			while (rs.next())
			{
				cities.add(new String[] { rs.getString("asciname"), rs.getString("latitude"), rs.getString("longitude"), rs.getString("country")});

				i++;
			}
			Random rn = new Random();
			int nbre = 0;
			double shift1, shift2;


			for (int k=0; k<addressType.length; k++) {
				System.out.print("\t\t+ " + addressType[k] + " addresses using " + i + " cities\t --> ");
				StringBuilder squery = new StringBuilder();
				squery.append("insert into " + targetDB + ".candidate_locations (candidate_id, city, country, geo_latitude, geo_longitude, address_type, distance) values ");

				query = "SELECT candidate_id from " + targetDB + ".candidate where candidate_id not in (select distinct candidate_id from " + targetDB + ".candidate_locations where address_type='" + addressType[k] + "')";
				rs = stmt.executeQuery(query);
				int j=0;
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
					squery.append("('" + rs.getInt(1) + "',\"" + cities.get(nbre)[0] + "\", '"
						+ cities.get(nbre)[3] + "', " + (Double.parseDouble(cities.get(nbre)[1]) + shift1) + ", "
						+ (Double.parseDouble(cities.get(nbre)[2]) + shift2) + ", '" + addressType[k] + "'," + (rn.nextInt(10) * 5 + 25)
						+ "),");
					//System.out.println(query);
					//System.out.print(".");
					j++;
				}
				stmtS.executeUpdate(squery.substring(0,squery.length()-1));
				System.out.println(j + " addresses(" + (new Date().getTime() - date.getTime())/1000 + "s)");
			}
			rs.close();

			//STEP 6: Clean-up environment
			stmt.close();
			stmtS.close();

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

	public static int generateCandidates(int from, int to, int nbre, int start, Connection conn) throws SQLException
	{
		Calendar now = Calendar.getInstance();   // Gets the current date and time
		int year = now.get(Calendar.YEAR); // current year
		int jump = 3;
		String Bdate = ""; // date of birth
		Date date = new Date();
		int age = 20; // working age
		int salary = 9000; // yearly salary
		int hours = 0; //availability hours
		String jobFP = ""; // job full/part time
		String query = "", jobTitle, jobCode;
		StringBuilder squery1 = new StringBuilder();
		StringBuilder squery2 = new StringBuilder();
		StringBuilder squery3 = new StringBuilder();
		int empCounts = 0; //counts of employers
		/*			rs = stmt.executeQuery("select concept, key_, value_ from " + taxonomyDB + ".statistics where objectType='Jobseeker'and status= 'On' order by concept, value_ desc, key_");
		int i = 0;
		String[][][] concept = new String[][][]();
		if (rs.next())
		{
			concept = rs.getString(1);
			String[][] rs.getString(1) = new String[][]();
		}

*/
		String[][] languages = {{"en","10"},{"es","0"}};
		String[][] genders = {{"Male","6"},{"Female","4"},{"X","0"}};
		String[][] driving = {{"YES","7"},{"NO","3"}};
		//String[][] M_Statuses = {{"Married","80"},{"Single","17"},{"Divorced","2"},{"Widowed","1"}};
		String[][] M_Statuses = {{"Single","45"},{"Married","30"},{"Widow","5"},{"Partnership","10"},{"Divorced","5"},{"Defacto","5"}};
		String[][] statuses = {{"Available","60"},{"Not available","10"},{"Available currently working","2"},{"Available currently not working","2"}};
		String[][] nationalities = {{"US","80","ES","5"},{"RF","5"},{"DE","4"},{"BR","4"},{"NL","4"},{"PT","4"},{"CA","14"}};
		String[][] jobFPs = {{"fulltime40","45"},{"fulltime36","25"},{"parttime32","20"},{"parttime24","5"},{"parttime20","3"},{"parttime16","2"}};
		String[][] contractType = {{"contract","45"},{"Permanent","35"},{"Potentialy Permanent","15"},{"Fixed Period","5"}};
		String[][] jobType = {{"Regular","65"},{"EMPDVNAC","5"},{"JOBTRNO","5"},{"Internship","10"},{"Summer Job","10"},{"Mini Job","5"}};
		//String[][] jobShifts = {{"Day","65"},{"Night","20"},{"Two Shifts","15"}};
		String[][] jobShifts = {{"day","60"},{"night","20"},{"rolling","20"},{"rostered","10"}};
		String[][] jobScheduleP = {{"08:00 - 12:00","20"},{"12:00 - 17:00","20"},{"09:00 - 12:00","15"},{"13:00 - 17:00","15"},{"12:00 - 16:00","10"},{"17:00 - 22:00","10"},{"18:00 - 24:00","10"}};
		String[][] jobScheduleF = {{"09:00 - 17:00","20"},{"09:00 - 17:00","20"},{"08:00 - 16:00","15"},{"08:00 - 16:00","15"},{"16:00 - 24:00","10"},{"18:00 - 02:00","10"},{"20:00 - 04:00","10"}};
		//String[][] travel = {{"0%","40"},{"25%","20"},{"50%","15"},{"75%","12"},{"100%","8"}};
		String[][] travel = {{"0%","40"},{"25%","20"},{"50%","15"},{"75%","12"},{"100%","8"}};
		String[][] disabilities = {{"","96"},{"Attention-Deficit","1"},{"Hyperactivity Disorders","1"},{"Blindness","1"},{"Low Vision","1"},{"Blindness","1"},
			{"Brain Injuries","1"},{"Deaf","1"},{"Hard-of-Hearing","1"},{"Learning Disabilities","1"},{"Medical Disabilities","1"},
			{"Speech Disabilities","1"},{"Language Disabilities","1"}};
		String[][] telework = {{"Teleworking","2"},{"No Teleworking","8"}};
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


		Statement stmt = null;
		int i=1;
		try
		{
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;

			rs = stmt.executeQuery("select count(*) from " + targetDB + ".employer");
			if (rs.next())
			{
				empCounts = rs.getInt(1);
			}

			String lang = "";

			System.out.print("\t *** Age: " + from + "-" + to + " \t...");
			//System.out.print("\t- ");
			squery1.append("insert into " + targetDB + ".candidate (candidate_id,birth_date, driving_licence, marital_status, candidate_Status, nationality, "
				+ "work_permit_validity, language,first_name,last_name,email, gender) values ");
			squery2.append("insert into " + targetDB + ".candidate_ambitions (candidate_id,job_code, occupation, availability_date, salary, salary_period, "
				+ "salary_currency, desired_contract_type, desired_job_type, desired_full_part_time, availability_hours, hours_period, "
				+ "travel_to_work, desired_shift_type, telework, commute_distance, language) values ");
			squery3.append("insert into " + targetDB + ".work_experience (candidate_id, job_title_we, isco_code, full_part_time, start_we, end_we, employer_id, employee_id, language) values ");
			for (i = start; i < nbre + start; i++)
			{
				/*				if (i % 10 == 0)
				{
					System.out.print(".");
					// generating date of birth and working experience (only dates) yyyy-mm-dd
				}
				 */
				Random rn = new Random();

				lang = pickValue(languages, rn.nextInt(totals[0]) + 1);

				int d = rn.nextInt(30)+1;// generates a random day between 1 and 29
				int m = rn.nextInt(12)+1;// generates a random month between 1 and 12
				int y = rn.nextInt(to - from +1) + 1;
				y += year - to; // generates a random day between 'from' and 'to'
				age = rn.nextInt(11) + 15; // generates a random working age between '15' and '30'
				if (m==2 && d>28)
					d=28;

				Bdate = y + "-" + m + "-" + d;

				// generating gender type
				//String gender = pickValue(genders, rn.nextInt(totals[1]) + 1);
				String occupation = "";
				occupation = jobsDic.get(String.valueOf(rn.nextInt(jobsDic.size())));
				jobCode = occupation.substring(0, occupation.indexOf("##"));
				//String isco_code = occupation.substring(occupation.indexOf("##") + 2, occupation.length());

				//System.out.println("\n\t\n" + occupation);
				//System.out.println("\n\t\n" + occupation.charAt(occupation.indexOf("##")+2));
				switch (occupation.charAt(occupation.indexOf("##") + 2))
				{
				case '0':
					salary = (rn.nextInt(40) + 40) * 230;
					break; // Armed forces occupations
				case '1':
					salary = (rn.nextInt(30) + 55) * 270;
					break; // Managers
				case '2':
					salary = (rn.nextInt(30) + 50) * 260;
					break; // Professionals
				case '3':
					salary = (rn.nextInt(30) + 45) * 240;
					break; // Technicians and associate professionals
				case '4':
					salary = (rn.nextInt(25) + 30) * 155;
					break; // Clerical support workers
				case '5':
					salary = (rn.nextInt(30) + 30) * 210;
					break; // Service and sales workers
				case '6':
					salary = (rn.nextInt(30) + 30) * 190;
					break; // Skilled agricultural, forestry and fishery workers
				case '7':
					salary = (rn.nextInt(30) + 30) * 175;
					break; // Craft and related trades workers
				case '8':
					salary = (rn.nextInt(30) + 30) * 160;
					break; // Plant and machine operators and assemblers
				case '9':
					salary = (rn.nextInt(30) + 30) * 150;
					break; // Elementary occupations
				default:
					salary = 0;
					break; // a
				}
				salary = salary/4;

				int validity = rn.nextInt(100);
				String days = "'2099-12-31'";
				if (validity < 10)
				{
					days = "date_sub(curdate(), interval " + rn.nextInt(365 * 2) + " day)";
				}
				if (validity < 30)
				{
					days = "date_add(curdate(), interval " + rn.nextInt(365 * 3) + " day)";
				}

				squery1.append("(" 
					+ i
					+ ",'"
					+ Bdate
					/*					+ "', '"
					+ gender
					 */					+ "', '"
					 + pickValue(driving, rn.nextInt(totals[2]) + 1)
					 + "', '"
					 + pickValue(M_Statuses, rn.nextInt(totals[3]) + 1)
					 + "', '"
					 + pickValue(statuses, rn.nextInt(totals[4]) + 1)
					 + "', '"
					 + pickValue(nationalities, rn.nextInt(totals[5]) + 1)
					 + "', "
					 + days
					 + ", '"
					 + lang
					 + "',"
					 + names.get(i-start)
					 + "),");
				jobFP = pickValue(jobFPs, rn.nextInt(totals[6]) + 1);
				hours = jobFP.equalsIgnoreCase("Full-time")?getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]):getHours(jobScheduleP[rn.nextInt(jobScheduleP.length)][0]);

				//stmt.executeUpdate("SET NAMES 'utf8'");
				squery2.append("("
					+ i
					+ ",\""
					+ jobCode
					+ "\", '"
					+ occupation.substring(occupation.indexOf("##") + 2)
					+ "', date_add(curdate(), interval "
					+ rn.nextInt(6 * 30)
					+ " day), "
					+ salary
					+ ", 'monthly', 'US $', '"
					+ pickValue(contractType, rn.nextInt(totals[7]) + 1)
					+ "', '"
					+ pickValue(jobType, rn.nextInt(totals[8]) + 1)
					+ "', '"
					+ pickValue(jobFPs, rn.nextInt(totals[6]) + 1)
					+ "', "
					+ hours
					+ ", 'Week', '"
					+ pickValue(travel, rn.nextInt(totals[12]) + 1)
					+ "', '"
					+ pickValue(jobShifts, rn.nextInt(totals[9]) + 1)
					+ "', '"
					+ pickValue(telework, rn.nextInt(totals[14]) + 1)
					+ "', '"
					+ pickValue(commute_distance, rn.nextInt(totals[15]) + 1)
					+ "', '"
					+ lang
					+ "'),");

				//System.out.println(query);
				//System.out.print("amb-exp");
				//stmt.executeUpdate(query);


				// retrieve list of similar occupations, to create work experience
				query = "SELECT ConceptPT, ConceptPT_en FROM " + taxonomyDB + ".occupation where Parent_ISCOcode='"
					+ occupation.substring(occupation.indexOf("##") + 2) + "'";

				//System.out.println(query);
				int j = 0;
				rs = stmt.executeQuery(query);
				Map<Integer, String> similars = new HashMap<Integer, String>();
				jobCode = occupation.substring(0, occupation.indexOf("##"));
				/*				jobCode = jobsLang.get(jobCode);
				jobCode = (lang.equalsIgnoreCase("en")?jobCode.substring(0, jobCode.indexOf("##")):jobCode.substring(jobCode.indexOf("##")+2, jobCode.length()));
				//System.out.println("\tSimilar occupations for work experience: " + occupation);
				similars.put(0, jobCode);
				 */				j=1;
				 while (rs.next())
				 {
					 similars.put(j++, rs.getString(1));
					 similars.put(j++, rs.getString(2));
				 }
				 rs.close();

				 int exp = year - y - age; ///3;
				 //System.out.println("\t\t -exp1 " + exp);
				 //exp = exp<2?1:exp;
				 //System.out.println("\t\t -y= " + exp);
				 for (j = y + age; j < year; j = j + jump)
				 {

					 jobFP = pickValue(jobFPs, rn.nextInt(totals[6]) + 1);
					 hours = jobFP.equalsIgnoreCase("FULLT")?getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]):getHours(jobScheduleP[rn.nextInt(jobScheduleP.length)][0]);

					 jobTitle = similars.get(rn.nextInt(similars.size()));

					 if (exp < 2)
					 {
						 jump = exp;
					 }
					 else
					 {
						 jump = rn.nextInt(exp / 2) + 1; //jump = ((j+jump)>y) ?year-j:jump;
					 }

					 squery3.append("("
						 + i
						 + ", \""
						 + jobTitle
						 + "\", \""
						 //+ occupation.substring(occupation.indexOf("##") + 2)
						 + occupation.substring(occupation.indexOf("##") + 2)
						 + "\", '"
						 + jobFP
						 + "', '"
						 + j
						 + "-"
						 + (rn.nextInt(12) + 1)
						 + "-"
						 + (rn.nextInt(28) + 1)
						 + "', '"
						 + (j + jump > year ? year : j + jump)
						 + "-"
						 + (rn.nextInt(12) + 1)
						 + "-"
						 + (rn.nextInt(28) + 1)
						 + "', "
						 + (rn.nextInt(empCounts) + 1)
						 + ", "
						 + (rn.nextInt(90000) + 1)
						 + ", '"
						 + lang
						 + "'),");

					 //System.out.println("\t- " + query);
					 //System.out.print(".");
					 //stmt.executeUpdate(query);


				 }
			}
			//System.out.println(squery1.substring(0, squery1.length()-1));
			stmt.executeUpdate(squery1.substring(0, squery1.length()-1));
			stmt.executeUpdate(squery2.substring(0, squery2.length()-1));
			stmt.executeUpdate(squery3.substring(0, squery3.length()-1));



			System.out.println("\t--> " + (i - start) + " candidates created (" + (new Date().getTime() - date.getTime())/1000 + "s)");

			stmt.close();
			stmtS.close();

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
		return i;
	}//end

	public static void GenCandidateSkills()
	{
		Date date = new Date();
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmtC = null;
		try
		{
			Class.forName(JDBC_DRIVER);

			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmtC = conn.createStatement();

			System.out.print("\t\t+ Technical skills \t--> " );

			ResultSet rs = null, rsC;
			int i = 0;
			int c = 0;
			int js_counts=0;
			int skill_counts=0;

			String query = "SELECT candidate_id, job_code, occupation from " + targetDB + ".candidate_ambitions where candidate_id not in (select distinct candidate_id from " + targetDB + ".candidate_skills)";
			//System.out.println(query);
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();

				query = "SELECT hasRelatedConcept skill_id, ConceptPT skill_name FROM " + taxonomyDB + ".occupation_skill," + taxonomyDB + ".skills "
					+ "where hasRelatedConcept=conceptURI and isRelatedConcept= '" + rsC.getString(2) + "'";
				//System.out.println(query);
				rs = stmt2.executeQuery(query);
				for (; rs.next();)
				{
					skills.put(rs.getString(1),rs.getString(2));
					i++;
				}
				rs.close();
				if (skills.size() >= 25) { i = 12;}
				if (skills.size() < 25 && skills.size() >= 20){ i = 10;}
				if (skills.size() <= 15 && skills.size() > 8){ i = 8;}
				if (skills.size() <= 8 && skills.size() > 4){ i = 5;}
				if (skills.size() <= 4){ i = skills.size();}

				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();

				c = 0;

				while (it.hasNext() && c < i)
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into " + targetDB + ".candidate_skills (candidate_id, skill_id, skill_name, skill_type) values ("
						+ rsC.getString(1) + ", '" + entry.getKey() + "', \"" + entry.getValue() + "\", 'skill');";
					stmt.executeUpdate(query);
					c++;

				}
				js_counts++;
				skill_counts+=i;
				/*				System.out.print("[" + rsC.getString(1) + ", " + i + "], ");
				if (js_counts%15==0)
					System.out.println();
				 */				i = 0;
			}
			System.out.println("Total of " + skill_counts + " skills (average of " + skill_counts/js_counts + " skills per jobseeker)(" + (new Date().getTime() - date.getTime())/1000 + "s)");

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
	}//end function

	public static void GenCandidateSkills(Connection conn)
	{
		Date date = new Date();
		try
		{
			Class.forName(JDBC_DRIVER);

			Statement stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();

			System.out.print("\t\t+ Technical skills \t--> " );

			ResultSet rs = null, rs2;
			int i = 0;
			int c = 0;
			int js_counts=0;
			int skill_counts=0;
			String occupation="", tempOccup="";
			StringBuilder squery = new StringBuilder();
			squery.append("insert into " + targetDB + ".candidate_skills (candidate_id, skill_id, skill_name, skill_type) values ");

			String query = "SELECT isRelatedConcept occupation, hasRelatedConcept skill_id, skill_name "
				+ "FROM " + taxonomyDB + ".occupation_skill order by occupation"; 
			rs = stmt.executeQuery(query);


			List<String[]> skills = new ArrayList<String[]>();
			while (rs.next())
			{
				occupation =rs.getString(1);
				skills.clear();
				skills.add(new String[] { rs.getString(2), rs.getString(3)});


				for (; rs.next();)
				{
					if (!occupation.equalsIgnoreCase(rs.getString(1)))
						break;
					skills.add(new String[] { rs.getString(2), rs.getString(3)});
					i++;
				}
				tempOccup = occupation;
				//occupation =rs.getString(1);

				if (skills.size() >= 25) { i = 12;}
				if (skills.size() < 25 && skills.size() >= 20){ i = 10;}
				if (skills.size() <= 15 && skills.size() > 8){ i = 8;}
				if (skills.size() <= 8 && skills.size() > 4){ i = 5;}
				if (skills.size() <= 4){ i = skills.size();}

				query = "SELECT candidate_id, job_code, occupation from " + targetDB + ".candidate_ambitions where job_code='" + tempOccup 
					+ "' and candidate_id not in (select distinct candidate_id from " + targetDB + ".candidate_skills)";
				//System.out.println(query);
				ListIterator<String[]> it = skills.listIterator();

				rs2 = stmt2.executeQuery(query);

				while (rs2.next())
				{
					c = 0;	
					while (it.hasNext() && c < i)
					{
						String[] st = it.next();
						squery.append("(" + rs2.getString(1) + ", '" + st[0]  + "', \"" + st[1] + "\", 'skill'),");
						c++;

					}
					js_counts++;
					skill_counts+=i;
				}
				i = 0;
			}
			stmt3.executeUpdate(squery.substring(0, squery.length()-1));
			System.out.println("Total of " + skill_counts + " skills (average of " + skill_counts/js_counts + " skills per jobseeker)(" + (new Date().getTime() - date.getTime())/1000 + "s)");

			stmt.close();
		}
		catch (SQLException se)
		{
			se.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}//end function

	public static void GenCandidateCertificates(Connection conn)
	{
		Date date = new Date();
		Statement stmt = null, stmt2 = null, stmtC = null;
		try
		{
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmtC = conn.createStatement();

			System.out.print("\t\t+ Certificates \t--> " );

			ResultSet rs = null, rsC;
			int i = 0;
			int c = 0;
			int js_counts=0;
			int skill_counts=0;

			String query = "SELECT candidate_id, job_code, occupation from " + targetDB + ".candidate_ambitions where candidate_id not in (select distinct candidate_id from " + targetDB + ".candidate_certificates)";
			StringBuilder squery = new StringBuilder();
			squery.append("insert into " + targetDB + ".candidate_certificates (candidate_id, certificate_id, certificate_type, validity_date) values ");
			//System.out.println(query);
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();

				query = "SELECT hasRelatedConcept certificatel_id FROM " + taxonomyDB + ".occupation_qualification "
					+ "where isRelatedConcept= '" + rsC.getString(2) + "'";
				//System.out.println(query);
				rs = stmt2.executeQuery(query);
				String skillId = "";
				for (; rs.next();)
				{
					skillId = rs.getString(1);
					skills.put(skillId,skillId);
					i++;
				}
				rs.close();
				if (skills.size() >= 25) { i = 12;}
				if (skills.size() < 25 && skills.size() >= 20){ i = 10;}
				if (skills.size() <= 15 && skills.size() > 8){ i = 8;}
				if (skills.size() <= 8 && skills.size() > 4){ i = 5;}
				if (skills.size() <= 4){ i = skills.size();}

				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();

				c = 0;

				while (it.hasNext() && c < i)
				{
					Map.Entry<String, String> entry = it.next();
					squery.append("(" + rsC.getString(1) + ", '" + entry.getKey() + "', 'certificate', current_date()),");
					c++;

				}
				js_counts++;
				skill_counts+=i;
				i = 0;
			}
			stmt.executeUpdate(squery.substring(0, squery.length()-1));
			System.out.println("Total of " + skill_counts + " certificates (average of " + skill_counts/js_counts + " certificates per jobseeker) (" + (new Date().getTime() - date.getTime())/1000 + "s)");

			stmt.close();
		}
		catch (SQLException se)
		{
			se.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}//end function

	/*
	 * Generate Languages for the job seekers  
	 * based on statistics which are specific to KSA 
	 */
	public static void generateCandidateLanguages(Connection conn)
	{
		Date date = new Date();
		String[][] languages = {{"English","70"},{"Spanish","10"},{"French","5"},{"Porteguese","5"},{"German","5"},{"Dutch","5"}};
		String[][] langLevel_1 = {{"Advanced","70"},{"Intermediate","30"}};
		String[][] langLevel_2 = {{"Advanced","30"},{"Intermediate","50"},{"Beginner","20"}};
		int[] totals = {0,0,0}; // to hold the totals for each array
		System.out.print("\t\t+ Language skills \t\t\t--> " );

		for (int i=0; i<languages.length; i++)
			totals[0] = totals[0] + Integer.parseInt(languages[i][1]);

		for (int i=0; i<langLevel_1.length; i++)
			totals[1] = totals[1] + Integer.parseInt(langLevel_1[i][1]);

		for (int i=0; i<langLevel_2.length; i++)
			totals[2] = totals[2] + Integer.parseInt(langLevel_2[i][1]);

		Map<String, String> canLanguages = new HashMap<String, String>(); // dictionary to store candidate languages
		StringBuilder squery = new StringBuilder();
		squery.append("insert into " + targetDB + ".candidate_languages (candidate_id, language_id, level) values ");

		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;

			String cId, lang, level = "";
			// Generate Candidate Languages
			rs = stmt.executeQuery(
				"SELECT Candidate_id, nationality FROM " + targetDB + ".candidate where candidate_id not in (select distinct candidate_id from " + targetDB + ".candidate_languages)");

			int j=0;
			while (rs.next())
			{
				cId = rs.getString(1);
				Random rn = new Random();
				lang = pickValue(languages, rn.nextInt(totals[0]) + 1);
				level = pickValue(langLevel_1, rn.nextInt(totals[1]) + 1);
				canLanguages.put(lang, level);

				int rr = rn.nextInt(3);
				for (int l = 0; l < rr; l++)
				{
					lang = pickValue(languages, rn.nextInt(totals[0]) + 1);
					level = pickValue(langLevel_2, rn.nextInt(totals[2]) + 1);
					canLanguages.put(lang, level);
				}
				Iterator<Map.Entry<String, String>> it = canLanguages.entrySet().iterator();
				while (it.hasNext())
				{
					Map.Entry<String, String> entry = it.next();
					squery.append("(" + cId + ", '" + entry.getKey() + "', '"
						+ entry.getValue() + "'),");
					j++;
				}
				canLanguages.clear();
			}
			stmtS.executeUpdate(squery.substring(0, squery.length()-1));
			System.out.println(j + " language skills (" + (new Date().getTime() - date.getTime())/1000 + "s)");

			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			stmtS.close();

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
	 * This function loads the a list of jobs into a dictionary, so that it becomes easy to randomly pick jobs when automatically generating the data
	 * @param trg_occup if a target occupation is provided, this modules only loads similar jobs to the specified occupation (level 5)
	 * if occupation is not specified (null), all the jobs in the taxonomy will be loaded
	 * @param occupSRC: source of taxonomy occupations
	 * @return void
	 */
	public static void loadJobs(String occupSRC)
	{
		Connection conn = null;
		Statement stmt = null;
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			ResultSet rs;

			//String query = "SELECT conceptURI jobCode, Parent_ISCOcode occupCode from escoskos.occupation_es where Parent_ISCOcode in (select left(occup_code,4) from asoc3.occupation where occup_title_es is not null)";
			//String query2= "SELECT conceptURI jobCode, ConceptPT_en job_title_en, ConceptPT  job_title_es from escoskos.occupation_es where Parent_ISCOcode in (select left(occup_code,4) from asoc3.occupation where occup_title_es is not null)";
			String query = "SELECT conceptURI jobCode, Parent_ISCOcode occupCode from " + occupSRC + " where Parent_ISCOcode !=''";
			String query2= "SELECT conceptURI jobCode, ConceptPT_en job_title_en from " + occupSRC + " where Parent_ISCOcode  !=''";
			//System.out.println("\n" + query);
			rs = stmt.executeQuery(query);

			int i = 0;
			jobsDic.clear();
			while (rs.next())
				jobsDic.put(String.valueOf(i++), rs.getString(1) + "##" + rs.getString(2));

			rs = stmt.executeQuery(query2);

			jobsLang.clear();
			while (rs.next())
			{
				jobsLang.put(rs.getString(1), rs.getString(2));

			}
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
	}//end function

	public static void generateCandidateEducation(Connection conn) throws ClassNotFoundException
	{ //synonyms and similars from Janzz
		Date date = new Date();
		Statement stmt = null;
		Statement stmt2 = null;
		Statement stmt3 = null;
		String query = "";
		//System.out.println("Generating Candidate Education data:");
		try
		{
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmt3 = conn.createStatement();
			ResultSet rs, rs2;

			StringBuilder squery = new StringBuilder();
			squery.append("insert into " + targetDB + ".candidate_education (candidate_id, education_level, education_name, education_area, education_field) values ");

			System.out.print("\t\t+ Education \t\t--> " );

			//stmt3.executeUpdate("delete from " + targetDB + ".candidate_education");

			rs = stmt.executeQuery(
				"SELECT candidate_id, job_title, occupation, sector, 'en' language FROM " + targetDB + ".candidate_ambitions where candidate_id not in (select candidate_id from " + targetDB + ".candidate_education)");

			int j = 0;
			while (rs.next())
			{
				//System.out.println(rs.getString(1) + ": " + rs.getString(2));
				query = "SELECT oe.occup_code, e.code, e.title_" + rs.getString("language") + " FROM " + taxonomyDB + ".occupationeducation_asoc oe, " + taxonomyDB + ".education_asoc e where oe.edu_code=e.code"
					+ " and oe.occup_code like \"" + rs.getString(3) + "%\"";
				//System.out.print(".");
				//System.out.println(query);
				rs2 = stmt2.executeQuery(query);

				if (rs2.next())
				{
					squery.append("(\"" + rs.getString(1) + "\",\"");
					squery.append((rs2.getString(2).length()>=2)?rs2.getString(2).substring(0, 2):"");
					squery.append("\",\"");
					squery.append((rs2.getString(2).length()>=4)?rs2.getString(2).substring(0, 4):"");
					squery.append("\",\"");
					squery.append(rs2.getString(3));
					squery.append("\",\"");
					squery.append((rs2.getString(2).length()>=6)?rs2.getString(2):"");
					squery.append("\"),");

					j++;

				}
			}
			//System.out.println(squery.substring(0, squery.length()-1));
			stmt3.executeUpdate(squery.substring(0, squery.length()-1));

			System.out.println(j + " education (" + (new Date().getTime() - date.getTime())/1000 + "s)");
			rs.close();
			stmt.close();
			stmt2.close();
			stmt3.close();
		}
		catch (SQLException se)
		{
			se.printStackTrace();
		} //end finally try
	}//end function


	/* Full clean of the staging database
	 * allows to re-generate the data
	 */
	public static void fullClean(Connection conn) throws ClassNotFoundException, SQLException
	{
		Date d = new Date();
		String[] queries = {
			"delete FROM " + targetDB + ".work_experience",
			/*			"delete FROM " + targetDB + ".employer",
			*/
			"delete FROM " + targetDB + ".job_education",
			"delete FROM " + targetDB + ".job_certificates",
			"delete FROM " + targetDB + ".job_skills",
			"delete FROM " + targetDB + ".job",
			"Delete from " + targetDB + ".candidate_related",
			"Delete from " + targetDB + ".candidate_children",
			"Delete from " + targetDB + ".candidate_disabilities",
			 			
			"delete FROM " + targetDB + ".candidate_contacts",
			 "delete FROM " + targetDB + ".candidate_education",
			 "delete FROM " + targetDB + ".candidate_certificates",
			 "Delete from " + targetDB + ".candidate_skills",
			 "Delete from " + targetDB + ".candidate_languages",
			 "Delete from " + targetDB + ".candidate_education",
			 "Delete from " + targetDB + ".candidate_ambitions",
			 "Delete from " + targetDB + ".candidate_locations",
			 "delete FROM " + targetDB + ".candidate",
			 "delete FROM " + targetDB + ".branch",
		};

		Statement stmt = conn.createStatement();
		for (int i = 0; i < queries.length-1; i++)
		{
			stmt.executeUpdate(queries[i]);
			//System.out.println(queries[i] + ": \n\t--> " + stmt.executeUpdate(queries[i]) + " records deleted");
		}
		System.out.println("\t+++ Cleaning processed data: " + stmt.executeUpdate(queries[queries.length-1]) + " objects in " + (new Date().getTime() - d.getTime())/1000 + "s\n");
		stmt.close();

	}

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

		/*		System.out.println(" - telework"); 
		rs = stmt.executeQuery("SELECT  telework, COUNT(telework), concat(FORMAT(((COUNT(telework) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(telework) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY telework");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		System.out.println(" - full_part_time"); 
		rs = stmt.executeQuery("SELECT  full_part_time, COUNT(full_part_time), concat(FORMAT(((COUNT(full_part_time) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(full_part_time) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY full_part_time");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		System.out.println(" - job_shift_type"); 
		rs = stmt.executeQuery("SELECT  job_shift_type, COUNT(job_shift_type), concat(FORMAT(((COUNT(job_shift_type) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(job_shift_type) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY job_shift_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		System.out.println(" - telework"); 
		rs = stmt.executeQuery("SELECT  telework, COUNT(telework), concat(FORMAT(((COUNT(telework) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(telework) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY telework");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		System.out.println(" - full_part_time"); 
		rs = stmt.executeQuery("SELECT  full_part_time, COUNT(full_part_time), concat(FORMAT(((COUNT(full_part_time) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(full_part_time) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY full_part_time");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		System.out.println(" - job_shift_type"); 
		rs = stmt.executeQuery("SELECT  job_shift_type, COUNT(job_shift_type), concat(FORMAT(((COUNT(job_shift_type) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(job_shift_type) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY job_shift_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		System.out.println(" - job_contract_type"); 
		rs = stmt.executeQuery("SELECT  job_contract_type, COUNT(job_contract_type), concat(FORMAT(((COUNT(job_contract_type) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(job_contract_type) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY job_contract_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		System.out.println(" - travel_to_work"); 
		rs = stmt.executeQuery("SELECT  travel_to_work, COUNT(travel_to_work), concat(FORMAT(((COUNT(travel_to_work) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".job, (SELECT COUNT(travel_to_work) AS iCount FROM " + targetDB + ".job) newJobs GROUP BY travel_to_work");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 

		 */		
		/************** Job Seeker ***************************/
		System.out.println("\n\n *** Job Seeker *** "); 
		rs = stmt.executeQuery("select count(*) FROM " + targetDB + ".candidate");
		for (;rs.next();)
			System.out.println("Total number of jobseekers: " + rs.getString(1)); 

		System.out.println(" - Languages"); 
		rs = stmt.executeQuery("SELECT  language, COUNT(language) AS `Count`, concat(FORMAT(((COUNT(language) * 100) / newJobs.iCount),2),'%') AS `Percentage` FROM   " + targetDB + ".candidate, (SELECT COUNT(language) AS iCount FROM " + targetDB + ".candidate) newJobs GROUP BY language");
		for (;rs.next();) 
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " candidates - " + rs.getString(3)); 

		System.out.println(" - driving_licence"); 
		rs = stmt.executeQuery("SELECT  driving_licence, COUNT(driving_licence), concat(FORMAT(((COUNT(driving_licence) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".candidate, (SELECT COUNT(driving_licence) AS iCount FROM " + targetDB + ".candidate) newJobs GROUP BY driving_licence");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 

		System.out.println(" - marital_status"); 
		rs = stmt.executeQuery("SELECT  marital_status, COUNT(marital_status), concat(FORMAT(((COUNT(marital_status) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".candidate, (SELECT COUNT(marital_status) AS iCount FROM " + targetDB + ".candidate) newJobs GROUP BY marital_status");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 

		System.out.println(" - nationality"); 
		rs = stmt.executeQuery("SELECT  nationality, COUNT(nationality), concat(FORMAT(((COUNT(nationality) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".candidate, (SELECT COUNT(nationality) AS iCount FROM " + targetDB + ".candidate) newJobs GROUP BY nationality");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 

		System.out.println(" - gender"); 
		rs = stmt.executeQuery("SELECT  gender, COUNT(gender), concat(FORMAT(((COUNT(gender) * 100) / newJobs.iCount),2),'%') FROM   " + targetDB + ".candidate, (SELECT COUNT(gender) AS iCount FROM " + targetDB + ".candidate) newJobs GROUP BY gender");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 

		rs.close();
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

			input = new FileInputStream("dataGenerator_gn.properties");

			// load a properties file
			prop.load(input);
			JDBC_DRIVER = prop.getProperty("driver");
			DB_URL = prop.getProperty("url");
			USER = prop.getProperty("user");
			PASS = prop.getProperty("pass");
			trx_table = prop.getProperty("trx_table");
			taxonomyDB = prop.getProperty("taxonomyDB");
			targetDB = prop.getProperty("targetDB");

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

			input = new FileInputStream("batchQueries.properties");
			prop.load(input);
			for (int i=0; i<10; i++)
				batchQueries[i] = prop.getProperty(type+i);

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