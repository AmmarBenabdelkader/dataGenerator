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
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/*
 * TEST Data generator for jobs and jobseekers based on ASOC MOL list of occupations
 * data is generated in mixed mode in Arabic and English and the data distribution 
 * is based on statistics that are specific to HRDF project
 */
public class dataGenerator_Dubai_copy
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Map<String, String> jobsDic = new HashMap<String, String>();
	static Map<String, String> jobsLang = new HashMap<String, String>();
	static Map<Integer, String> zipcodes = new HashMap<Integer, String>();
	static Map<Integer, String> disabilityList = new HashMap<Integer, String>();
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
		options.add("4");
		options.add("5");
		options.add("6");
		options.add("7");
		options.add("8");

		while (true)
		{
			System.out.printf("Please select the action to perform from the following:\n");
			System.out.println("\t- 1- Generate test data for UAE (New Asoc)");
			System.out.println("\t- 2- Generate Lists for UAE (New Asoc)");
			System.out.println("\t- 4- Clean data (full clean)");
			System.out.println("\t- 5- Clean data (only generic part)");
			System.out.println("\t- 6- Clean data (only specific part)");
			System.out.println("\t- 7- Statistics");
			System.out.println("\t- 8- generate job education");
			System.out.println("\t- 9- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			generateDataAsocNew(0, 500); // generate test data for based on ASOC Job title index
			break;
		case "2":
			generateLists_UAE();
			break;
		case "4":
			cleanFull(); // cleans the full test data
			break;
		case "5":
			cleanGeneric(101); // clean test data from 101 (included)
			break;
		case "6":
			cleanSpecific(1, 100); // clean test data from 1 to 100 (included)
			break;
		case "7":
			fullStatistics(); // perform statistics on the data
			break;
		case "8":
			//generateJobEducation();
			//generateCandidateEducation();
			//GenCandidateSkills();
			//GenJobSkills();
			//updateASOCSkills();
			generateJobLanguages2();
			break;
		default:
			System.out.println("Bye"); // quit

		}

	}

	public static void generateDataAsocNew(int jobCounts, int canCounts) throws SQLException, ClassNotFoundException
	{
		Date date = new Date();
		int sCounts = 10; // number of jobs to generate per occupation
		//int[][] occupations = { { 235601, sCounts } , { 251102, sCounts }, { 251202, sCounts }, { 351301, sCounts } , { 251401, sCounts } };  
		int[][] occupations = { { 216102, sCounts } , { 343505, sCounts }, { 121104, sCounts }, { 121114, sCounts } , { 134909, sCounts } };  
		jobCode = "job_code";
		occupCode= "occup_code";
		occupSRC = "asoc.occupation";
		readBatchQueries("jquery_asoc");

		//Generate specific occupations for main demo
		int i = 0;
		from_ = 1;
	/*	for (i = 0; i < occupations.length; i++)
		{
			generateJobs(occupations[i][0], occupations[i][1], occupSRC); // generate jobs
		}
*/		to_ = i*sCounts;
		/***Generate more generic occupations (usually thousands) ***/
		generateJobs(0, jobCounts, occupSRC); // generate jobs
		//GenerateJobCompanies_ar();
		//assignJobCompany_ar();
		//readBatchQueries("cquery_asoc");
		
		//Generate specific ~100 candidates for main demo
		generateCandidates(15, 64, to_, "specific");
		
		//Generate generic candidates (usually thousands) // based on unemployed people
		generateCandidates(15, 29, (int)(canCounts*34/100), "generic");
		generateCandidates(30, 34, (int)(canCounts*12/100), "generic");
		generateCandidates(35, 39, (int)(canCounts*14/100), "generic");
		generateCandidates(40, 44, (int)(canCounts*12/100), "generic");
		generateCandidates(45, 49, (int)(canCounts*9/100), "generic");
		generateCandidates(50, 54, (int)(canCounts*6/100), "generic");
		generateCandidates(55, 59, (int)(canCounts*4/100), "generic");
		generateCandidates(60, 64, (int)(canCounts*2/100), "generic");
		
		generateCandidateLanguages();
		generateChildren();

		AssignNames_ar();
		generateAddresses("Home", "AE", 3000);
		generateAddresses("Work", "AE", 3000);
/*
		//generateCandidateEducation();
		generateJobEducation();
		
*/		
		
/*		
		//generateJobLanguages();
		//GenCandidateSkills();

*/		readBatchQueries("gquery_asoc");
		executeFinalUpdates();
		
		System.out.println("\n\tStarting at: " + date);
		System.out.println("\tFinished at: " + new Date());
	}

	/*
	 * This function executes the final updates using pre-defined queries from batchQueries.proprieties
	 */
	public static void executeFinalUpdates()
	{
		Connection conn = null;
		Statement stmt = null;
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			for (int q=0; batchQueries[q]!=null ; q++) {
				//System.out.println(batchQueries[q]);
				stmt.executeUpdate(batchQueries[q]);
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
	public static void AssignNames_ar()
	{
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		try
		{

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			int maxinput=190;
			String[][] Names = new String[maxinput][7];
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();

			String query = "";
			query = "SELECT * from asoc.names_ar";
			ResultSet rs = null;
			rs = stmt.executeQuery(query);
			int i = 0;
			int j = maxinput-1;
			while (rs.next())
			{
				if (rs.getString("gender").equalsIgnoreCase("Male"))
				{
					Names[i][0] = rs.getString(2);
					Names[i][1] = rs.getString(3);
					Names[i][2] = rs.getString(4);
					Names[i][3] = rs.getString(8);
					Names[i][4] = rs.getString(9);
					Names[i][5] = rs.getString(10);
					Names[i][6] = rs.getString(5);
					System.out.println ("M: " + Names[i][0] + "\ti=" + i );
					i++;
					
				}
				else
				{
					Names[j][0] = rs.getString(2);
					Names[j][1] = rs.getString(3);
					Names[j][2] = rs.getString(4);
					Names[j][3] = rs.getString(8);
					Names[j][4] = rs.getString(9);
					Names[j][5] = rs.getString(10);
					Names[j][6] = rs.getString(5);
					System.out.println ("\tF: " + Names[j][0] + "\tj=" + j );
					j--;
					
				}
					}
			i--;
			j++;
			System.out.println ("i=" + i + "\tj=" +j + "\tName: " + Names[i][0]);
			Random rn = new Random();
			int nbre = 0;
			query = "SELECT candidate_id, language, gender FROM emp_dubai.candidate";
			//String query = "SELECT distinct job_title FROM test.job";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				if (rs.getString("gender").equalsIgnoreCase("2"))
					nbre = rn.nextInt(i)+1;
				else
					nbre = rn.nextInt((maxinput-j))+i+1;
					
				if (rs.getString("language").equalsIgnoreCase("ar"))
				{
					stmt2.executeUpdate(
						"Update emp_dubai.Candidate set first_name=\""
							+ Names[nbre][0]
							+ "\", middle_name= \""
							+ Names[rn.nextInt(maxinput)][1]
							+ "\", last_name=\""
							+ Names[rn.nextInt(maxinput)][2]
							+ "\" where candidate_id="
							+ rs.getString("candidate_id"));
				}
				else
				{
					stmt2.executeUpdate(
						"Update emp_dubai.Candidate set first_name='"
							+ Names[nbre][3]
							+ "', middle_name= '"
							+ Names[rn.nextInt(j)][4]
							+ "', last_name='"
							+ Names[rn.nextInt(j)][5]
							+ "' where candidate_id="
							+ rs.getString("candidate_id")
							+ ";\n");
				}

			}
			//writer.close();
			//System.out.println (new Date());
			//file.close();
			//workbook.close();
			conn.close();
			stmt.close();
			rs.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
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
			query = "select candidate_id, birth_date, timestampdiff(year,birth_date, curdate()) age FROM emp_dubai.candidate where candidate_id not in (select distinct candidate_id from emp_dubai.candidate_education)";
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
						query = "insert into emp_dubai.candidate_education (candidate_id, education_name, education_level) values ("
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
			String query = "SELECT count(*) from emp_dubai.employer";
			rs = stmt.executeQuery(query);
			if (rs.next() && !rs.getString(1).equalsIgnoreCase("0"))
				return;

			//System.out.println (new Date());
			query = "SELECT * FROM taxonomies.geoname where country='AE' and feature_class='P' and population>3000";
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
			query = "SELECT * from taxonomies.companies where country='AE'";
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
				query = "insert into emp_dubai.employer (employer_id, company_name, industry_sector, city, country, latitude, longitude) values ("
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
			//query = "update emp_dubai.employer a set industry_sector=(select sector_id from asoc.sectors b where a.industry_sector=b.name_en) where a.industry_sector in (select name_en from asoc.sectors)";
			//stmt2.executeUpdate(query);
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
			String query = "update emp_dubai.job a set job_sector = (select sector from taxonomies.sector_ar b where left(a.job_occupation,6)=b.occup_code) where language='ar'";
			stmt2.executeUpdate(query);

			query = "SELECT distinct industry_sector FROM emp_dubai.employer";
			rs = stmt.executeQuery(query);
			while (rs.next())
				EmpDic.put(rs.getString("industry_sector"), "");
			
			query = "SELECT employer_id, industry_sector FROM emp_dubai.employer order by industry_sector, employer_id";
			rs = stmt.executeQuery(query);
			//String strValue;
			while (rs.next()) {
				//strValue = EmpDic.get(rs.getString("job_sector"));
				EmpDic.put(rs.getString("industry_sector"), EmpDic.get(rs.getString("industry_sector")) + " " + rs.getString("employer_id"));
			}
			
			Random rn = new Random();

			query = "SELECT count(*) from emp_dubai.employer";
			rs = stmt.executeQuery(query);
			int counts = 0;
			if (rs.next())
			{
				counts = rs.getInt(1);
			}

			String employers[] = null;
			query = "SELECT job_id, job_title, job_sector from emp_dubai.job";
			rs = stmt.executeQuery(query);
			rn = new Random();
			String strValue = "";
			while (rs.next())
			{
				System.out.println ("job " + rs.getString("job_id") + "\tSector: " + rs.getString("job_sector"));
				if (EmpDic.get(rs.getString("job_sector"))!=null)
					strValue = EmpDic.get(rs.getString("job_sector"));
				else
					strValue = EmpDic.get("Cross Sector");
				
				if (strValue!=null) {
					//continue;
				
				System.out.println ("job " + rs.getString("job_id") + "\tSector: " + rs.getString("job_sector") +
					"\t- '" + strValue + "'");
				strValue = strValue.substring(1, strValue.length());
				employers = strValue.split(" ");
					
				query = "update  emp_dubai.job set employer_id='"
					+ (employers[rn.nextInt(employers.length)])
					//+ (rn.nextInt(counts) + 1)
					+ "' where job_id='"
					+ rs.getString("job_id")
					+ "'";
				System.out.println (query + rs.getString("job_sector"));
				stmt2.executeUpdate(query);
				}
			}
			// change the sector back (to Arabic) for job titles in Arabic
			query = "update emp_dubai.job a set job_sector = (select sector_ar from taxonomies.sector_ar b where left(a.job_occupation,6)=b.occup_code) where language='ar'";
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

			query = "SELECT job_id disc_title_en, proficiency_title_en from asoc.occup_languages, emp_dubai.job where left(job_occupation,5)=left(occup_code,5)";
			rs = stmt.executeQuery(query);

			while (rs.next())
			{
				query = "insert into emp_dubai.job_languages (job_code, language_name, language_level) values ('"
					+ rs.getString(1)
					+ "', '"
					+ rs.getString(2)
					+ "', '"
					+ rs.getString(3)
					+ "')";
				System.out.println("\t\t- " + query);
				System.out.print(".");
				stmt2.executeUpdate(query);

			}

			rs.close();

			stmt2.executeUpdate("update emp_dubai.job_languages set language_level='Beginner' where language_level='Basic'");
			stmt2.executeUpdate("update emp_dubai.job_languages set language_level='Advanced' where language_level='Expert'");
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

	public static void generateJobEducation_2() throws ClassNotFoundException
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
				query = "insert into emp_dubai.job_education (job_id, education_level, education_name) values ('"
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
				stmt2.executeUpdate("update emp_dubai.job_education set education_level='" + edu[i][1] + "' where education_level='" + edu[i][0] + "'");
			}
			rs.close();

			stmt2.executeUpdate("update emp_dubai.job_education set edu_discipline=(select distinct disc_title_en from asoc.discipline where occup_code=job_id limit 1)");
			stmt2.executeUpdate("update emp_dubai.job_education set edu_field=(select distinct field_title_en from asoc.discipline where occup_code=job_id limit 1)");
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
	 * Generate children for job seekers based on the age of the persons
	 * The number of kids is random and in accordance with age, for each kid we also 
	 * generate the date of birth and a gender
	 */
	public static void generateChildren()
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
			int age = 0, nbre = 0, cId = 0, Byear = 0;
			query = "SELECT candidate_id, birth_date, timestampdiff(year,birth_date, curdate()) age, year(birth_date) Byear FROM emp_dubai.candidate where marital_status!='Single' and candidate_id not in (select distinct candidate_id from emp_dubai.candidate_children)";
			//System.out.println("\t- " + query);
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			while (rs.next())
			{
				age = rs.getInt("age");
				cId = rs.getInt("candidate_id");
				Byear = rs.getInt("Byear");
				if (age > 40)
				{
					nbre = rn.nextInt(4);
				}
				else if (age > 35)
				{
					nbre = rn.nextInt(3);
				}
				else if (age > 29)
				{
					nbre = rn.nextInt(2);
				}
				else if (age > 22)
				{
					nbre = rn.nextInt(1);
				}
				//System.out.println("\tAge " + age + " --> Childeren: " + nbre);
				if (nbre == 0)
				{
					continue;
				}
				for (int i = 1; i <= nbre; i++)
				{
					//System.out.println("\t- " + query);
					Byear = Byear + 20 + rn.nextInt(4) + 1;
					query = "insert into emp_dubai.candidate_children (candidate_id, child_dob, child_gender) values ("
						+ cId
						+ ", '"
						+ Byear
						+ "-"
						+ (rn.nextInt(12) + 1)
						+ "-"
						+ (rn.nextInt(28) + 1)
						+ "', '"
						+ (rn.nextInt(5) >= 2 ? "F" : "M")
						+ "')";
					//System.out.println("\t\t- " + query);
					stmt2.executeUpdate(query);

				}
			}
			rs.close();
			System.out.println("\n *** Generating candidate_childeren ***  ");

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

			query = "SELECT candidate_id from emp_dubai.candidate where candidate_id not in (select distinct candidate_id from emp_dubai.candidate_locations)";
			//query = "SELECT candidate_id from emp_dubai.candidate where zipcode is null and geo_latitude is null";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			for (; rs.next();)
			{
				String[] addresses = zipcodes.get(rn.nextInt(j - 1) + 1).split("##");
				query = "insert into emp_dubai.candidate_locations values ('"
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
	public static void generateAddresses(String addressType, String country, Integer population)
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
			String query = "SELECT * FROM taxonomies.geoname where country='" + country + "'and feature_class='P' and population>" + population;
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
			query = "SELECT candidate_id from emp_dubai.candidate where candidate_id not in (select distinct candidate_id from emp_dubai.candidate_locations where address_type='" + addressType + "')";
			//query = "SELECT candidate_id from emp_dubai.candidate where zipcode is null and geo_latitude is null";
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
				query = "insert into emp_dubai.candidate_locations (candidate_id, city, country, geo_latitude, geo_longitude, address_type, distance) values ('"
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

			query = "SELECT candidate_id from emp_dubai.candidate where zipcode is null";
			//query = "SELECT candidate_id from emp_dubai.candidate where zipcode is null and geo_latitude is null";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			for (; rs.next();)
			{
				String[] addresses = zipcodes.get(rn.nextInt(j - 1) + 1).split(" ");
				System.out.println(addresses[0]);
				query = "update emp_dubai.candidate set zipcode ='"
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
	
	/*
	 *  The generateCandidates method generates candidates with random values for occupation, driving license, gender, marital status, and disabilities
	 *  It also generates working experience for the candidate based on his/her age and based on current occupation
	 */

	public static void generateCandidates(int from, int to, int nbre, String genType) throws SQLException
	{
		int jump = 3;
		String Bdate = ""; // date of birth
		//Date date = new Date();
		int year = 2015; // current year
		int age = 20; // working age
		int salary = 9000; // yearly salary
		int start = 1; // start counter for candidate Id
		int hours = 0; //availability hours
		String jobFP = ""; // job full/part time
		String jobShift = ""; // job Shift Type
		String query = "", jobTitle, jobCode, ocId;
		int empCounts = 0; //counts of employers
		
		String[][] languages = {{"en","6"},{"ar","4"}};
		String[][] genders = {{"2","6"},{"1","4"},{"ANY","0"}};
		String[][] driving = {{"YES","7"},{"NO","3"}};
		//String[][] M_Statuses = {{"Married","80"},{"Single","17"},{"Divorced","2"},{"Widowed","1"}};
		String[][] M_Statuses = {{"1","80"},{"0","17"},{"2","2"},{"3","1"}};
		String[][] statuses = {{"PUBL","8"},{"HIDN","2"}};
		String[][] nationalities = {{"AE","100"}}; //,{"SD","5"},{"BH","5"},{"OM","4"},{"PK","4"},{"IN","4"},{"QA","4"},{"ID","5"},{"IR","4"},{"PH","5"}};
		String[][] jobFPs = {{"FULLT","65"},{"PART","35"}};
		String[][] contractType = {{"CONTR","45"},{"PERM","35"},{"EMPDVNAC","5"},{"JOBTRNO","5"},{"INTERN","10"},{"SUMMJOB","10"},{"MINJOB","10"}};
		String[][] jobType = {{"CONTR","45"},{"PERM","35"},{"EMPDVNAC","5"},{"JOBTRNO","5"},{"INTERN","10"},{"SUMMJOB","10"},{"MINJOB","10"}};
		//String[][] jobShifts = {{"Day","65"},{"Night","20"},{"Two Shifts","15"}};
		String[][] jobShifts = {{"1","65"},{"2","20"},{"3","15"}};
		String[][] jobScheduleP = {{"08:00 - 12:00","20"},{"12:00 - 17:00","20"},{"09:00 - 12:00","15"},{"13:00 - 17:00","15"},{"12:00 - 16:00","10"},{"17:00 - 22:00","10"},{"18:00 - 24:00","10"}};
		String[][] jobScheduleF = {{"09:00 - 17:00","20"},{"09:00 - 17:00","20"},{"08:00 - 16:00","15"},{"08:00 - 16:00","15"},{"16:00 - 24:00","10"},{"18:00 - 02:00","10"},{"20:00 - 04:00","10"}};
		//String[][] travel = {{"0%","40"},{"25%","20"},{"50%","15"},{"75%","12"},{"100%","8"}};
		String[][] travel = {{"2","40"},{"3","20"},{"4","15"},{"5","12"},{"6","8"}};
		String[][] disabilities = {{"","96"},{"Attention-Deficit","1"},{"Hyperactivity Disorders","1"},{"Blindness","1"},{"Low Vision","1"},{"Blindness","1"},
			{"Brain Injuries","1"},{"Deaf","1"},{"Hard-of-Hearing","1"},{"Learning Disabilities","1"},{"Medical Disabilities","1"},
			{"Speech Disabilities","1"},{"Language Disabilities","1"}};
		String[][] telework = {{"TELEW","2"},{"NOTELEW","8"}};
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
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;

			rs = stmt.executeQuery("select max(candidate_id) from emp_dubai.candidate");
			if (rs.next())
			{
				start = rs.getInt(1) + 1;
			}
			rs = stmt.executeQuery("select count(*) from emp_dubai.employer");
			if (rs.next())
			{
				empCounts = rs.getInt(1);
			}

			if (genType.equalsIgnoreCase("specific"))
			{
				start = 1;
			}
			if (start>to_ && start<maxSpecific)
				start = maxSpecific;

			if (genType.equalsIgnoreCase("specific"))
				loadJobs(to_, occupSRC);
			else
				loadJobs(0, occupSRC);
				
			String lang = "";

			System.out.println("\n *** Generating candidates (" + nbre + ") Age: " + from + "-" + to + " ***  ");
			System.out.print("\t- ");
			int i;
			for (i = start; i < nbre + start; i++)
			{
				if (i % 51 == 0)
				{
					System.out.print("\n\t- ");
					// generating date of birth and working experience (only dates) yyyy-mm-dd
				}

				jobShift = "Day";
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
				String gender = pickValue(genders, rn.nextInt(totals[1]) + 1);
				String occupation = "";
				occupation = jobsDic.get(String.valueOf(rn.nextInt(jobsDic.size())));
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

				query = "insert into emp_dubai.candidate (candidate_id,birth_date, gender, driving_licence, marital_status, candidate_Status, nationality, work_permit_validity, language) values ("
					+ i
					+ ",'"
					+ Bdate
					+ "', '"
					+ gender
					+ "', '"
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
					+ "')";
				//System.out.println(query);
				System.out.print("| " + Bdate + "(" + gender + ") ");
				stmt.executeUpdate(query);
				// assign unlimited work-permit_validity to Local citizens
				//stmt.executeUpdate("update emp_dubai.candidate set work_permit_validity='2099-12-31'  where nationality='SA'");
				

/*				String disability = pickValue(disabilities, rn.nextInt(totals[5]));
				
				if (disability.length()>1)
				{
					query = "insert into emp_dubai.candidate_disabilities (candidate_id, disability_name) values ("
						+ i
						+ ", \""
						+ disability
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue());
					//System.out.println(query); //TODO: remove println
					//stmt.executeUpdate(query); // TODO fix and uncomment

				}
*/
				jobFP = pickValue(jobFPs, rn.nextInt(totals[6]) + 1);
				hours = jobFP.equalsIgnoreCase("Full-time")?getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]):getHours(jobScheduleP[rn.nextInt(jobScheduleP.length)][0]);

				// retrieve list of synonym occupations, to create work experience
				query = "SELECT distinct job_title_" + lang + " FROM asoc.example_job_titles where occup_code='"
					+ occupation.substring(occupation.indexOf("##") + 2)
					+ "'";

				//System.out.println(query);
				rs = stmt.executeQuery(query);
				Map<Integer, String> similars = new HashMap<Integer, String>();
				ocId = "";
				jobCode = occupation.substring(0, occupation.indexOf("##"));
				jobCode = jobsLang.get(jobCode);
				jobCode = (lang.equalsIgnoreCase("en")?jobCode.substring(0, jobCode.indexOf("##")):jobCode.substring(jobCode.indexOf("##")+2, jobCode.length()));
				//System.out.println("\tSimilar occupations for work experience: " + occupation);
				similars.put(0, jobCode);
				int j = 0;
				for (j = 1; rs.next(); j++)
				{
					String occup = rs.getString(1);
					int l = occup.indexOf(" ")-1;
					if (l<1 || occup.charAt(l)!='ة') // skip job titles which are specific for females
						similars.put(j, occup);
				}
				rs.close();
				jobTitle = similars.get(rn.nextInt(similars.size()));

				// ********** end list of similar occupations

				stmt.executeUpdate("SET NAMES 'utf8'");
				query = "insert into emp_dubai.candidate_ambitions (candidate_id,job_title, occupation, availability_date, salary, salary_period, "
					+ "salary_currency, desired_contract_type, desired_job_type, desired_full_part_time, availability_hours, hours_period, "
					+ "travel_to_work, desired_shift_type, telework, commute_distance, language) values ("
					+ i
					+ ",\""
					+ jobTitle
					+ "\", '"
					//+ occupation.substring(occupation.indexOf("##") + 2)
					+ occupation.substring(0, occupation.indexOf("##"))
					+ "', date_add(curdate(), interval "
					+ rn.nextInt(6 * 30)
					+ " day), "
					+ salary
					+ ", 'Month', 'AED', '"
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
					+ "')";

				//System.out.println(query);
				System.out.print("amb-exp");
				stmt.executeUpdate(query);


				int exp = year - y - age; ///3;
				//System.out.println("\t\t -exp1 " + exp);
				//exp = exp<2?1:exp;
				//System.out.println("\t\t -y= " + exp);
				Map<String, String> skills = new HashMap<String, String>();
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

					query = "insert into emp_dubai.work_experience (candidate_id, job_title_we, isco_code, full_part_time, start_we, end_we, employer_id, language) values ("
						+ i
						+ ", \""
						+ jobTitle
						+ "\", \""
						//+ occupation.substring(occupation.indexOf("##") + 2)
						+ occupation.substring(0, occupation.indexOf("##"))
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
						+ ", '"
						+ lang
						+ "')";

					//System.out.println("\t- " + query);
					System.out.print(".");
					stmt.executeUpdate(query);

					// retrieve list of skills for jobs, to create candidate skills
					query = "SELECT ConceptURI, ConceptPT FROM escoskos.skills where ConceptURI in (select hasRelatedConcept from escoskos.occupation_skill where isRelatedConcept='"
						+ ocId
						+ "')";
					//System.out.println(query);
					rs = stmt.executeQuery(query);
					String skillId = "";
					for (; rs.next();)
					{
						skillId = rs.getString(1);
						skillId = skillId.substring(skillId.lastIndexOf("/") + 1, skillId.length());
						skills.put(skillId, rs.getString(2));
						//System.out.println("\t\t skill --> " + rs.getString(1) + ": " + rs.getString(2));
						//query = "insert into emp_dubai.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');";
						//System.out.println("\t" + query);
						//stmt.executeUpdate(query);
					}
					rs.close();

					// ********** end list of similar occupations

				}
				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();
				//System.out.println("\tCombined skills for all for work experience: ");

				System.out.print("-sk");
				while (it.hasNext())
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into emp_dubai.candidate_skills (candidate_id, skill_id, skill_name) values ("
						+ i
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue());
					System.out.print(".");
					stmt.executeUpdate(query);

				}
			}

			for (int q=0; batchQueries[q]!=null ; q++) {
				//System.out.print("\n" + batchQueries[q]);
				stmt.executeUpdate(batchQueries[q]);
			}

			System.out.println("\n\t" + (i - start) + " candidates have been created");

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

			String query = "SELECT candidate_id, job_title, left(occupation,6) occupation from emp_dubai.candidate_ambitions where candidate_id not in (select distinct candidate_id from emp_dubai.candidate_skills)";
			//System.out.println(query);
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();

				//System.out.print("\nskills for " + rsC.getString(2) + ": \n\t-");
				query = "SELECT skill_code, skill_title_en FROM asoc.skills where occup_code= '"
					+ rsC.getString(3) + "'";
				//System.out.println(query);
				rs = stmt2.executeQuery(query);
				String skillId = "";
				for (; rs.next();)
				{
					skillId = rs.getString(1);
					skills.put(skillId, rs.getString(2));
					System.out.print(rs.getString(2) + ", ");
					i++;
					//query = "insert into emp_dubai.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');";
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
					query = "insert into emp_dubai.candidate_skills (candidate_id, skill_id, skill_name) values ("
						+ rsC.getString(1)
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue());
					System.out.print(entry.getValue() + ", ");
					//System.out.println("\n" + query);
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

	public static void GenJobSkills()
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

			String query = "SELECT job_id, job_title, left(job_code,6) occupation from emp_dubai.job where job_id not in (select distinct job_id from emp_dubai.job_skills)";
			//System.out.println(query);
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();

				//System.out.print("\nskills for " + rsC.getString(2) + ": \n\t-");
				query = "SELECT skill_code, skill_title_en FROM asoc.skills where occup_code= '"
					+ rsC.getString(3) + "'";
				//System.out.println(query);
				rs = stmt2.executeQuery(query);
				String skillId = "";
				for (; rs.next();)
				{
					skillId = rs.getString(1);
					skills.put(skillId, rs.getString(2));
					System.out.print(rs.getString(2) + ", ");
					i++;
					//query = "insert into emp_dubai.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');";
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
					query = "insert into emp_dubai.job_skills (job_id, skill_id, skill_name) values ("
						+ rsC.getString(1)
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue());
					System.out.print(entry.getValue() + ", ");
					//System.out.println("\n" + query);
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

	public static void updateASOCSkills()
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
			int i = 133;
			int c = 0;

			String query = "SELECT skill_title_en, count(skill_code) counts FROM asoc.skills where skill_code like '< %' group by skill_title_en order by counts desc;";
			//System.out.println(query);
			rsC = stmtC.executeQuery(query);
			while (rsC.next())
			{

				//System.out.print("\nskills for " + rsC.getString(2) + ": \n\t-");
					query = "update asoc.skills set skill_code = 'EF_" + (i) + "' where skill_title_en=\""
						+ rsC.getString(1) + "\"";
					System.out.println(query);
					stmt.executeUpdate(query);
					i++;
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

			String query = "SELECT candidate_id, job_title from emp_dubai.candidate_ambitions where candidate_id not in (select distinct candidate_id from emp_dubai.candidate_skills)";
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
						//query = "insert into emp_dubai.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');";
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
					query = "insert into emp_dubai.candidate_skills (candidate_id, skill_id, skill_name) values ("
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
	 * Generate Languages for the job seekers  
	 * based on statistics which are specific to KSA 
	 */
	public static void generateCandidateLanguages()
	{
		String[][] languages = {{"ar","70"},{"en","30"}};
		String[][] langLevel_1 = {{"ADV","70"},{"INT","30"}};
		String[][] langLevel_2 = {{"ADV","30"},{"INT","50"},{"BEG","20"}};
		int[] totals = {0,0,0}; // to hold the totals for each array
		System.out.println("\n *** Generating candidate languages ***  ");

		for (int i=0; i<languages.length; i++)
			totals[0] = totals[0] + Integer.parseInt(languages[i][1]);

		for (int i=0; i<langLevel_1.length; i++)
			totals[1] = totals[1] + Integer.parseInt(langLevel_1[i][1]);

		for (int i=0; i<langLevel_2.length; i++)
			totals[2] = totals[2] + Integer.parseInt(langLevel_2[i][1]);

		Map<String, String> canLanguages = new HashMap<String, String>(); // dictionary to store candidate languages
		String query = "";

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

			String cId, lang, level = "";
			// Generate Candidate Languages
			rs = stmt.executeQuery(
				"SELECT Candidate_id, nationality FROM emp_dubai.candidate where candidate_id not in (select distinct candidate_id from emp_dubai.candidate_languages)");

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
					query = "insert into emp_dubai.candidate_languages (candidate_id, language_id, level) values ("
						+ cId
						+ ", '"
						+ entry.getKey()
						+ "', '"
						+ entry.getValue()
						+ "')";
					stmtS.executeUpdate(query);
				}
				canLanguages.clear();
			}

			//STEP 6: Clean-up environment
			rs.close();
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
	 * Generate Languages for the jobs  
	 * based on statistics which are specific to KSA 
	 */
	public static void generateJobLanguages2()
	{
		String[][] languages = {{"ar","70"},{"en","30"}};
		String[][] langLevel_1 = {{"ADV","70"},{"INT","30"}};
		String[][] langLevel_2 = {{"ADV","30"},{"INT","50"},{"BEG","20"}};
		int[] totals = {0,0,0}; // to hold the totals for each array
		System.out.println("\n *** Generating candidate languages ***  ");

		for (int i=0; i<languages.length; i++)
			totals[0] = totals[0] + Integer.parseInt(languages[i][1]);

		for (int i=0; i<langLevel_1.length; i++)
			totals[1] = totals[1] + Integer.parseInt(langLevel_1[i][1]);

		for (int i=0; i<langLevel_2.length; i++)
			totals[2] = totals[2] + Integer.parseInt(langLevel_2[i][1]);

		Map<String, String> canLanguages = new HashMap<String, String>(); // dictionary to store candidate languages
		String query = "";

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

			String cId, lang, level = "";
			// Generate Candidate Languages
			rs = stmt.executeQuery(
				"SELECT job_id FROM emp_dubai.job where job_id not in (select distinct job_id from emp_dubai.job_languages)");

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
					query = "insert into emp_dubai.job_languages (job_id, language_id, level) values ("
						+ cId
						+ ", '"
						+ entry.getKey()
						+ "', '"
						+ entry.getValue()
						+ "')";
					stmtS.executeUpdate(query);
				}
				canLanguages.clear();
			}

			//STEP 6: Clean-up environment
			rs.close();
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

	/*
	 * This function loads the a list of jobs into a dictionary, so that it becomes easy to randomly pick jobs when automatically generating the data
	 * @param trg_occup if a target occupation is provided, this modules only loads similar jobs to the specified occupation (level 5)
	 * if occupation is not specified (null), all the jobs in the taxonomy will be loaded
	 * @param occupSRC: source of taxonomy occupations
	 * @return void
	 */
	public static void loadJobs(int trg_occup, String occupSRC)
	{
		Connection conn = null;
		Statement stmt = null;
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			String occup="";
			ResultSet rs;
			if (String.valueOf(trg_occup).length() > 5)
				occup = " where left(" + occupCode + ",6) = '" + String.valueOf(trg_occup).substring(0, 6) + "'";

			if (trg_occup==0)
				occup = " where left(" + occupCode + ",1) not in " + exclude;

			String query = "SELECT distinct "+ jobCode + ", " + occupCode + " FROM " + occupSRC + occup;
			String query2 = "SELECT distinct "+ jobCode + ", job_title_en, job_title_ar FROM " + occupSRC + occup;
			if (trg_occup ==99) // use 99 for work_experience
			{
				query = "SELECT distinct job_code, job_occupation from emp_dubai.job where job_id>=" + from_ + " and job_id<=" + to_;
				query2 = "SELECT distinct " + jobCode + ", job_title_en, job_title_ar FROM " + occupSRC 
					+ " where job_code in (SELECT distinct job_code from emp_dubai.job where job_id>=" + from_ + " and job_id<=" + to_ + ")";
			}

			//System.out.println("\n" + query);
/*			if (1==1)
				return;
*/			rs = stmt.executeQuery(query);

			int i = 0;
			jobsDic.clear();
			while (rs.next())
			{
				jobsDic.put(String.valueOf(i++), rs.getString(1) + "##" + rs.getString(2));
				if (rs.getString(2).equalsIgnoreCase(occup))
				{
					jobsDic.put(String.valueOf(i++), rs.getString(1) + "##" + rs.getString(2));
					jobsDic.put(String.valueOf(i++), rs.getString(1) + "##" + rs.getString(2));
				}

			}
			//System.out.println(query2);
			rs = stmt.executeQuery(query2);

			jobsLang.clear();
			while (rs.next())
			{
				jobsLang.put(rs.getString(1), rs.getString(2) + "##" + rs.getString(3));

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

			rs = stmt.executeQuery("select max(job_id) from emp_dubai.job");
			int start = 1;
			if (rs.next())
			{
				start = rs.getInt(1) + 1;
			}
			loadJobs(trg_occup, tableName);
			
			if (start<maxSpecific && from_>maxSpecific)
				start = from_;
			
			int salary = 0;

			System.out.println("\n *** Generating Job data (" + counts + ") " + ((trg_occup!=0)?"occupation: " + trg_occup:"") + " ***  ");

			String[][] languages = {{"en","6"},{"ar","4"}};
			String[][] genders = {{"M","3"},{"F","2"},{"ANY","5"}};
			String[][] driving = {{"Yes","7"},{"No","3"}};
			//String[][] statuses = {{"Active","8"},{"Inactive","2"}};
			String[][] statuses = {{"PUBL","10"},{"HIDN","0"}};
			String[][] contractType = {{"CONTR","45"},{"PERM","35"},{"EMPDVNAC","5"},{"JOBTRNO","5"},{"INTERN","10"},{"SUMMJOB","10"},{"MINJOB","10"}};
			String[][] jobType = {{"CONTR","45"},{"PERM","35"},{"EMPDVNAC","5"},{"JOBTRNO","5"},{"INTERN","10"},{"SUMMJOB","10"},{"MINJOB","10"}};
			//String[][] travel = {{"0%","40"},{"25%","20"},{"50%","15"},{"75%","12"},{"100%","8"}};
			String[][] travel = {{"2","40"},{"3","20"},{"4","15"},{"5","12"},{"6","8"}};
			//String[][] jobShifts = {{"Day","65"},{"Night","20"},{"Two Shifts","15"}};
			String[][] jobShifts = {{"1","65"},{"2","20"},{"3","15"}};
			String[][] jobScheduleP = {{"08:00 - 12:00","20"},{"12:00 - 17:00","20"},{"09:00 - 12:00","15"},{"13:00 - 17:00","15"},{"12:00 - 16:00","10"},{"17:00 - 22:00","10"},{"18:00 - 24:00","10"}};
			String[][] jobScheduleF = {{"09:00 - 17:00","20"},{"09:00 - 17:00","20"},{"08:00 - 16:00","15"},{"08:00 - 16:00","15"},{"16:00 - 24:00","10"},{"18:00 - 02:00","10"},{"20:00 - 04:00","10"}};
			//String[][] jobFPs = {{"Full-time","65"},{"Part-time","35"}};
			String[][] jobFPs = {{"FULLT","65"},{"PART","35"}};
			String[][] ordering = {{"","20"},{" order by ConceptPT","20"},{" order by ConceptPT desc","20"},{" order by length(ConceptPT)","20"},{" order by length(ConceptPT) desc","20"}};
			//String[][] telework = {{"Yes","2"},{"No","8"}};
			String[][] telework = {{"TELEW","2"},{"NOTELEW","8"}};
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
			String occup, order, part_full_time, jobSchedule = "", jobShift = "Day", job_code, isco_code, lang, jobTitle;
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
				lang = pickValue(languages, rn.nextInt(totals[0]) + 1);

				// retrieve list of synonym occupations, to create abmition desired job
				String query = "SELECT distinct job_title_" + lang + " FROM asoc.example_job_titles where occup_code='"
					+ occup.substring(occup.indexOf("##") + 2)
					+ "'";

				//System.out.println(query);
				rs = stmt.executeQuery(query);
				Map<Integer, String> similars = new HashMap<Integer, String>();
				String jobCode = occup.substring(0, occup.indexOf("##"));
				jobCode = jobsLang.get(jobCode);
				jobCode = (lang.equalsIgnoreCase("en")?jobCode.substring(0, jobCode.indexOf("##")):jobCode.substring(jobCode.indexOf("##")+2, jobCode.length()));
				//System.out.println("\tSimilar occupations for work experience: " + occupation);
				similars.put(0, jobCode);
				int i = 0;
				for (i = 1; rs.next(); i++)
				{
					String occupation = rs.getString(1);
					int l = occupation.indexOf(" ")-1;
					if (l<1 || occupation.charAt(l)!='ة') // skip job titles which are specific for females
						similars.put(i, occupation);
				}
				rs.close();
				jobTitle = similars.get(rn.nextInt(similars.size()));

				
				part_full_time = pickValue(jobFPs, rn.nextInt(totals[10]) + 1);
				order = pickValue(ordering, rn.nextInt(totals[11]) + 1);
				jobShift = pickValue(jobShifts, rn.nextInt(totals[7]) + 1);

				jobSchedule = part_full_time.equalsIgnoreCase("FULLT")?pickValue(jobScheduleF, rn.nextInt(totals[9]) + 1):pickValue(jobScheduleP, rn.nextInt(totals[8]) + 1);
				hours = getHours(jobSchedule);
				//hours = part_full_time.equalsIgnoreCase("Full-time")?getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]):getHours(jobScheduleF[rn.nextInt(jobScheduleF.length)][0]);


				switch (isco_code.charAt(0))
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

				//System.out.println(i++ + ". " + skillId + " - " + jobType + " (" + jobSchedule + ")");
				query = "insert into emp_dubai.job (job_id, job_code, job_title, full_part_time, job_schedule, job_occupation, salary, salary_period, "
					+ "salary_currency, start_date, telework, job_status, job_type, job_contract_type, travel_to_work, job_shift_type, desired_gender, "
					+ "job_hours, hours_period, language) values ("
					+ start
					+ ", '"
					+ job_code
					+ "', \""
					+ jobTitle 
					+ "\", '"
					+ part_full_time
					+ "', '"
					+ jobSchedule
					+ "', '"
					+ job_code
					+ "', '"
					+ salary
					+ "', 'Month', 'AED', date_add(curdate(), interval "
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
					+ lang
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
				    query = "insert into emp_dubai.job_skills (job_id, skill_id) values (" + i + ", '" +skillId + "');";
				    System.out.println("\t" + query);
				    stmtQ.executeUpdate(query);
				}
				rsS.close();
				*/

			}
			j--;
			// updating job title, description, and sector from the taxonomy database
			for (int q=0; batchQueries[q]!=null ; q++) {
			    //System.out.println(batchQueries[q]);
				stmtQ.executeUpdate(batchQueries[q]);
			}
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

	/* Generates occupations list for HRDF Dictionary
	 * The generated list corresponds to the HRDF structure
	 */
	public static void generateLists_UAE()
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null, rs2 = null;

			System.out.println(new Date());
			String filepath = "C:\\Elise_test\\dictionaries\\DubaiDictionary1.1\\list\\ct_maji.ed";
			PrintWriter writer = new PrintWriter(filepath);
			filepath = "C:\\Elise_test\\dictionaries\\DubaiDictionary1.1\\list\\ct_majii.ed";
			PrintWriter writer2 = new PrintWriter(filepath);
			String query = "SELECT distinct disc_code, disc_title_en, disc_title_ar FROM asoc.discipline";
			rs = stmt.executeQuery(query);
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString("disc_code")
						+ "\""
						+ "\n\t@en, \""
						+ rs.getString("disc_title_en")
						+ "\"\n\t@ar, \""
						+ rs.getString("disc_title_ar")
						+ "\"\n");
				query = "SELECT distinct field_code, field_title_en, field_title_ar FROM asoc.discipline where disc_code='" + rs.getString("disc_code") + "'";

				rs2 = stmt2.executeQuery(query);
				while (rs2.next())
				{
					if (rs2.getString("field_code").length()>2)
					writer2.println("\""
						+ rs2.getString("field_code") + "-"
						+ rs.getString("disc_code")
							+ "\", \""
							+ rs.getString("disc_code")
							+ "\"\n\t@en, \""
							+ rs2.getString("field_title_en")
							+ "\"\n\t@ar, \""
							+ rs2.getString("field_title_ar")
							+ "\"\n");
				}

			}
			writer.close();
			writer2.close();


/*			filepath = "C:\\Elise_test\\dictionaries\\DubaiDictionary1.1\\list\\ct_occu.ed";
			writer = new PrintWriter(filepath);
			// query to retrieve occupations
			query = "SELECT occup_code, left(occup_code, 4), job_code, job_title_en, job_title_ar FROM asoc.occupation where right(job_code,2)='01'";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString(1)
						+ "\", \""
						+ rs.getString(2)
						+ "\"\n\t@en, \""
						+ rs.getString(4)
						+ "\"\n\t@ar, \""
						+ rs.getString(5)
						+ "\"");
			}

			System.out.println("\tgenerated list at: " + filepath + "\nFinished at: " + new Date());
			writer.close();
*/
			filepath = "C:\\Elise_test\\dictionaries\\DubaiDictionary1.1\\list\\ct_sec.ed";
			System.out.println(new Date());
			writer = new PrintWriter(filepath);
			// query to retrieve occupations
			query = "SELECT distinct sector_id, name_en, name_ar FROM asoc.sectors;";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString("sector_id")
						+ "\"\n\t@en, \""
						+ rs.getString("name_en")
						+ "\"\n\t@ar, \""
						+ rs.getString("name_ar")
						+ "\"");
			}

			System.out.println("\tgenerated list at: " + filepath + "\nFinished at: " + new Date());
			writer.close();

			filepath = "C:\\Elise_test\\dictionaries\\DubaiDictionary1.1\\list\\ct_ind.ed";
			System.out.println(new Date());
			writer = new PrintWriter(filepath);
			// query to retrieve occupations
			query = "SELECT distinct industry_id, name_en, name_ar FROM asoc.industry;";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString("industry_id")
						+ "\"\n\t@en, \""
						+ rs.getString("name_en")
						+ "\"\n\t@ar, \""
						+ rs.getString("name_ar")
						+ "\"");
			}

			System.out.println("\tgenerated list at: " + filepath + "\nFinished at: " + new Date());

			writer.close();
			rs.close();
			stmt.close();
			conn.close();
			writer.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


	public static void generateCandidateEducation() throws ClassNotFoundException
	{ //synonyms and similars from Janzz
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		Statement stmt3 = null, stmt4 = null;
		String query = "";
		Date date = new Date();
		System.out.println("Generating Candidate Education data:");
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmt3 = conn.createStatement();
			stmt4 = conn.createStatement();
			ResultSet rs, rs2, rs3;

			// JDBC2Elise data type mappings

			stmt4.executeUpdate("delete from emp_dubai.candidate_education");

			rs = stmt.executeQuery(
				"SELECT candidate_id, job_title, left(occupation,6) occupation, sector, 'en' language FROM emp_dubai.candidate_ambitions");

			int i = 1;
			int j = 0;
			int idx = 0, idx2=0;
			String update="";
			Random rn = new Random();
			while (rs.next())
			{
				//System.out.println(rs.getString(1) + ": " + rs.getString(2));
				update = "insert into emp_dubai.candidate_education (candidate_id, education_level, education_area, education_field) values (\"" + rs.getString(1) + "\",\"";
				query = "SELECT oe.job_code, e.edu_code, e.edu_title_" + rs.getString("language") + " FROM asoc.occupation_education oe, asoc.education e where oe.edu_code=e.edu_code"
					+ " and oe.job_code=\"" + rs.getString(3) + "\"";
				//System.out.println(query);
				rs2 = stmt2.executeQuery(query);

				while (rs2.next())
				{
					idx = 0;
					//System.out.println("\t- " + rs2.getString(2) + ": " + rs2.getString(3));
					update += rs2.getString(2) + "\",\"";
					
					query = "SELECT * FROM asoc.discipline where occup_code='" + rs.getString(3) + "'";
					//System.out.println(query);
					rs3 = stmt3.executeQuery(query);
					rs3.last();
					idx2=rs3.getRow();
					if (rs3.getRow()>0)
						idx = rn.nextInt(rs3.getRow());
				    rs3.beforeFirst();

					for (int k=0; rs3.next(); k++)
					{
						//System.out.println("\t   - " + rs3.getString("disc_title_" + rs.getString("language")) + ": " + rs3.getString("field_title_en"));
						if (k==idx)
							update += rs3.getString("disc_code") + "\",\"" + rs3.getString("field_code") + "\")";
							//update += rs3.getString("disc_title_" + rs.getString("language")) + "\",\"" + rs3.getString("field_title_" + rs.getString("language")) + "\")";
					}
					if (idx2<=0)
						update += "\",\"\")";
					
					//System.out.println(update.substring(116, update.length()));
					//System.out.println(update);
					stmt4.executeUpdate(update);

				}
			}
			stmt4.executeUpdate("update emp_dubai.candidate_education set education_area=null where education_area=''");
			stmt4.executeUpdate("update emp_dubai.candidate_education set education_field=null where education_field=''");

			System.out.println("\nGenerating Jobs data:  Start at: \t" + date);
			System.out.println("Generating Jobs data:  Finished at: \t" + new Date());
			System.out.println("\t--> " + j + " Synonyms and similar jobs have been created");
			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			stmt2.close();
			conn.close();
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
	}//end function

	public static void generateJobEducation() throws ClassNotFoundException
	{ //synonyms and similars from Janzz
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		Statement stmt3 = null, stmt4 = null;
		String query = "";
		Date date = new Date();
		System.out.println("\nGenerating Job Education data:");
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmt3 = conn.createStatement();
			stmt4 = conn.createStatement();
			ResultSet rs, rs2, rs3;

			stmt4.executeUpdate("delete from emp_dubai.job_education");

			rs = stmt.executeQuery(
				"SELECT job_id, job_title, left(job_occupation,6) occupation, job_sector, 'en' language FROM emp_dubai.job");

			int i = 1;
			int j = 0;
			int idx = 0, idx2=0;
			String update="";
			Random rn = new Random();
			while (rs.next())
			{
				//System.out.println(rs.getString(1) + ": " + rs.getString(2));
				update = "insert into emp_dubai.job_education (job_id, education_level, education_area, education_field) values (\"" + rs.getString(1) + "\",\"";
				query = "SELECT distinct oe.job_code, e.edu_code, e.edu_title_" + rs.getString("language") + " FROM asoc.occupation_education oe, asoc.education e where oe.edu_code=e.edu_code"
					+ " and oe.job_code=\"" + rs.getString(3) + "\"";
				System.out.println(rs.getString(1) + ": ");
				System.out.println(query);
				rs2 = stmt2.executeQuery(query);

				while (rs2.next())
				{
					idx = 0;
					update = "insert into emp_dubai.job_education (job_id, education_level, education_area, education_field) values (\"" + rs.getString(1) + "\",\"";
					//System.out.println("\t- " + rs2.getString(2) + ": " + rs2.getString(3));
					update += rs2.getString(2) + "\",\"";
					
					query = "SELECT * FROM asoc.discipline where occup_code='" + rs.getString(3) + "'";
					//System.out.println(query);
					rs3 = stmt3.executeQuery(query);
					rs3.last();
					idx2=rs3.getRow();
					if (rs3.getRow()>0)
						idx = rn.nextInt(rs3.getRow());
				    rs3.beforeFirst();

					for (int k=0; rs3.next(); k++)
					{
						//System.out.println("\t   - " + rs3.getString("disc_title_" + rs.getString("language")) + ": " + rs3.getString("field_title_en"));
						if (k==idx)
							update += rs3.getString("disc_code") + "\",\"" + rs3.getString("field_code") + "\")";
							//update += rs3.getString("disc_title_" + rs.getString("language")) + "\",\"" + rs3.getString("field_title_" + rs.getString("language")) + "\")";
					}
					if (idx2<=0)
						update += "\",\"\")";
					
					//System.out.println(update.substring(116, update.length()));
					System.out.println(update);
					stmt4.executeUpdate(update);

				}

			}
			stmt4.executeUpdate("update emp_dubai.job_education set education_area=null where education_area=''");
			stmt4.executeUpdate("update emp_dubai.job_education set education_field=null where education_field=''");


			System.out.println("\nGenerating Jobs data:  Start at: \t" + date);
			System.out.println("Generating Jobs data:  Finished at: \t" + new Date());
			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			stmt2.close();
			conn.close();
			}
			catch (SQLException se)
			{
				se.printStackTrace();
			} //end finally try
	}//end function

	/* Full clean of the staging database
	 * allows to re-generate the data
	 */
	public static void cleanFull() throws ClassNotFoundException, SQLException
	{
		String[] queries = {
			"delete FROM emp_dubai.job_education",
			"delete FROM emp_dubai.job_languages",
			"delete FROM emp_dubai.work_experience",
			"delete FROM emp_dubai.job",
			"delete FROM emp_dubai.employer",
			"delete FROM emp_dubai.job_skills",
			"delete FROM emp_dubai.candidate_education",
			"delete FROM emp_dubai.candidate_certificates",
			"Delete from emp_dubai.candidate_skills",
			"Delete from emp_dubai.candidate_languages",
			"Delete from emp_dubai.candidate_related",
			"Delete from emp_dubai.candidate_education",
			"Delete from emp_dubai.candidate_children",
			"Delete from emp_dubai.candidate_disabilities",
			"Delete from emp_dubai.candidate_ambitions",
			"Delete from emp_dubai.candidate_locations",
			"Delete from emp_dubai.candidate_benefits",
			"Delete from emp_dubai.candidate_disabilities",
			"delete FROM emp_dubai.candidate",
			};

		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		for (int i = 0; i < queries.length; i++)
		{
			System.out.println(queries[i] + ": \n\t--> " + stmt.executeUpdate(queries[i]) + " records deleted");
		}
		stmt.close();
		conn.close();
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
		System.out.println(" *** JOBS *** "); 
		rs = stmt.executeQuery("select count(*) FROM emp_dubai.job");
		for (;rs.next();)
			System.out.println("Total number of jobs: " + rs.getString(1)); 
		
		System.out.println(" - Languages"); 
		rs = stmt.executeQuery("SELECT  language, COUNT(language) AS `Count`, concat(FORMAT(((COUNT(language) * 100) / newJobs.iCount),2),'%') AS `Percentage` FROM   emp_dubai.job, (SELECT COUNT(language) AS iCount FROM emp_dubai.job) newJobs GROUP BY language");
		for (;rs.next();) 
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - telework"); 
		rs = stmt.executeQuery("SELECT  telework, COUNT(telework), concat(FORMAT(((COUNT(telework) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(telework) AS iCount FROM emp_dubai.job) newJobs GROUP BY telework");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - full_part_time"); 
		rs = stmt.executeQuery("SELECT  full_part_time, COUNT(full_part_time), concat(FORMAT(((COUNT(full_part_time) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(full_part_time) AS iCount FROM emp_dubai.job) newJobs GROUP BY full_part_time");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - job_shift_type"); 
		rs = stmt.executeQuery("SELECT  job_shift_type, COUNT(job_shift_type), concat(FORMAT(((COUNT(job_shift_type) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(job_shift_type) AS iCount FROM emp_dubai.job) newJobs GROUP BY job_shift_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - telework"); 
		rs = stmt.executeQuery("SELECT  telework, COUNT(telework), concat(FORMAT(((COUNT(telework) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(telework) AS iCount FROM emp_dubai.job) newJobs GROUP BY telework");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - full_part_time"); 
		rs = stmt.executeQuery("SELECT  full_part_time, COUNT(full_part_time), concat(FORMAT(((COUNT(full_part_time) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(full_part_time) AS iCount FROM emp_dubai.job) newJobs GROUP BY full_part_time");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - job_shift_type"); 
		rs = stmt.executeQuery("SELECT  job_shift_type, COUNT(job_shift_type), concat(FORMAT(((COUNT(job_shift_type) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(job_shift_type) AS iCount FROM emp_dubai.job) newJobs GROUP BY job_shift_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - job_contract_type"); 
		rs = stmt.executeQuery("SELECT  job_contract_type, COUNT(job_contract_type), concat(FORMAT(((COUNT(job_contract_type) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(job_contract_type) AS iCount FROM emp_dubai.job) newJobs GROUP BY job_contract_type");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - travel_to_work"); 
		rs = stmt.executeQuery("SELECT  travel_to_work, COUNT(travel_to_work), concat(FORMAT(((COUNT(travel_to_work) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(travel_to_work) AS iCount FROM emp_dubai.job) newJobs GROUP BY travel_to_work");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		System.out.println(" - desired_gender"); 
		rs = stmt.executeQuery("SELECT  desired_gender, COUNT(desired_gender), concat(FORMAT(((COUNT(desired_gender) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.job, (SELECT COUNT(desired_gender) AS iCount FROM emp_dubai.job) newJobs GROUP BY desired_gender");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " jobs - " + rs.getString(3)); 
		
		/************** Job Seeker ***************************/
		System.out.println("\n\n *** Job Seeker *** "); 
		rs = stmt.executeQuery("select count(*) FROM emp_dubai.candidate");
		for (;rs.next();)
			System.out.println("Total number of jobseekers: " + rs.getString(1)); 
		
		System.out.println(" - Languages"); 
		rs = stmt.executeQuery("SELECT  language, COUNT(language) AS `Count`, concat(FORMAT(((COUNT(language) * 100) / newJobs.iCount),2),'%') AS `Percentage` FROM   emp_dubai.candidate, (SELECT COUNT(language) AS iCount FROM emp_dubai.candidate) newJobs GROUP BY language");
		for (;rs.next();) 
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getInt(2) + " candidates - " + rs.getString(3)); 
		
		System.out.println(" - driving_licence"); 
		rs = stmt.executeQuery("SELECT  driving_licence, COUNT(driving_licence), concat(FORMAT(((COUNT(driving_licence) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.candidate, (SELECT COUNT(driving_licence) AS iCount FROM emp_dubai.candidate) newJobs GROUP BY driving_licence");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 
		
		System.out.println(" - marital_status"); 
		rs = stmt.executeQuery("SELECT  marital_status, COUNT(marital_status), concat(FORMAT(((COUNT(marital_status) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.candidate, (SELECT COUNT(marital_status) AS iCount FROM emp_dubai.candidate) newJobs GROUP BY marital_status");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 
		
		System.out.println(" - nationality"); 
		rs = stmt.executeQuery("SELECT  nationality, COUNT(nationality), concat(FORMAT(((COUNT(nationality) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.candidate, (SELECT COUNT(nationality) AS iCount FROM emp_dubai.candidate) newJobs GROUP BY nationality");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 
		
		System.out.println(" - gender"); 
		rs = stmt.executeQuery("SELECT  gender, COUNT(gender), concat(FORMAT(((COUNT(gender) * 100) / newJobs.iCount),2),'%') FROM   emp_dubai.candidate, (SELECT COUNT(gender) AS iCount FROM emp_dubai.candidate) newJobs GROUP BY gender");
		for (;rs.next();)
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(2) + " candidates - " + rs.getString(3)); 
		
		rs.close();
		stmt.close();
		conn.close();
	}

	/* Cleans the specific data in the staging database
	 * allows to re-generate the specific data
	 */
	public static void cleanSpecific(int from, int to) throws ClassNotFoundException, SQLException
	{
		String[] queries = {
			"delete FROM emp_dubai.job where job_id>=" + from + " and job_id<=" + to,
			"delete FROM emp_dubai.work_experience where candidate_id>=" + from + " and candidate_id<=" + to,
/*			"delete FROM emp_dubai.employer where employer_id not in (select employer_id from emp_dubai.job where job_id>="
				+ from
				+ " and job_id<="
				+ to
				+ ")",
*/			"delete FROM emp_dubai.job_skills where job_id>=" + from + " and job_id<=" + to,
			"delete FROM emp_dubai.candidate_certificates where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_skills where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_languages where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_related where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_education where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_children where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_disabilities where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_ambitions where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_locations where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_benefits where candidate_id>=" + from + " and candidate_id<=" + to,
			"Delete from emp_dubai.candidate_disabilities where candidate_id>=" + from + " and candidate_id<=" + to,
			"delete FROM emp_dubai.candidate where candidate_id>=" + from + " and candidate_id<=" + to };

		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		for (int i = 0; i < queries.length; i++)
		{
			System.out.println(queries[i] + ": \n\t--> " + stmt.executeUpdate(queries[i]) + " records deleted");
		}
		stmt.close();
		conn.close();
	}

	/* Cleans the generic data in the staging database
	 * allows to re-generate the generic data
	 */
	public static void cleanGeneric(int from) throws ClassNotFoundException, SQLException
	{
		String[] queries = {
			"delete FROM emp_dubai.job where job_id>=" + from,
			"delete FROM emp_dubai.work_experience where candidate_id>=" + from,
			//"delete FROM emp_dubai.employer",
			"delete FROM emp_dubai.job_skills where job_id>=" + from,
			"delete FROM emp_dubai.candidate_certificates where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_skills where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_languages where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_related where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_education where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_children where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_disabilities where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_ambitions where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_locations where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_benefits where candidate_id>=" + from,
			"Delete from emp_dubai.candidate_disabilities where candidate_id>=" + from,
			"delete FROM emp_dubai.candidate where candidate_id>=" + from };

		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		for (int i = 0; i < queries.length; i++)
		{
			System.out.println(queries[i] + ": \n\t--> " + stmt.executeUpdate(queries[i]) + " records deleted");
		}
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

			input = new FileInputStream("dataGenerator_Dubai.properties");

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

			input = new FileInputStream("batchQueries_dubai.properties");
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