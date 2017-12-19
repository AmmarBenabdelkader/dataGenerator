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

public class utilities_asoc
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
		options.add("7");
		options.add("8");
		options.add("9");
		options.add("10");
		options.add("11");
		options.add("12");
		options.add("13");
		options.add("14");
		
		while (true)
		{
			System.out.printf("Please select the action to perform from the following:\n");
			System.out.println("\t- 1- Process new ASOC Occupations");
			System.out.println("\t- 2- Process new ASOC Taxonomy");
			System.out.println("\t- 3- Generate occupation list");
			System.out.println("\t- 4- Generate occupation list HRDF");
			System.out.println("\t- 5- Generate occupations affinities");
			System.out.println("\t- 6- Clean data (full clean)");
			System.out.println("\t- 7- Clean data (one occupation)");
			System.out.println("\t- 8- Occupation Prediction List");
			System.out.println("\t- 9- Occupation Prediction List MOL");
			System.out.println("\t- 10- Occupation Prediction List MOL");
			System.out.println("\t- 11- ProcessMatching");
			System.out.println("\t- 12- Explore Education");
			System.out.println("\t- 13- generate SQL Server data");
			System.out.println("\t- 14- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			int[] cols = { 6, 7, 5, 11 };
			processNewASOCtaxonomy(
				//"P:\\HRDF - ASOC\\RFP Q4 2015\\ASOC documents\\ASOC ODs V13 (1242 Multitab)\\ASOC ODs V13 (1242 Multitab)",
				"P:\\HRDF - ASOC\\RFP Q4 2015\\ASOC documents\\ASOC ODs V13 (1242 Multitab)\\ASOC ODs V13 (1242 Multitab)",
				11,
				cols,
				3);
			break;
		case "2":
			processNewASOCoccupations("P:\\HRDF - ASOC\\RFP Q4 2015\\ASOC documents\\ASOC Index English and Arabic 20150831 (4)_REV_final.xlsx", 0, 5, 1); // clean test data from 1 to 100 (included)
			break;
		case "3":
			occupation_list(); // to be implemented
			break;
		case "4":
			; // clean test data from 101 (included)
			break;
		case "5":
			occupation_affinities(); // to be implemented
			break;
		case "6":
			cleanDataFull(); // cleans the full test data
			break;
		case "7":
			cleanData("242402"); // cleans the test data for a given occupation
			break;
		case "8":
			occupation_Prediction_list_newASOC("ar"); // cleans the test data for a given occupation
			break;
		case "9":
			occupation_Prediction_list_MOL("ar"); // cleans the test data for a given occupation
			break;
		case "10":
			processSectors();
			break;
		case "11":
			ProcessMatching("P:\\TestData\\Functionality Test\\MatchOwn_data66c.txt");
			break;			
		case "12":
			exploreEducation("ar");
			break;			
		case "13":
			MySQL_to_SSQL("ar");
			break;			
		default:
			System.out.println("Bye"); // quit

		}

	}

	public static void MySQL_to_SSQL(String language) throws ClassNotFoundException, SQLException, FileNotFoundException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;
		int address_id = 1;
		String filepath = "c:\\temp\\employer.sql";
		PrintWriter writer = new PrintWriter(filepath);

		System.out.println("********** Employer's Data **********");
		query = "SELECT * FROM employment.employer order by employer_id";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			writer.println("insert into [dbo].[LOCATION] (LOCATION_ID,CITY,COUNTRY,GEOCODE_LONGITUDE,GEOCODE_LATTITUDE) values ("   
			+ address_id + ", '" + rs.getString(6).replaceAll("'", "''") + "', '" + rs.getString(8) + "', " + rs.getString(9) + ", " + rs.getString(10) + ");");

			writer.println("insert into [dbo].[EMPLOYER] (EMPLOYER_ID,ESTABLISHMENT_NAME,INDUSTRY,ADDRESS_ID) values ("  
				+ rs.getString(1) + ", '" + rs.getString(2) + "', '" + rs.getString(3) + "', " + address_id + ");" );

			address_id++;

		}
		writer.close();
		
		System.out.println("********** JOB's Data **********");
		filepath = "c:\\temp\\job.sql";
		writer = new PrintWriter(filepath);

		query = "SELECT * FROM employment.job order by job_id";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			writer.println("insert into [dbo].[JOB_POST] (JOB_POST_ID,OCCUPATION,TITLE,STATUS,JOB_TYPE,SECTOR,"
				+ "TELEWORKING,TRAVEL_REQUIREMENT,WORK_TIME,SHIFT_TYPE,GENDER,SALARY_TO,LANGUAGE,EMPLOYER_ID) values (" + rs.getString("job_id") 
				+ ", '" + rs.getString("job_occupation") + "', N'" + rs.getString("job_title").replaceAll("'", "''") 
				//+ "', '" + rs.getString("job_description").replaceAll("'", "''")
				+ "', '" + rs.getString("job_status") 
				+ "', '" + rs.getString("job_type") + (rs.getString("job_sector")!=null?("', '" + rs.getString("job_sector") 
				+ "', '"):("', " + rs.getString("job_sector") + ", '")) 
				+ rs.getString("telework") + "', '" + rs.getString("travel_to_work") 
				+ "', '" + rs.getString("full_part_time") + "', '" + rs.getString("job_shift_type") 
				+ "', '" + rs.getString("desired_gender") + "', '" + rs.getString("salary_to") 
				+ "', '" + rs.getString("language") + "', " + rs.getString("employer_id") 
				+ ");" );


		}
		writer.close();
		
		System.out.println("********** JobSeeker's Data **********");
		filepath = "c:\\temp\\jobseeker.sql";
		writer = new PrintWriter(filepath);

		query = "SELECT * FROM employment.candidate a, employment.candidate_locations b where b.address_type='Home' and a.candidate_id=b.candidate_id order by a.candidate_id";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			writer.println("insert into [dbo].[LOCATION] (LOCATION_ID,CITY,COUNTRY,GEOCODE_LONGITUDE,GEOCODE_LATTITUDE) values ("   
			+ address_id + ", '" + rs.getString("b.city").replaceAll("'", "''") + "', '" + rs.getString("country") + "', " + rs.getString("geo_longitude") + ", " + rs.getString("geo_latitude") + ");");

			writer.println("insert into [dbo].[INDIVIDUAL] (NES_INDIVIDUAL_ID,INDIVIDUAL_FIRST_NAME,INDIVIDUAL_SECOND_NAME,INDIVIDUAL_LAST_NAME,"
				+ "INDIVIDUAL_DOB,GENDER,MARITAL_STATUS,PUBLICATION_STATUS,NATIONALITY,ADDRESS_ID) values (" + rs.getString("a.candidate_id") 
				+ ", N'" + rs.getString("first_name").replaceAll("'", "''") + "', N'" + rs.getString("last_name").replaceAll("'", "''") 
				+ "', N'" + rs.getString("middle_name").replaceAll("'", "''")
				+ "', '" + rs.getString("birth_date") 
				+ "', '" + rs.getString("gender") + "', '" + rs.getString("marital_status") 
				+ "', '" + rs.getString("candidate_status") 
				+ "', '" + rs.getString("nationality") 
				+ "', " + address_id + ");" );

			address_id++;
		}

		System.out.println("********** JobSeeker's Ambitions **********");
		query = "SELECT * FROM employment.candidate_ambitions order by candidate_id";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			writer.println("insert into [dbo].[EMPLOYMENT_PREFERENCE] (NES_INDIVIDUAL_ID,JOB_TITLE,OCCUPATION,SECTOR,START_DATE,"
				+ "SALARY_FROM,JOB_TYPE,PREFER_TRAVEL,DISTANCE) values (" + rs.getString("candidate_id") 
				+ ", N'" + rs.getString("job_title").replaceAll("'", "''") + "', '" + rs.getString("occupation") 
				//+ "', '" + rs.getString("sector")
				+ (rs.getString("sector")!=null?("', '" + rs.getString("sector") 
				+ "', '"):("', " + rs.getString("sector") + ", '")) 
				+ rs.getString("availability_date") 
				+ "', '" + rs.getString("salary_to") + "', '" + rs.getString("desired_job_type") 
				+ "', '" + rs.getString("travel_to_work") 
				+ "', " + rs.getString("commute_distance") 
				+ ");" );

		}

		System.out.println("********** JobSeeker's Work Experience **********");
		query = "SELECT * FROM employment.work_experience order by candidate_id";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			writer.println("insert into [dbo].[PROFESSIONAL_EXPERIENCE] (NES_INDIVIDUAL_ID,JOB_FUNCTION,OCCUPATION,SECTOR,START_DATE,"
				+ "END_DATE,COMPANY_NAME) values (" + rs.getString("candidate_id") 
				+ ", N'" + rs.getString("job_title_we").replaceAll("'", "''") + "', '" + rs.getString("isco_code") 
				//+ "', '" + rs.getString("sector_we")
				+ (rs.getString("sector_we")!=null?("', '" + rs.getString("sector_we") 
				+ "', '"):("', " + rs.getString("sector_we") + ", '")) 
				+ rs.getString("start_we") + "', '" + rs.getString("end_we") 
				+ "', '" + rs.getString("company_name") 
				+ "');" );

		}

		System.out.println("********** JobSeeker's Language Skills **********");
		query = "SELECT * FROM employment.candidate_languages order by candidate_id";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			writer.println("insert into [dbo].[SKILL] (NES_INDIVIDUAL_ID,LANGUAGE,LANGUAGE_LEVEL) values (" + rs.getString("candidate_id") 
				+ ", '" + rs.getString("language_id") + "', '" + rs.getString("level") 
				+ "');" );

		}

		System.out.println("********** JobSeeker's Education **********");
		query = "SELECT candidate_id,education_level,education_area, concat(education_area,'-',education_field) education_field FROM employment.candidate_education order by candidate_id";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			writer.println("insert into [dbo].[EDUCATIONAL_INFORMATION] (NES_INDIVIDUAL_ID,EDUCATION_LEVEL,MAJOR_SPECIALIZATION,MINOR_SPECIALIZATION,"
				+ "END_DATE,SALARY_TO,COMPANY_NAME) values (" + rs.getString("candidate_id") 
				+ ", '" + rs.getString("education_level") + "', '" + rs.getString("education_area") 
				+ "', '" + rs.getString("education_field")
				+ "');" );

		}

		writer.close();
		rs.close();
		//rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}
	public static void exploreEducation(String language) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		System.out.println("********** ASOC Education Structure **********");
		//query = "SELECT distinct distinct disc_code, disc_title_en, disc_title_ar FROM asoc.discipline order by disc_code";
		query = "SELECT distinct distinct disc_code, disc_title_" + language + " FROM asoc.discipline order by disc_code";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println(rs.getString(1) + ": " + rs.getString(2));

			query = "SELECT distinct distinct field_code, field_title_" + language + " FROM asoc.discipline where disc_code='"
				+ rs.getString(1) + "' order by field_code";
			rs2 = stmt2.executeQuery(query);
			while (rs2.next())
			{
				System.out.println("\t" + rs2.getString(1) + ": " + rs2.getString(2));

			}
		}
		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}
	public static void occupation_list()
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			ResultSet rs = null;

			System.out.println(new Date());
			String filepath = "c:\\ELISE\\Dictionaries\\BPDictionary\\list\\occupations_list.ed";
			PrintWriter writer = new PrintWriter(filepath);
			// query to retrieve occupations
			String query = "SELECT distinct occup_id, ConceptPT FROM escoskos.occupations";
			//String query = "SELECT distinct job_title FROM test.job";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(rs.getString("occup_id") + "\n\t@en_us, \"" + rs.getString("ConceptPT") + "\"");
				//writer.println (i++ + "\n\t@en_us, \"" + rs.getString("job_title") + "\"" );
			}

			System.out.println("\tgenerated list at: " + filepath + "\nFinished at: " + new Date());
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

	public static void occupation_Prediction_list_newASOC(String language)
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			ResultSet rs = null;

			System.out.println(new Date());
			String filepath = "\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\HRDF-ASOC\\PredictionList_" + language + ".csv";
			PrintWriter writer = new PrintWriter(filepath);
			// query to retrieve occupations
			String query = "SELECT * FROM asoc.occupation_prediction where language='" + language + "' order by job_code";
			rs = stmt.executeQuery(query);
			String jobCode, occupCode="", occupTitle="";
			double match=0.0;
			int count = 0;
			if (rs.next()) {
				jobCode = rs.getString("job_code");
				occupCode = rs.getString("occup_code");
				occupTitle = rs.getString("occup_title");
				match = Double.parseDouble(rs.getString("accuracy"));
				count =1;
			}
			
			while (rs.next())
			{
				if (occupCode.equalsIgnoreCase(rs.getString("occup_code"))) {
					match = match + Double.parseDouble(rs.getString("accuracy"));
					count++;
				}
				else {
					writer.println(occupCode + "\t" + occupTitle + "\t" + ((match/count)==0?"Not Done":(match/count)>=1?"Done":"Partially done ") + "\t" + count + "\t" + match );
					jobCode = rs.getString("job_code");
					occupCode = rs.getString("occup_code");
					occupTitle = rs.getString("occup_title");
					match = Double.parseDouble(rs.getString("accuracy"));
					count =1;
					
				}
				
			}

			writer.close();

			System.out.println(new Date());

			rs.close();
			stmt.close();
			conn.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void occupation_Prediction_list_MOL(String language)
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			ResultSet rs = null;

			System.out.println(new Date());
			String filepath = "\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\HRDF-ASOC\\PredictionList_mol_" + language + ".csv";
			PrintWriter writer = new PrintWriter(filepath);
			// query to retrieve occupations
			String query = "SELECT * FROM asoc.occupation_prediction_mol where language='" + language + "' order by mol_id";
			rs = stmt.executeQuery(query);
			String jobCode="", occupCode="", occupTitle="";
			double match=0.0;
			int count = 0;
			if (rs.next()) {
				jobCode = rs.getString("mol_id");
				occupCode = rs.getString("isco_code");
				occupTitle = rs.getString("occup_title_" + language);
				match = Double.parseDouble(rs.getString("accuracy"));
				count =1;
			}
			
			while (rs.next())
			{
				if (occupCode.equalsIgnoreCase(rs.getString("isco_code"))) {
					match = match + Double.parseDouble(rs.getString("accuracy"));
					count++;
				}
				else {
					writer.println(occupCode + "\t" + occupTitle + "\t" + ((match/count)==0?"Not Done":(match/count)>=1?"Done":"Partially done ") + "\t" + count + "\t" + match );
					jobCode = rs.getString("mol_id");
					occupCode = rs.getString("isco_code");
					occupTitle = rs.getString("occup_title_" + language);
					match = Double.parseDouble(rs.getString("accuracy"));
					count =1;
					
				}
				
			}

			writer.close();

			System.out.println(new Date());

			rs.close();
			stmt.close();
			conn.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void occupation_affinities()
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null, rs2 = null;

			System.out.println("Start at: " + new Date());
			String filepath = "c:\\ELISE\\Dictionaries\\BPDictionary\\listaffinity\\occupations_affinities.ed";
			PrintWriter writer = new PrintWriter(filepath);
			// query to retrive occupations
			//String query = "SELECT distinct ConceptPT FROM escoskos.occupation_similars";
			String query = "SELECT distinct occup_id, conceptPT FROM escoskos.occupations";
			rs = stmt.executeQuery(query);
			int p = 100;
			while (rs.next())
			{
				//query = "SELECT similar FROM escoskos.occupation_similars where ConceptPT=\"" + rs.getString("ConceptPT") + "\"";
				query = "SELECT distinct occup_id, conceptPT FROM escoskos.occupations where conceptpt in (select similar FROM escoskos.occupation_similars where conceptpt=\""
					+ rs.getString("conceptpt")
					+ "\")";
				//System.out.println (query);
				rs2 = stmt2.executeQuery(query);
				p = 100;
				while (rs2.next())
				{
					writer.println(rs.getString("occup_id") + ", " + rs2.getString("occup_id") + ", " + (p < 40 ? p = 60 : p--));
				}
			}

			System.out.println("\tgenerated affinity list at: " + filepath + "\nFinished at: " + new Date());
			rs.close();
			rs2.close();
			stmt.close();
			stmt2.close();
			conn.close();
			writer.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void updateOneField()
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = null, rs2 = null;

			String query = "SELECT * FROM asoc.occupation_prediction where language='ar' limit 10";
			rs = stmt.executeQuery(query);
			while (rs.next())
			{
				query = "select job_title_ar from asoc.occupation_cat2  where job_title_en=\"" + rs.getString("occup_title") + "\"";
				System.out.println (query);
				rs2 = stmt2.executeQuery(query);
				if (rs2.next()) {
					query= "update asoc.occupation_prediction a set occup_title=\"" + rs2.getString("job_title_ar") + "\" where job_code='" + rs.getString("job_code") + "'";
					System.out.println (query);
					stmt3.executeUpdate(query);
				}
			}

			rs.close();
			rs2.close();
			stmt.close();
			stmt2.close();
			conn.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


	public static void processNewASOCoccupations(String path, int page, int cols, int ignore)
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();

			// JDBC2Elise data type mappings
			FileInputStream file = new FileInputStream(new File(path));

			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(page);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			String query = "";
			int i = 1;
			for (i = 1; i <= ignore; i++)
			{
				rowIterator.next();
			}

			while (rowIterator.hasNext())
			{
				//rowIterator.next();
				Row row = rowIterator.next();
				query = "insert into asoc.occupation values (";
				for (i = 0; i < cols; i++)
				{
					query = query
						+ (row.getCell(i).getCellType() == 1 ? "\"" + row.getCell(i).getStringCellValue() + "\""
							: row.getCell(i).getNumericCellValue())
						+ ",";
				}
				query = query.substring(0, query.length() - 1) + ")";
				System.out.println(query);
				stmt.executeUpdate(query);
			}
			System.out.println(new Date());
			file.close();
			workbook.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/*
	 * This methods processes the new ASOC data related to occupations, skills, eductaion, etc.
	 * Data is read from excel files and loaded into MySQL database as taxonomy
	 */
	public static void processNewASOCtaxonomy(String path, int page, int[] cols, int ignore)
	{
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			ResultSet rs = null;

			File folder = new File(path);
			Date date =new Date();
			String occup = "";
			File fileEntry[] = folder.listFiles();
			FileInputStream file;
			XSSFWorkbook workbook;
			//for (final File fileEntry : folder.listFiles())
			System.out.println("Processing data at: " + path + "\n\t- Ignoring " + ignore + " out of " + fileEntry.length);
			if (ignore> fileEntry.length)
				return;
			for (int l=ignore; l<=fileEntry.length; l++)
			{
				if (!fileEntry[l].isDirectory())
				{
					occup = fileEntry[l].getName().substring(3, 9);
					System.out.print("\n\toccupation: " + fileEntry[l].getName().substring(3, 9) + "\t");

					// JDBC2Elise data type mappings
					file = new FileInputStream(new File(path + "\\" + fileEntry[l].getName()));

					//Create Workbook instance holding reference to .xlsx file
					workbook = new XSSFWorkbook(file);

					int i = 1;
					int col;
					String query = "";
					Row row;
					Iterator<Row> rowIterator;

					// ********* Page 1 - Job Title *********************//
					//Get desired sheet from the workbook
					XSSFSheet sheet = workbook.getSheetAt(1);
					//Iterate through each rows one by one
										row = sheet.getRow(3);
					query="insert into asoc.occupation3 (job_code, job_title_en, job_title_ar) values (" + row.getCell(4).getStringCellValue() + ", \"" + row.getCell(5).getStringCellValue() + "\", \"" + row.getCell(7).getStringCellValue() + "\")";
							        	//System.out.println(query);
							        	stmt.executeUpdate(query);
										row = sheet.getRow(4);
					query="insert into asoc.occupation3 (job_code, job_title_en, job_title_ar) values (" + row.getCell(4).getStringCellValue() + ", \"" + row.getCell(5).getStringCellValue() + "\", \"" + row.getCell(7).getStringCellValue() + "\")";
							        	//System.out.println(query);
							        	stmt.executeUpdate(query);
										row = sheet.getRow(5);
					query="insert into asoc.occupation3 (job_code, job_title_en, job_title_ar) values (" + row.getCell(4).getStringCellValue() + ", \"" + row.getCell(5).getStringCellValue() + "\", \"" + row.getCell(7).getStringCellValue() + "\")";
							        	//System.out.println(query); 
					stmt.executeUpdate(query);
										row = sheet.getRow(6);
					query="insert into asoc.occupation3 (job_code, job_title_en, job_title_ar) values (" + row.getCell(4).getStringCellValue() + ", \"" + row.getCell(5).getStringCellValue() + "\", \"" + row.getCell(7).getStringCellValue() + "\")";
							        	//System.out.println(query); 
							        	stmt.executeUpdate(query);
					 row = sheet.getRow(2);
					row.getCell(4).setCellType(1);
					occup = row.getCell(4).getStringCellValue();
					System.out.print(row.getCell(5).getStringCellValue());
					query = "insert into asoc.occupation2 values ("
						+ occup
						+ ", \""
						+ row.getCell(5).getStringCellValue()
						+ "\", \""
						+ row.getCell(7).getStringCellValue()
						+ "\", \"";

					// ********* Page 2 - Job description *********************//
					sheet = workbook.getSheetAt(2);
					row = sheet.getRow(3);
					rowIterator = sheet.iterator();
					if (rowIterator.hasNext()) {
					query = query
						+ row.getCell(4).getStringCellValue()
						+ "\", \""
						+ row.getCell(6).getStringCellValue().replace("\"", "'")
						+ "\")";
					}
					else
						query = query + "\",'')";
					//System.out.println(query);
					stmt.executeUpdate(query);

					// ********* Page 3 - Example job titles *********************//
					sheet = workbook.getSheetAt(3);
					rowIterator = sheet.iterator();
					System.out.print(" Titles:");
					if (rowIterator.hasNext())
					{
						for (i = 1; i <= 3; i++)
						{
							rowIterator.next();
						}

						while (rowIterator.hasNext())
						{
							row = rowIterator.next();
							if (row.getCell(5) == null)
							{
								break;
							}
							if (row.getCell(5).getStringCellValue().length() < 2)
							{
								break;
							}
							//row.getCell(7).setCellType(1);
							query = "insert into asoc.example_job_titles values (";
							query = query
								+ "\""
								+ row.getCell(5).getStringCellValue()
								+ "\", \""
								+ row.getCell(7).getStringCellValue()
								+ "\", "
								+ occup
								+ ")";
							System.out.print(".");
							//System.out.println(query);
							stmt.executeUpdate(query);
						}
					}

					// ********* Page 4 - similar occupations *********************//
					sheet = workbook.getSheetAt(4);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 4; i++)
					{
						rowIterator.next();
					}

					System.out.print(" Similars:");
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(6) == null)
						{
							break;
						}
						if (row.getCell(6).getStringCellValue().length() < 2)
						{
							break;
						}
						row.getCell(5).setCellType(1);
						query = "insert into asoc.similar_occup values (";
						query = query
							+ "\""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(6).getStringCellValue()
							+ "\", \""
							+ row.getCell(8).getStringCellValue()
							+ "\", "
							+ occup
							+ ")";
						//System.out.print(".");
						System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 5 - else were classified *********************//
					sheet = workbook.getSheetAt(4);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 4; i++)
					{
						rowIterator.next();
					}

					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(6) == null)
						{
							break;
						}
						if (row.getCell(6).getStringCellValue().length() < 2)
						{
							break;
						}
						row.getCell(5).setCellType(1);
						query = "insert into asoc.elsewere_classified_occup values (";
						query = query
							+ "\""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(6).getStringCellValue()
							+ "\", \""
							+ row.getCell(8).getStringCellValue()
							+ "\", "
							+ occup
							+ ")";
						//System.out.print(".");
						System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 6 - key accountability / tasks and duties *********************//
					sheet = workbook.getSheetAt(6);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 4; i++)
					{
						rowIterator.next();
					}

					rs = stmt.executeQuery("select max(task_code) from asoc.accountability");
					int start = 1;
					if (rs.next())
					{
						start = rs.getInt(1);
					}
					start++;
					System.out.print(" Duties:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						query = "insert into asoc.accountability (task_code, task_title_en, task_title_ar,task_desc_en, task_desc_ar) values (";
						query = query
							+ start
							+ ", \""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(9).getStringCellValue()
							+ "\", \""
							+ row.getCell(6).getStringCellValue().replaceAll("\"", "'")
							+ "\", \""
							+ row.getCell(8).getStringCellValue().replaceAll("\"", "'")
							+ "\")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
						stmt.executeUpdate("insert into asoc.occupation_accountability values ('" + occup + "'," + start++ + ")");
					}

					// ********* Page 7 - Education Level *********************//
					sheet = workbook.getSheetAt(7);
					rowIterator = sheet.iterator();
					System.out.print(" Education:"); 
					if (rowIterator.hasNext())
					{
						for (i = 1; i <= 3; i++)
						{
							rowIterator.next();
						}

						while (rowIterator.hasNext())
						{
							row = rowIterator.next();
							if (row.getCell(5) == null)
							{
								break;
							}
							if (row.getCell(5).getStringCellValue().length() < 2)
							{
								break;
							}
							query = "insert into asoc.education values (";
							query = query
								+ "\""
								+ row.getCell(6).getStringCellValue()
								+ "\", \""
								+ row.getCell(5).getStringCellValue()
								+ "\", \""
								+ row.getCell(9).getStringCellValue()
								+ "\")";
							System.out.print(".");
							//System.out.println(query);
							stmt.executeUpdate(query);
							stmt.executeUpdate(
								"insert into asoc.occupation_education values ('"
									+ occup
									+ "','"
									+ row.getCell(6).getStringCellValue()
									+ "')");
						}
					}

					// ********* Page 8 - Discipline *********************//
					sheet = workbook.getSheetAt(8);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 3; i++)
					{
						rowIterator.next();
					}

					System.out.print(" Discipline:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						//disc_code, disc_title_en, disc_title_ar, field_code, field_title_en, field_title_ar
						query = "insert into asoc.discipline values (";
						query = query
							+ "\""
							+ row.getCell(6).getStringCellValue()
							+ "\", \""
							+ row.getCell(5).getStringCellValue().replaceAll("\n", " ")
							+ "\", \""
							+ row.getCell(19).getStringCellValue()
							+ "\", \""
							+ row.getCell(11).getStringCellValue()
							+ "\", \""
							+ row.getCell(10).getStringCellValue()
							+ "\", \""
							+ row.getCell(14).getStringCellValue()
							+ "\", "
							+ occup
							+ ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 9 - Qualifications *********************//
					sheet = workbook.getSheetAt(9);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 2; i++)
					{
						rowIterator.next();
					}

					System.out.print(" Qualif:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						row.getCell(5).setCellType(1);
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						//row.getCell(7).setCellType(1);  
						//disc_code, disc_title_en, disc_title_ar, field_code, field_title_en, field_title_ar
						query = "insert into asoc.qualification values (";
						query = query
							+ "\""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(7).getStringCellValue()
							+ "\", "
							+ occup
							+ ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 10 - Technical Skills *********************//
					sheet = workbook.getSheetAt(10);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 5; i++)
					{
						rowIterator.next();
					}

					System.out.print(" Skills:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						row.getCell(5).setCellType(1);
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						query = "insert into asoc.skills values (";
						for (i = 0; i < cols.length; i++)
						{
							col = cols[i];
							row.getCell(col).setCellType(1);
							query = query
								+ (row.getCell(cols[i]).getCellType() == 1 ? "\"" + row.getCell(cols[i]).getStringCellValue() + "\""
									: row.getCell(cols[i]).getNumericCellValue())
								+ ",";
						}
						query = query.substring(0, query.length() - 1) + ",'Technical'," + occup + ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 11 - Employability skills *********************//
					sheet = workbook.getSheetAt(11);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 3; i++)
					{
						rowIterator.next();
					}

					System.out.print(" Empl:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null || row.getCell(6) == null)
						{
							break;
						}
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						row.getCell(7).setCellType(1);
						row.getCell(11).setCellType(1);
						query = "insert into asoc.skills values (";
						query = query
							+ "\""
							+ row.getCell(6).getStringCellValue()
							+ "\", \""
							+ row.getCell(7).getStringCellValue()
							+ "\", \""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(11).getStringCellValue()
							+ "\",'Employability',"
							+ occup
							+ ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 12 - Work Experience *********************//
					sheet = workbook.getSheetAt(12);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 3; i++)
					{
						rowIterator.next();
					}

					System.out.print(" Exp:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						//row.getCell(9).setCellType(1);
						query = "insert into asoc.work_experience values (";
						query = query
							+ "\""
							+ row.getCell(6).getStringCellValue()
							+ "\", \""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(9).getStringCellValue()
							+ "\","
							+ occup
							+ ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 13 - Legal Requirements *********************//
					sheet = workbook.getSheetAt(13);
					rowIterator = sheet.iterator();
					if (rowIterator.hasNext())
					{
						for (i = 1; i <= 3; i++)
						{
							rowIterator.next();
						}

						System.out.print(" LegalR:"); 
						while (rowIterator.hasNext())
						{
							row = rowIterator.next();
							if (row.getCell(5) == null)
							{
								break;
							}
							if (row.getCell(5).getStringCellValue().length() < 2)
							{
								break;
							}
							//row.getCell(7).setCellType(1);
							query = "insert into asoc.occup_requirements values (";
							query = query
								+ "\""
								+ row.getCell(5).getStringCellValue()
								+ "\", \""
								+ row.getCell(7).getStringCellValue()
								+ "\", 'Legal', "
								+ occup
								+ ")";
							System.out.print(".");
							//System.out.println(query);
							stmt.executeUpdate(query);
						}
					}

					// ********* Page 14 - Gender & National Requirements *********************//
					sheet = workbook.getSheetAt(14);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 3; i++)
					{
						rowIterator.next();
					}

					System.out.print(" OtherR:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						//row.getCell(7).setCellType(1);
						query = "insert into asoc.occup_requirements values (";
						query = query
							+ "\""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(7).getStringCellValue()
							+ "\", 'Gender & National', "
							+ occup
							+ ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 16 - Language Skills *********************//
					sheet = workbook.getSheetAt(16);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 3; i++)
					{
						rowIterator.next();
					}

					System.out.print(" Languages:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						//row.getCell(7).setCellType(1);  
						//lang_code, lang_title_en, langauge_title_ar, field_code, field_title_en, field_title_ar
						//System.out.println(row.getCell(5).getStringCellValue());
						query = "insert into asoc.occup_languages values (";
						query = query
							+ "\""
							+ row.getCell(6).getStringCellValue()
							+ "\", \""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(19).getStringCellValue()
							+ "\", \""
							+ row.getCell(11).getStringCellValue()
							+ "\", \""
							+ row.getCell(10).getStringCellValue()
							+ "\", \""
							+ row.getCell(14).getStringCellValue()
							+ "\", "
							+ occup
							+ ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					// ********* Page 17 - Additional Requirements *********************//
					sheet = workbook.getSheetAt(17);
					rowIterator = sheet.iterator();
					for (i = 1; i <= 3; i++)
					{
						rowIterator.next();
					}

					System.out.print(" AdditR:"); 
					while (rowIterator.hasNext())
					{
						row = rowIterator.next();
						if (row.getCell(5) == null)
						{
							break;
						}
						if (row.getCell(5).getStringCellValue().length() < 2)
						{
							break;
						}
						//row.getCell(7).setCellType(1);
						query = "insert into asoc.occup_requirements values (";
						query = query
							+ "\""
							+ row.getCell(5).getStringCellValue()
							+ "\", \""
							+ row.getCell(7).getStringCellValue()
							+ "\", 'Additional', "
							+ occup
							+ ")";
						System.out.print(".");
						//System.out.println(query);
						stmt.executeUpdate(query);
					}

					file.close();
					workbook.close();
				}
			}
			System.out.println("\n\tStarting at: " + date);
			System.out.println("\tFinished at: " + new Date());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	/* Full clean of the staging database
	 * allows to re-generate the data
	 */
	public static void cleanDataFull() throws ClassNotFoundException, SQLException
	{
		String[] queries = {
			"delete FROM asoc.occupation2",
			"delete FROM asoc.occupation_accountability",
			"delete FROM asoc.accountability",
			"delete FROM asoc.education",
			"delete FROM asoc.occupation_education",
			"delete FROM asoc.skills",
			"delete FROM asoc.work_experience",
			"delete FROM asoc.discipline",
			"delete FROM asoc.qualification",
			"delete FROM asoc.example_job_titles",
			"delete FROM asoc.occup_languages",
			"delete FROM asoc.occupation_education",
			"delete FROM asoc.education",
			"delete FROM asoc.similar_occup" };

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

	/* Cleans test data for a given occupation (isco code) 
	 * allows to re-generate the data for that specific occupation
	 * in case of errors while parsing the data
	 */
	public static void cleanData(String occup) throws ClassNotFoundException, SQLException
	{
		String[] queries = {
			"delete FROM asoc.occupation2 where job_code='" + occup + "'",
			"delete FROM asoc.accountability where task_code in (select task_code from asoc.occupation_accountability where job_code='" + occup + "')",
			"delete FROM asoc.occupation_accountability where job_code='" + occup + "'",
			"delete FROM asoc.education where edu_code in (select edu_code from asoc.occupation_education where job_code='" + occup + "')",
			"delete FROM asoc.occupation_education where job_code='" + occup + "'"};
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

	/* Cleans test data for a given occupation (isco code) 
	 * allows to re-generate the data for that specific occupation
	 * in case of errors while parsing the data
	 */
	public static void processSectors() throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		ResultSet rs =null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		rs = stmt.executeQuery("SELECT distinct sector FROM taxonomies.sector_ar");
		String[] sectors;
		while (rs.next())
		{
			sectors = rs.getString("sector").split(";");
			for (int i = 0; i < sectors.length; i++)
				System.out.println(sectors[i] + ": \n\t--> " + stmt2.executeUpdate("insert into taxonomies.sectors (sector_name_en) values ('" + sectors[i].trim() + "')") + " records inserted");
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
	public static void ProcessMatching(String filepath) throws ClassNotFoundException, SQLException, IOException
	{
		String query = "";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		String jobId = "";
		try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			String everything = sb.toString();
			String[] result = everything.split("####");
			for (int x=0; x<result.length; x++) {
				//System.out.println("'" + result[x] + "'");
				StringTokenizer st = new StringTokenizer(result[x].trim());
				String query1="";
				if (st.hasMoreTokens()) {
					jobId = st.nextToken();
					query1 = "insert into matchtest.matching (job_id, candidate_id, match_type, match_score, match_dir, elise_version) values ('" + jobId + "','";
					System.out.print("\n" + jobId + ": ");
				}

				while (st.hasMoreTokens()) {
					query = query1 + st.nextToken() + "','MatchOwn'," + st.nextToken() + ",'Job->JS','6.6')";
					System.out.print(".");
					//System.out.println(query);
					stmt.executeUpdate(query);
					st.nextToken();
				}
			}
		}
		query = "create table matchtest.matchOwn66 (SELECT job_id, sum(match_score)/count(candidate_id) avgMatchOwn, count(candidate_id) counts FROM matchtest.matching where match_type='MatchOwn' and elise_version= '6.6' group by job_id)";
		stmt.executeUpdate(query);
		stmt.close();
		conn.close();
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