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

public class utilities_asoc3
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
			System.out.println("\t- 1- Process new ASOC Occupations/Jobs");
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
			System.out.println("\t- 14- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			String path = "P:\\HRDF - ASOC\\ASOC_Taxonomy_v1.xlsx";
			//processASOCoccupations(path); //both occupations and jobs
			//processASOCeducation(path); 
			processASOCsector(path); 
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
		default:
			System.out.println("Bye"); // quit

		}

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


	public static void processASOCoccupations(String path)
	{
		int page = 1;
		int ignore = 1;
		String query = "";
		int i = 1;
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();

			FileInputStream file = new FileInputStream(new File(path));
			System.out.println(new Date());
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			/********************* Process Occupations *************************/
			page = 1; //occupation page on the excel sheet
			XSSFSheet sheet = workbook.getSheetAt(page);
			Iterator<Row> rowIterator = sheet.iterator();
			for (i = 1; i <= ignore; i++)
			{
				rowIterator.next();
			}

			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				query = "insert into asoc3.occupation values (";
				for (i = 4; i < 9; i++)
				{
					query = query
						+ (row.getCell(i).getCellType() == 1 ? "\"" + row.getCell(i).getStringCellValue().replaceAll("\"", "'").trim() + "\""
							: (int) row.getCell(i).getNumericCellValue())
						+ ",";
				}
				query = query.substring(0, query.length() - 1) + ")";
				System.out.println(query);
				stmt.executeUpdate(query);
			}


			/********************* Process Jobs *************************/
			
			page = 4; //jobs page on the excel sheet
			sheet = workbook.getSheetAt(page);
			rowIterator = sheet.iterator();
			for (i = 1; i <= ignore; i++)
			{
				rowIterator.next();
			}
			int job_index=1;
			int occup_code=0;

			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				query = "insert into asoc3.job (occup_code, job_code, job_title_en, job_title_ar) values (";
				if (occup_code!=(int) row.getCell(0).getNumericCellValue()) {
					job_index=1;
					occup_code = (int) row.getCell(0).getNumericCellValue();
				}
				else
					job_index++;
				
					query = query + occup_code + ",\"" + occup_code + "-" + job_index + "\",\"" 
						+ row.getCell(2).getStringCellValue().replaceAll("\"", "'").trim() + "\", \""
						+ row.getCell(3).getStringCellValue().replaceAll("\"", "'").trim() + "\")";
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

	public static void processASOCeducation(String path)
	{
		int page = 10;
		int ignore = 1;
		String query = "";
		int i = 1;
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();

			FileInputStream file = new FileInputStream(new File(path));
			System.out.println(new Date());
			XSSFWorkbook workbooke = new XSSFWorkbook(file);

			/********************* Process Education *************************/
			page = 9; //education level page on the excel sheet
			XSSFSheet sheet = workbooke.getSheetAt(page);
			Iterator<Row> rowIterator = sheet.iterator();
			for (i = 1; i <= ignore; i++)
			{
				rowIterator.next();
			}

			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				query = "insert into asoc3.education (edu_code, edu_title_en, edu_title_ar, edu_parent) values (\"";
				
					query = query 
						+ row.getCell(0).getStringCellValue().replaceAll("\"", "'").trim() + "\", \""
						+ row.getCell(1).getStringCellValue().replaceAll("\"", "'").trim() + "\", \""
						+ row.getCell(2).getStringCellValue().replaceAll("\"", "'").trim() + "\", \""
						+ (int) row.getCell(3).getNumericCellValue() + "\")";
				//System.out.println(query);
				stmt.executeUpdate(query);
			}

			/********************* Process Occupation-Education Links *************************/
			page = 1; //education level page on the excel sheet
			sheet = workbooke.getSheetAt(page);
			rowIterator = sheet.iterator();
			for (i = 1; i <= ignore; i++)
			{
				rowIterator.next();
			}

			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				for (i = 22; i < 25; i++)
				{
					query = "insert into asoc3.occupationeducation (occup_code, edu_code) values (\"";
					//if (row.getCell(i).getNumericCellValue()=null) {
						query = query 
							+ (int) row.getCell(4).getNumericCellValue() + "\", \""
							//+ row.getCell(i).getStringCellValue() + "\")";
							+ (row.getCell(i).getCellType() == 1 ? row.getCell(i).getStringCellValue() 
							: (int) row.getCell(i).getNumericCellValue())
							+ "\")";
						//System.out.println(query);
						if (!query.contains("\"\""))
							stmt.executeUpdate(query);
					//}
				}
			}

			System.out.println(new Date());
			file.close();
			workbooke.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void processASOCsector(String path)
	{
		int page = 5;
		int ignore = 1;
		String query = "";
		int i = 1;
		try
		{

			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();

			FileInputStream file = new FileInputStream(new File(path));
			System.out.println(new Date());
			XSSFWorkbook workbooke = new XSSFWorkbook(file);

			/********************* Process Sector *************************/
			page = 5; //education level page on the excel sheet
			XSSFSheet sheet = workbooke.getSheetAt(page);
			Iterator<Row> rowIterator = sheet.iterator();
			for (i = 1; i <= ignore; i++)
			{
				rowIterator.next();
			}

			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				query = "insert into asoc3.sector values (\"";
				
					query = query 
						+ (int)row.getCell(0).getNumericCellValue() + "\", \""
						+ row.getCell(1).getStringCellValue().replaceAll("\"", "'").trim() + "\", \""
						+ row.getCell(2).getStringCellValue().replaceAll("\"", "'").trim() + "\")";
				//System.out.println(query);
				stmt.executeUpdate(query);
			}

			/********************* Process Occupation-Sector Links *************************/
			page = 6; //education level page on the excel sheet
			sheet = workbooke.getSheetAt(page);
			rowIterator = sheet.iterator();
			for (i = 1; i <= ignore; i++)
			{
				rowIterator.next();
			}

			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				query = "insert into asoc3.occupationsector values (\""
					+ (int) row.getCell(0).getNumericCellValue() + "\", \"";
				for (i = 2; i < 10; i++)
				{
					if (row.getCell(i)!=null) {
						query = query 
							+ (row.getCell(i).getCellType() == 1 ? row.getCell(i).getStringCellValue() 
							: (int) row.getCell(i).getNumericCellValue())
							+ ",";
					}
				}
				query = query.substring(0, query.length()-1) + "\")";
				//System.out.println(query);
				stmt.executeUpdate(query);
			}

			System.out.println(new Date());
			file.close();
			workbooke.close();
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