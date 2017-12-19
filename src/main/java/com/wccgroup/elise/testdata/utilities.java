/**
 * @author abenabdelkader
 *
 * testing.java
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

public class utilities
{
	private final String USER_AGENT = "Mozilla/5.0";
	static Connection conn = null;
	static Statement stmt = null, stmt2 = null;

	public static void main(String[] args) throws IOException
	{
		bisComptenceToWCC();
		//bisSkillSynonymsToNiri();
		//bisComptenceToNiri();
		xlsx2csv1();
		//xlsx2csv2();
		//xlsx2csv3();
		//occupation_affinities();
		//occupation_list();
		//occupation_list_HRDF();
		//GenerateNames_ar();
		//xlsx2DB("P:\\HRDF - ASOC\\RFP Q4 2015\\ASOC documents\\ASOC Index English and Arabic 20150831 (4)_REV_final.xlsx", 0, 5, 1);
		//CheckSimilars();
		//DBcounts("bisams");
		BIS2xlsxPoolParty("Bauausschreibung", null, 0, 0, 0);
	}

	public static void DBcounts(String dbName)
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/" + dbName, "root", "");
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			ResultSet rs = null;
			ResultSet rs2 = null;

			System.out.println(new Date());
			rs = stmt.executeQuery("show tables");
			int i = 1;
			while (rs.next())
			{
				rs2 = stmt2.executeQuery("select count(*) from " + rs.getString(1));
				if (rs2.next())
					System.out.println(i++ + "- " + rs.getString(1) + ": \n\t -->" 
				+ rs2.getString(1) + " records");
				if (rs2.getInt(1) == 0)
					stmt2.executeUpdate("drop table " + rs.getString(1));
			}

			System.out.println("\nFinished at: " + new Date());
			rs.close();
			stmt.close();
			rs2.close();
			stmt2.close();
			conn.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void occupation_list()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();
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

	public static void occupation_list_HRDF()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();
			ResultSet rs = null;

			System.out.println(new Date());
			String filepath = "c:\\ELISE6.6\\dictionaries\\HRDFDictionary1.1\\list\\ct_occu_new.ed";
			PrintWriter writer = new PrintWriter(filepath);
			// query to retrieve occupations
			String query = "SELECT job_code, left(occup_code,4) occup_code, job_title_en, job_title_ar FROM asoc.occupation";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString("job_code")
						+ "\", \""
						+ rs.getString("occup_code")
						+ "\"\n\t@en, \""
						+ rs.getString("job_title_en")
						+ "\"\n\t@ar, \""
						+ rs.getString("job_title_ar")
						+ "\"\n");
			}

			System.out.println("\tgenerated list at: " + filepath + "\nFinished at: " + new Date());
			writer.close();

			filepath = "c:\\ELISE6.6\\dictionaries\\HRDFDictionary1.1\\list\\ct_sec_new.ed";
			System.out.println(new Date());
			writer = new PrintWriter(filepath);
			// query to retrieve occupations
			query = "SELECT distinct sector, sector_ar FROM taxonomies.sector_ar;";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString("sector").replaceAll(" ", "").replaceAll(";", "")
						+ "\"\n\t@en, \""
						+ rs.getString("sector")
						+ "\"\n\t@ar, \""
						+ rs.getString("sector_ar")
						+ "\"");
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


	public static void education_list_HRDF()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();
			ResultSet rs = null;

			System.out.println(new Date());
			String filepath = "c:\\ELISE\\Dictionaries\\HRDFDictionary\\list\\ct_maji_ammar.ed";
			PrintWriter writer = new PrintWriter(filepath);
			// query to retrieve occupations
			String query = "SELECT distinct disc_code, disc_title_en, disc_title_ar FROM asoc.discipline order by disc_code";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString("disc_code")
						+ "\"\n\t@en, \""
						+ rs.getString("disc_title_en")
						+ "\"\n\t@ar, \""
						+ rs.getString("disc_title_ar")
						+ "\"");
			}

			System.out.println("\tgenerated list at: " + filepath + "\nFinished at: " + new Date());
			writer.close();

			filepath = "c:\\ELISE\\Dictionaries\\HRDFDictionary\\list\\ct_majii_ammar.ed";
			System.out.println(new Date());
			writer = new PrintWriter(filepath);
			// query to retrieve occupations
			query = "SELECT distinct disc_code, disc_title_en, disc_title_ar, field_code, field_title_en, field_title_ar FROM asoc.discipline where length(field_title_en)>3 order by disc_code, field_title_en";
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				writer.println(
					"\""
						+ rs.getString("sector").replaceAll(" ", "").replaceAll(";", "")
						+ "\"\n\t@en, \""
						+ rs.getString("sector")
						+ "\"\n\t@ar, \""
						+ rs.getString("sector_ar")
						+ "\"");
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

	public static void occupation_affinities()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
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

	public static void GenerateNames_ar()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");
			String[][] Names = new String[200][7];

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/Employment", "root", "");
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();

			// JDBC2Elise data type mappings
			FileInputStream file = new FileInputStream(
				new File("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\Arabic Names.xlsx"));
			PrintWriter writer = new PrintWriter(
				"\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\Arabic Names.txt",
				"UTF-8");

			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			String query = "";
			rowIterator.next(); //rowIterator.next();
			//PrintWriter writer = new PrintWriter("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\Mol list2.csv", "UTF-8");
			int i = 0;
			while (rowIterator.hasNext())
			{
				//rowIterator.next();
				Row row = rowIterator.next();
				//For each row, iterate through all the columns
				Names[i][0] = row.getCell(2).getStringCellValue();
				Names[i][1] = row.getCell(3).getStringCellValue();
				Names[i][2] = row.getCell(4).getStringCellValue();
				Names[i][3] = row.getCell(8).getStringCellValue();
				Names[i][4] = row.getCell(9).getStringCellValue();
				Names[i][5] = row.getCell(10).getStringCellValue();
				Names[i][6] = row.getCell(5).getStringCellValue();
				i++;

				System.out.println(query);
				//stmt.executeUpdate(query);
				//writer.println(query);
			}
			Random rn = new Random();
			int nbre = 0;
			query = "SELECT candidate_id, language FROM employment.candidate";
			//String query = "SELECT distinct job_title FROM test.job";
			ResultSet rs = null;
			rs = stmt.executeQuery(query);
			//int i=1;
			while (rs.next())
			{
				nbre = rn.nextInt(i);
				//stmt2.executeUpdate("Update Employment.Candidate set first_name='" + Names [nbre][3] + "', middle_name= '" + Names [rn.nextInt(i)][4] + "', last_name='" + Names [rn.nextInt(i)][5]  + "', gender='" + Names [nbre][6] +"' where candidate_id=" + rs.getString("candidate_id"));
				stmt2.executeUpdate("SET NAMES 'utf8'");
				if (rs.getString("language").equalsIgnoreCase("en"))
				{
					System.out.println(
						"Update Employment.Candidate set first_name='"
							+ Names[nbre][3]
							+ "', middle_name= '"
							+ Names[rn.nextInt(i)][4]
							+ "', last_name='"
							+ Names[rn.nextInt(i)][5]
							+ "', gender='"
							+ Names[nbre][6]
							+ "' where candidate_id="
							+ rs.getString("candidate_id"));
					stmt2.executeUpdate(
						"Update Employment.Candidate set first_name=\""
							+ Names[nbre][3]
							+ "\", middle_name= \""
							+ Names[rn.nextInt(i)][4]
							+ "\", last_name=\""
							+ Names[rn.nextInt(i)][5]
							+ "\", gender='"
							+ Names[nbre][6]
							+ "' where candidate_id="
							+ rs.getString("candidate_id"));
				}
				else
				{
					//stmt2.executeUpdate ("Update Employment.Candidate set first_name='" + Names [nbre][0] + "', middle_name= '" + Names [rn.nextInt(i)][1] + "', last_name='" + Names [rn.nextInt(i)][2]  + "', gender='" + Names [nbre][6] +"' where candidate_id=" + rs.getString("candidate_id"));
					writer.write(
						"Update Employment.Candidate set first_name='"
							+ Names[nbre][0]
							+ "', middle_name= '"
							+ Names[rn.nextInt(i)][1]
							+ "', last_name='"
							+ Names[rn.nextInt(i)][2]
							+ "', gender='"
							+ Names[nbre][6]
							+ "' where candidate_id="
							+ rs.getString("candidate_id")
							+ ";\n");
				}

			}
			//writer.close();	
			System.out.println(new Date());
			file.close();
			writer.close();
			workbook.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void xlsx2DB(String path, int page, int cols, int ignore)
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/asoc?useUnicode=true&characterEncoding=utf-8", "root", "");
			stmt = conn.createStatement();

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

	public static void xlsx2csv1()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();

			// JDBC2Elise data type mappings
			FileInputStream file = new FileInputStream(
				new File("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\Mol list2.xlsx"));

			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			String query = "";
			rowIterator.next();
			rowIterator.next();
			PrintWriter writer = new PrintWriter(
				"\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\Mol list2.csv",
				"UTF-8");
			while (rowIterator.hasNext())
			{
				//rowIterator.next();
				Row row = rowIterator.next();
				//For each row, iterate through all the columns
				//System.out.println("cell type: "  + row.getCell(0).getCellType() + ", "  + row.getCell(1).getCellType() + ", "  + row.getCell(2).getCellType() + ", "  + row.getCell(3).getCellType() + ", "  + row.getCell(4).getCellType() + ", "  + row.getCell(5).getCellType() + ", ");
				query = (row.getCell(0).getCellType() == 1 ? row.getCell(0).getStringCellValue()
					: row.getCell(0).getNumericCellValue())
					+ ";"
					+ (row.getCell(1).getCellType() == 1 ? row.getCell(1).getStringCellValue()
						: row.getCell(1).getNumericCellValue())
					+ ";"
					+ (row.getCell(2).getCellType() == 1 ? row.getCell(2).getStringCellValue()
						: row.getCell(2).getNumericCellValue())
					+ ";\""
					+ row.getCell(3).getStringCellValue()
					+ "\";\""
					+ row.getCell(4).getStringCellValue()
					+ "\";\""
					+ row.getCell(5).getStringCellValue()
					+ "\";";
				/*	        	query = "insert into taxonomies.occupations_ar values ("  + (row.getCell(0).getCellType()==1?row.getCell(0).getStringCellValue():row.getCell(0).getNumericCellValue()) + ", "  
										   + (row.getCell(1).getCellType()==1?row.getCell(1).getStringCellValue():row.getCell(1).getNumericCellValue()) + ", "  
										   + (row.getCell(2).getCellType()==1?row.getCell(2).getStringCellValue():row.getCell(2).getNumericCellValue()) + ", '"  
					        	+ row.getCell(3).getStringCellValue() + "', '"  + row.getCell(4).getStringCellValue() + "', \""  + row.getCell(5).getStringCellValue() + "\");";
				*/ System.out.println(query);
				//stmt.executeUpdate(query);
				writer.println(query);
			}
			writer.close();
			System.out.println(new Date());
			file.close();
			workbook.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void bisComptenceToNiri()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();

			// JDBC2Elise data type mappings
			FileInputStream file = new FileInputStream(
				new File("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\AMS\\skills_detail.xlsx"));

			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			String query = "";
			stmt.executeUpdate("delete from niri.skills_uni");
			rowIterator.next();
			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				row.getCell(2).setCellType(1);
				row.getCell(3).setCellType(1);
				query = "insert into niri.skills_uni values ('"  + row.getCell(2).getStringCellValue() + "', \""
					+ row.getCell(0).getStringCellValue()  + "\", \""  
					+ row.getCell(1).getStringCellValue()  + "\", "  
					+ row.getCell(3).getStringCellValue()  + ")";  
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
	 * This module creates a new data out of the BIS taxonomy, it seeks the simplification of the data structures.
	 * In this first case about the competences we merge tables 'stammdaten' and 'spezielle' into one table called 'occupation'
	 * and we merge tables 'qualifikation' and 'qualifikation_detail' into 'competence'
	 */

	public static void bisComptenceToWCC()
	{
		try
		{

			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();
			String[][] queries = {{"create database if not exists newbis","create database"},
				{"drop table if exists newbis.occupation_competence","drop table occupation_competence"},
				{"drop table if exists newbis.occupation","drop table occupation"},
				{"drop table if exists newbis.competence","drop table competence"},
				{"CREATE table newbis.occupation (SELECT distinct concat(noteid,'C') occupation_id, bezeichnung designation, concat(stammdaten_noteid,'P') parent, 0 deleted, 2 webstatus FROM bisams.spezielle)","create table occupation with data from spezielle"},
				{"ALTER TABLE newbis.occupation CHANGE COLUMN occupation_id occupation_id VARCHAR(12) NOT NULL,ADD PRIMARY KEY (occupation_id)","alter table occupation"},
				{"insert into newbis.occupation (SELECT distinct concat(noteid,'P') occupation_id, bezeichnung, null parent, deleted, webstatusext_noteid webstatus FROM bisams.stammdaten)","insert into table occupation with data from stammdaten"},
				{"CREATE table newbis.competence (SELECT distinct concat(noteid,'C') competence_id, bezeichnung designation, concat(thesid,'C') child, concat(parent_thesid,'C') parent, concat(qualifikation_noteid,'P') group_parent, zertifikat certificat, ebene level, deleted, 2 webstatus FROM bisams.qualifikation_detail)","create table competence with data from qualifikation_detail"},
				{"ALTER TABLE newbis.competence CHANGE COLUMN competence_id competence_id VARCHAR(12) NOT NULL,ADD PRIMARY KEY (competence_id)","alter table competence"},
				{"INSERT into newbis.competence (SELECT distinct concat(noteid,'P') competence_id, bezeichnung, concat(thesid,'P') child, '', '', zertifikat, 0 level, deleted, webstatus_noteid webstatus FROM bisams.qualifikation)","insert into table competence with data from qualifikation"},
				{"CREATE table newbis.occupation_competence (SELECT distinct concat(stammdaten_noteid,'P') occupation_id, concat(qualifikation_noteid,'P') competence_id, 'Group' competence_type, deleted FROM bisams.stammdaten_qualifikation)","create table occupation_competence with data from stammdaten_qualifikation"},
				{"ALTER TABLE newbis.occupation_competence CHANGE COLUMN competence_type competence_type VARCHAR(45)","alter table occupation_competence competence_type"},
				{"ALTER TABLE newbis.occupation_competence CHANGE COLUMN occupation_id occupation_id VARCHAR(12) NOT NULL,ADD PRIMARY KEY (occupation_id, competence_id, competence_type)","alter table occupation_competence ADD PKEY"},
				{"ALTER TABLE newbis.occupation_competence ADD CONSTRAINT fk_occupation FOREIGN KEY (occupation_id) REFERENCES newbis.occupation (occupation_id) ON DELETE NO ACTION ON UPDATE NO ACTION","alter table occupation_competence ADD FK occupation"},
				{"ALTER TABLE newbis.occupation_competence ADD CONSTRAINT fk_competence FOREIGN KEY (competence_id) REFERENCES newbis.competence (competence_id) ON DELETE NO ACTION ON UPDATE NO ACTION","alter table occupation_competence ADD FK competence"},

				{"insert into newbis.occupation_competence (SELECT distinct concat(stammdaten_noteid,'P') occupation_id, concat(qualifikation_noteid,'P') competence_id, 'Group Basis' competence_type, 0 deleted FROM bisams.stammdaten_qualifikation_basis)","insert into table occupation_competence with data from stammdaten_qualifikation_basis"},
				{"insert into newbis.occupation_competence (SELECT distinct concat(spezielle_noteid,'C') occupation_id, concat(qualifikation_noteid,'P') competence_id, 'Group' competence_type, 0 deleted FROM bisams.qualifikation_spezielle where spezielle_noteid in (select noteid from bisams.spezielle))","insert into table occupation_competence with data from qualifikation_spezielle"},
				{"insert into newbis.occupation_competence (SELECT distinct concat(stammdaten_noteid,'P') occupation_id, concat(qualifikation_detail_noteid,'C') competence_id, 'Detail' competence_type, deleted FROM bisams.stammdaten_qualifikation_detail)","insert into table occupation_competence with data from stammdaten_qualifikation_detail"},
				{"insert into newbis.occupation_competence (SELECT distinct concat(stammdaten_noteid,'P') occupation_id, concat(qualifikation_detail_noteid,'C') competence_id, 'Detail Basis' competence_type, 0 deleted FROM bisams.stammdaten_qualifikation_detail_basis)","insert into table occupation_competence with data from stammdaten_qualifikation_detail_basis"},
				{"insert into newbis.occupation_competence (SELECT distinct concat(spezielle_noteid,'C') occupation_id, concat(qualifikation_detail_noteid,'C') competence_id, 'Detail' competence_type, 0 deleted FROM bisams.qualifikation_detail_spezielle where spezielle_noteid in (select noteid from bisams.spezielle))","insert into table occupation_competence with data from qualifikation_detail_spezielle"},
				};


			for (int i=0; i<queries.length; i++)
			{
				System.out.print(queries[i][1] + "\n\t--> ");
				System.out.println(stmt.executeUpdate(queries[i][0]));
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void bisSkillSynonymsToNiri()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
			stmt = conn.createStatement();

			// JDBC2Elise data type mappings
			FileInputStream file = new FileInputStream(
				new File("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\AMS\\skill_synonyms.xlsx"));

			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			String query = "";
			stmt.executeUpdate("delete from niri.skill_synonyms");
			rowIterator.next();
			while (rowIterator.hasNext())
			{
				Row row = rowIterator.next();
				row.getCell(0).setCellType(1);
				row.getCell(1).setCellType(1);
				query = "insert into niri.skill_synonyms values ('"  + row.getCell(0).getStringCellValue() + "', '"
					+ row.getCell(1).getStringCellValue()  + "', \""  
					+ row.getCell(2).getStringCellValue()  + "\", \""  
					+ row.getCell(3).getStringCellValue()  + "\")";  
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

	public static void xlsx2csv2()
	{
		try
		{

			// JDBC2Elise data type mappings
			FileInputStream file = new FileInputStream(
				new File(
					"\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\ASOC Index English and Arabic 20150831 (4)_REV_final.xlsx"));

			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			String query = "";
			rowIterator.next();
			PrintWriter writer = new PrintWriter(
				"\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\ASOC Index English and Arabic 20150831 (4)_REV_final.csv",
				"UTF-8");
			while (rowIterator.hasNext())
			{
				//rowIterator.next();
				Row row = rowIterator.next();
				//For each row, iterate through all the columns
				query = (row.getCell(0).getCellType() == 1 ? row.getCell(0).getStringCellValue()
					: row.getCell(0).getNumericCellValue())
					+ ";"
					+ row.getCell(1).getStringCellValue()
					+ ";"
					+ row.getCell(2).getStringCellValue()
					+ ";\""
					+ row.getCell(3).getStringCellValue()
					+ "\";\""
					+ row.getCell(4).getStringCellValue()
					+ "\";";
				System.out.println(query);
				writer.println(query);
			}
			writer.close();
			System.out.println(new Date());
			file.close();
			workbook.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void xlsx2csv3()
	{
		try
		{

			// JDBC2Elise data type mappings
			FileInputStream file = new FileInputStream(
				new File(
					"\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\T_20150911_ASOC Directory 11 0 (including Sectors).xlsx"));

			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			String query = "";
			rowIterator.next();
			PrintWriter writer = new PrintWriter(
				"\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\T_20150911_ASOC Directory 11 0 (including Sectors).csv",
				"UTF-8");
			while (rowIterator.hasNext())
			{
				//rowIterator.next();
				Row row = rowIterator.next();
				//For each row, iterate through all the columns
				query = (row.getCell(0).getCellType() == 1 ? row.getCell(0).getStringCellValue()
					: row.getCell(0).getNumericCellValue())
					+ ";"
					+ row.getCell(1).getStringCellValue()
					+ ";"
					+ row.getCell(2).getStringCellValue()
					+ ";\""
					+ row.getCell(3).getStringCellValue()
					+ "\";\""
					+ row.getCell(4).getStringCellValue()
					+ "\";";
				System.out.println(query);
				writer.println(query);
			}
			writer.close();
			System.out.println(new Date());
			file.close();
			workbook.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void CheckSimilars()
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/employment", "root", "");
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			ResultSet rs = null, rs2 = null;

			String query = "SELECT distinct occupation FROM employment.candidate_ambitions";
			rs = stmt.executeQuery(query);
			while (rs.next())
			{
				String occupation = rs.getString("occupation");

				query = "SELECT distinct synonym FROM taxonomies.occupations_ar_synonyms where isco_code='" + occupation + "'";

				System.out.println(query);
				rs2 = stmt2.executeQuery(query);
				Map<Integer, String> similars = new HashMap<Integer, String>();
				similars.put(0, occupation);
				for (int j = 1; rs2.next(); j++)
				{
					//similars.put(j, rs2.getString(2) + "##" + rs2.getString(1));
					similars.put(j, rs2.getString(1));
					System.out.println("\t - " + occupation + "##" + rs2.getString(1)); // + "##" + rs2.getString(3) + "##" + rs2.getString(4)); 
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

	public static void BIS2xlsxPoolParty(String qual, String path, int page, int cols, int ignore)
	{
		try
		{
	
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");
	
			//STEP 3: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/asoc?useUnicode=true&characterEncoding=utf-8", "root", "");
			stmt = conn.createStatement();
	
			String query;

			System.out.println("scheme\tconcept\tconcept\tconcept\tconcept\tnotation\nCompetencies");
			query = "SELECT aa.noteid Id, aa.bezeichnung Parent, bb.noteid Id1, bb.bezeichnung Child1, cc.noteid Id2, cc.bezeichnung Child2, dd.noteid Id3, dd.bezeichnung Child3"
				+ " FROM bisams.qualifikation_detail aa"
				+ " LEFT OUTER JOIN bissimple.qualifikation_detail bb"
				+ " ON aa.thesid=bb.parent_thesid"
				+ " LEFT OUTER JOIN bissimple.qualifikation_detail cc"
				+ " ON bb.thesid=cc.parent_thesid"
				+ " LEFT OUTER JOIN bissimple.qualifikation_detail dd"
				+ " ON cc.thesid=dd.parent_thesid where not aa.deleted"
				//and (aa.bezeichnung = '"
				//+ qual + "' or aa.noteid ='" + qual + "')"
				+ " order by Parent, Child1, Child2, Child3 limit 1000";
			System.out.println(query);
			ResultSet rs = stmt.executeQuery(query);
			String parent = "", child1 = "", child2 = "";
			for (; rs.next();)
			{
				if (!parent.equalsIgnoreCase(rs.getString("Parent")))
					System.out.println("\t" + rs.getString("Parent") + "\t\t\t\t" + rs.getString("id"));
				parent = rs.getString("Parent");
				if (rs.getString("Child1") != null)
				{
					if (!child1.equalsIgnoreCase(rs.getString("Child1")))
						System.out.println("\t\t" + rs.getString("Child1") + "\t\t\t" + rs.getString("id1"));
					child1 = rs.getString("Child1");
					if (rs.getString("Child2") != null)
					{
						if (!child2.equalsIgnoreCase(rs.getString("Child2")))
							System.out.println("\t\t\t" + rs.getString("Child2") + "\t\t" + rs.getString("id2"));
						child2 = rs.getString("Child2");
						if (rs.getString("Child3") != null)
							System.out.println("\t\t\t\t" + rs.getString("Child3") + "\t" + rs.getString("id3"));
					}
				}
			}
/*
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
	*/
		rs.close();
		stmt.close();
		conn.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	//}

}