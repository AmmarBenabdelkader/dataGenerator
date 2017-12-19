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
import java.util.*;
import java.util.Date;
import org.apache.poi.xssf.usermodel.*;

/*
 * BIS to ETM (EliseTaxonomy Manager) is a tool that converts data from BIS to a format that can be consumed by ETM
 * BIS_to_ETM provides the following functionalities:
 * occupation generator,
 * competences generator
 * etc.
 */
public class Bis2eTM
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException
	{

		readProperties(); //load database properties
		String input = "";
		ArrayList<String> options = new ArrayList<>();
		options.add("0");
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

		while (true)
		{
			System.out.printf("Please select the action to perform from the following:\n");
			System.out.println("\t- 1- Explore AMS Conpetences");
			System.out.println("\t- 2- Explore AMS Categories");
			System.out.println("\t- 3- Explore AMS Categories Job Counts");
			System.out.println("\t- 4- Explore BIS Qualifications");
			System.out.println("\t- 5- Explore BIS Qualifications Detail");
			System.out.println("\t- 6- Explore BIS Affinities");
			System.out.println("\t- 7- Explore Occupation");
			System.out.println("\t- 8- Statistics");
			System.out.println("\t- 9- Explore AMS Categories SKILL Counts");
			System.out.println("\t- 11- BIS to ETM (xlsx)");
			System.out.println("\t- 12- BIS to ETM (CSV)");
			System.out.println("\t- 10- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			BIStoETM_xls("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\BIS Occupations.xlsx", "all"); 
			break;
		case "2":
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BIS\\", "E");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BIS\\", "relations");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BIS\\", "O");  
/*			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BIS\\", "C");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BISdata\\", "WE");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BISdata\\", "OI");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BISdata\\", "A");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BISdata\\", "K");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BIS\\", "relationsWithTwoScores");  
			BIStoETM_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\TM\\BIS\\", "labels");  
*/			break;
		case "11":
			exploreCompetences("DrogistIn"); //BarkeeperIn //LederfärberIn Koch, Köchin DatenerfasserIn LederfärberIn Java-ProgrammiererIn "Pfarrerskoch, Pfarrersköchin" 
			break;
		case "12":
			exploreCategories();
			break;
		case "3":
			exploreCategoriesCounts();
			break;
		case "4":
			exploreQualifications2();
			break;
		case "5":
			exploreQualificationsDetail("Java"); //MS Office//Java//Nahrungsmittel//87//"Bauplanungskenntnisse"); 
			break;
		case "6":
			exploreAffinities(); //"Bauplanungskenntnisse"); 
			break;
		case "7":
			exploreBISOccupation("DrogistIn"); //Software-EntwicklerIn//BarkeeperIn//RaumpflegerIn//TischlerIn//LagerarbeiterIn//Küchenhilfskraft//SchlosserIn im Metallbereich//ElektroinstallationstechnikerIn//Einzelhandelskaufmann/-frau//LebensmittelverkäuferIn//TaxichauffeurIn//Koch, Köchin //Restaurantfachmann/-frau//"Bauplanungskenntnisse"); 
			break;
		case "8":
			DBcounts("bissimple3"); // perform statistics on the data
			break;
		case "9":
			exploreCategoriesSkillCounts();
			break;
		default:
			System.out.println("Bye"); // quit

		}

	}

	/*
	 * This module explores the BIS competences for a given 'specialized' job titles.
	 * By looking into the BIS data models, it seems like there are many ways to retrieve the competences.
	 * Some of the links are direct (qualifikation_detail) and some others are indirect (qualifikation, stammdaten, sechssteller)
	 * This module explores the competences of a given 'specialized' job title (using different paths)
	 */
	public static void exploreCompetences(String jobTitle) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmt3 = null;
		ResultSet rs = null, rs2 = null, rs3 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		stmt3 = conn.createStatement();
		int s_noteid = 0, stamm_noteid = 0;
		Map<Integer, String> qualDic = new HashMap<Integer, String>(); // Dictionary to hold distinct qualifications 
		System.out.println(" *** Spezielle: " + jobTitle + " *** ");
		String query = "SELECT * FROM bisams.spezielle where bezeichnung='" + jobTitle + "'"; 
		rs = stmt.executeQuery(query);
		if (rs.next())
		{
			s_noteid = rs.getInt("noteid");
			stamm_noteid = rs.getInt("stammdaten_noteid");
		}
		else
			return;

		//System.out.println(query); 

		System.out.println("Path 1: Detailed Qualifications");
		query = "SELECT * FROM bisams.qualifikation_detail where not deleted and noteid in (select qualifikation_detail_noteid from bisams.qualifikation_detail_spezielle where spezielle_noteid ="
			+ s_noteid
			+ ")";
		rs = stmt.executeQuery(query);
		for (; rs.next();) {
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(4) + " (level " + rs.getString("ebene") + ")");
		}

		System.out.println("Path 2: Qualifications");
		query = "SELECT * FROM bisams.qualifikation where  NOT deleted AND webstatus_noteid=2 and noteid in (select qualifikation_noteid from bisams.qualifikation_spezielle where spezielle_noteid ="
			+ s_noteid
			+ ")";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			qualDic.put(rs.getInt("noteid"), rs.getString("bezeichnung"));
			System.out.println("\t- " + rs.getString(1) + ": " + rs.getString(6));

			query = "SELECT * FROM bisams.qualifikation_detail where not deleted and qualifikation_noteid ="
				+ rs.getString(1)
				+ " order by ebene";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				//qualDic.put(rs2.getInt("noteid"), rs2.getString("bezeichnung"));
				System.out.println(
					"\t\t- "
						+ rs2.getString("noteid")
						+ ": "
						+ rs2.getString("bezeichnung")
						+ " (level "
						+ rs2.getString("ebene")
						+ ")");
		}

		System.out.println("Path 3: (Detailed Qualifications via Stammdaten)");
		query = "SELECT * FROM bisams.stammdaten where not deleted AND webstatusext_noteid=2 and noteid=" + stamm_noteid;
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t- " + rs.getString("noteid") + ": " + rs.getString("bezeichnung"));
			query = "SELECT * FROM bisams.qualifikation_detail where not deleted and qualifikation_noteid in (select qualifikation_detail_noteid from bisams.stammdaten_qualifikation_detail where stammdaten_noteid ="
				+ rs.getString("noteid")
				+ ")";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				//qualDic.put(rs2.getInt("noteid"), rs2.getString("bezeichnung"));
				System.out.println(
					"\t\t- "
						+ rs2.getString("noteid")
						+ ": "
						+ rs2.getString("bezeichnung")
						+ " (level "
						+ rs2.getString("ebene")
						+ ")");
		}

		System.out.println("Path 4: (Qualifications via Stammdaten)");
		query = "SELECT * FROM bisams.stammdaten where not deleted AND webstatusext_noteid=2 and noteid=" + stamm_noteid;
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t- " + rs.getString("noteid") + ": " + rs.getString("bezeichnung"));
			query = "SELECT * FROM bisams.qualifikation where  NOT deleted AND webstatus_noteid=2 and noteid in (select qualifikation_noteid from bisams.stammdaten_qualifikation where stammdaten_noteid ="
				+ rs.getString("noteid")
				+ ")";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t- " + rs2.getString("noteid") + ": " + rs2.getString("bezeichnung"));
				qualDic.put(rs2.getInt("noteid"), rs2.getString("bezeichnung"));
				query = "SELECT * FROM bisams.qualifikation_detail where not deleted and qualifikation_noteid ="
					+ rs2.getString("noteid")
					+ " order by ebene";
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
					//qualDic.put(rs3.getInt("noteid"), rs3.getString("bezeichnung"));
					System.out.println(
						"\t\t\t- "
							+ rs3.getString("noteid")
							+ ": "
							+ rs3.getString("bezeichnung")
							+ " (level "
							+ rs3.getString("ebene")
							+ ")");

			}
		}
		System.out.println("\nEXPLORING COMPETENCES:");
		Iterator<Map.Entry<Integer, String>> it = qualDic.entrySet().iterator();
		while (it.hasNext())
		{
			Map.Entry<Integer, String> entry = it.next();
			System.out.println("  - " + entry.getValue() + " (" + entry.getKey() + ")");
			exploreQualificationsDetail_inner(entry.getKey());
		}


		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * This module explores the BIS Categories.
	 */
	public static void exploreCategories() throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmt3 = null, stmt4 = null;
		ResultSet rs = null, rs2 = null, rs3 = null, rs4 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		stmt3 = conn.createStatement();
		stmt4 = conn.createStatement();
		String query;

		System.out.println("Path 1:");
		query = "SELECT noteid, bezeichnung FROM bis2.stammdatenkategorien WHERE NOT deleted AND webstatus_noteid=2";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println(rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bis2.berufsfelder WHERE NOT deleted AND webstatus_noteid=2 AND stammdatenkategorien_noteid="
				+ rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				query = "SELECT a.noteid, a.bezeichnung FROM bis2.stammdaten a, bis2.stammdaten_berufsfelder b where not a.deleted AND a.webstatusext_noteid=2 and a.noteid=b.stammdaten_noteid and b.berufsfelder_noteid="
					+ rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
				{
					System.out.println("\t\t- " + rs3.getString(2) + " (" + rs3.getString(1) + ")");
					query = "SELECT noteid, bezeichnung FROM bis2.spezielle where stammdaten_noteid=" + rs3.getString(1);
					rs4 = stmt4.executeQuery(query);
					for (; rs4.next();)
					{
						System.out.println("\t\t\t- " + rs4.getString(2) + " (" + rs4.getString(1) + ")");
					}
				}

			}
		}
		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * This module explores the BIS Categories with counts of the job titles for each spezielle.
	 * at the fourth level of the hierarchy it shows the counts of spezielle (instead of listing all the data)
	 */
	public static void exploreCategoriesCounts() throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmt3 = null, stmt4 = null;
		ResultSet rs = null, rs2 = null, rs3 = null, rs4 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		stmt3 = conn.createStatement();
		stmt4 = conn.createStatement();
		String query;

		System.out.println("Path 1:");
		query = "SELECT noteid, bezeichnung FROM bis2.stammdatenkategorien WHERE NOT deleted AND webstatus_noteid=2";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println(rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bis2.berufsfelder WHERE NOT deleted AND webstatus_noteid=2 AND stammdatenkategorien_noteid="
				+ rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				query = "SELECT a.noteid, a.bezeichnung FROM bis2.stammdaten a, bis2.stammdaten_berufsfelder b where not a.deleted AND a.webstatusext_noteid=2 and a.noteid=b.stammdaten_noteid and b.berufsfelder_noteid="
					+ rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
				{
					System.out.println("\t\t- " + rs3.getString(2) + " (" + rs3.getString(1) + "): ");
					query = "SELECT count(*) FROM bis2.spezielle where stammdaten_noteid=" + rs3.getString(1);
					rs4 = stmt4.executeQuery(query);
					if (rs4.next())
					{
						System.out.println("\t\t\t - > " + rs4.getString(1) + " job titles");
					}
				}

			}
		}
		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * This module explores the BIS Categories with counts of the skills for each spezielle.
	 * at the fourth level of the hierarchy it shows the counts of spezielle (instead of listing all the data)
	 */
	public static void exploreCategoriesSkillCounts() throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		System.out.println("Counts of Competences per ");
		query = "SELECT noteid, bezeichnung FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println(rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT bezeichnung FROM bis2.qualifikation WHERE qualifikationsbereich_noteid="
				+ rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			while (rs2.next())
			{
				System.out.println("\t+ " + rs2.getString(1) + " qualifications)");
/*				query = "SELECT a.noteid, a.bezeichnung FROM bis2.stammdaten a, bis2.stammdaten_berufsfelder b where not a.deleted AND a.webstatusext_noteid=2 and a.noteid=b.stammdaten_noteid and b.berufsfelder_noteid="
					+ rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
				{
					System.out.println("\t\t- " + rs3.getString(2) + " (" + rs3.getString(1) + "): ");
					query = "SELECT count(*) FROM bis2.spezielle where stammdaten_noteid=" + rs3.getString(1);
					rs4 = stmt4.executeQuery(query);
					if (rs4.next())
					{
						System.out.println("\t\t\t - > " + rs4.getString(1) + " job titles");
					}
				}
*/
			}
		}
		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * This module explores the BIS Qualifications.
	 * It the complete structure of qualifications
	 */
	public static void exploreQualifications() throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		System.out.println("SKILLS");
		query = "SELECT noteid, bezeichnung FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
		System.out.println("SOFT SKILLS");
		query = "SELECT noteid, bezeichnung FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=0 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
		System.out.println("CERTIFICATE");
		query = "SELECT noteid, bezeichnung FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=2 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * This module explores the BIS Qualifications.
	 * It the complete structure of qualifications
	 */
	public static void exploreQualifications2() throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		query = "SELECT noteid FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.print("\nP" + rs.getString(1));

			query = "SELECT aa.noteid level4, bb.noteid level5,cc.noteid level6, dd.noteid level7, e.noteid level8, f.noteid level9"
				+ " FROM bisams.qualifikation_detail aa"
				+ " LEFT OUTER JOIN bis2.qualifikation_detail bb"
				+ " ON aa.thesid=bb.parent_thesid"
				+ " LEFT OUTER JOIN bis2.qualifikation_detail cc"
				+ " ON bb.thesid=cc.parent_thesid"
				+ " LEFT OUTER JOIN bis2.qualifikation_detail dd ON cc.thesid=dd.parent_thesid"
				+ " LEFT OUTER JOIN bis2.qualifikation_detail e ON e.thesid=dd.parent_thesid"
				+ " LEFT OUTER JOIN bis2.qualifikation_detail f ON f.thesid=e.parent_thesid"
				+ " where not aa.deleted and aa.parent_thesid =" + rs.getString(1)
				+ " order by level4, level5, level6, level7, level8, level9";
			rs2 = stmt2.executeQuery(query);
			//System.out.println(query);
			for (; rs2.next();)
			{
				//if (!level4.equalsIgnoreCase(rs2.getString("level4")))
				//	System.out.print(rs2.getString("level4"));
				if (rs2.getString("level4") != null)
				{
					System.out.print("\tC" + rs2.getString("level4"));
					if (rs2.getString("level5") != null)
						System.out.print("\tC" + rs2.getString("level5"));
						if (rs2.getString("level6") != null)
							System.out.print("\tC" + rs2.getString("level6") );
							if (rs2.getString("level7") != null)
								System.out.print("\tC" + rs2.getString("level7") );
								if (rs2.getString("level8") != null)
									System.out.print("\tC" + rs2.getString("level8") );
									if (rs2.getString("level9") != null)
										System.out.print("\tC" + rs2.getString("level9") );
				}
				System.out.println();
			}
		}
		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * This module explores the BIS Qualifications.
	 * It the complete structure of qualifications
	 */
	public static void exploreOneQualification(int qual) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		System.out.println("SKILLS");
		query = "SELECT noteid, bezeichnung FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1 order by bezeichnung";
		rs = stmt.executeQuery(query);
			query = "SELECT noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ qual
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
/*		System.out.println("SOFT SKILLS");
		query = "SELECT noteid, bezeichnung FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=0 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
		System.out.println("CERTIFICATE");
		query = "SELECT noteid, bezeichnung FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=2 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
*/		rs.close();
		rs2.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * Uses the parent/child relationship to extract the detailed competences for a given qualification
	 */
	public static void exploreQualificationsDetail(int qual) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		String query;

		System.out.println("Path 1:");
		query = "SELECT aa.bezeichnung Parent, bb.bezeichnung Child1, cc.bezeichnung Child2, dd.bezeichnung Child3"
			+ " FROM bisams.qualifikation_detail aa"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail bb"
			+ " ON aa.thesid=bb.parent_thesid"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail cc"
			+ " ON bb.thesid=cc.parent_thesid"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail dd"
			+ " ON cc.thesid=dd.parent_thesid where not aa.deleted and aa.ebene=1 and aa.qualifikation_noteid="
			+ qual
			+ " order by Parent, Child1, Child2, Child3";
		rs = stmt.executeQuery(query);
		String parent = "", child1 = "", child2 = "";
		for (; rs.next();)
		{
			if (!parent.equalsIgnoreCase(rs.getString(1)))
				System.out.println(rs.getString(1));
			parent = rs.getString(1);
			if (rs.getString(2) != null)
			{
				if (!child1.equalsIgnoreCase(rs.getString(2)))
					System.out.println("\t- " + rs.getString(2));
				child1 = rs.getString(2);
				if (rs.getString(3) != null)
				{
					if (!child2.equalsIgnoreCase(rs.getString(3)))
						System.out.println("\t\t- " + rs.getString(3));
					child2 = rs.getString(3);
					if (rs.getString(4) != null)
						System.out.println("\t\t\t- " + rs.getString(4));
				}
			}
		}
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void exploreQualificationsDetail(String qual) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		String query;

		System.out.println("Path 1:");
		query = "SELECT aa.noteid Id, aa.bezeichnung Parent, bb.noteid Id1, bb.bezeichnung Child1, cc.noteid Id2, cc.bezeichnung Child2, dd.noteid Id3, dd.bezeichnung Child3"
			+ " FROM bisams.qualifikation_detail aa"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail bb"
			+ " ON aa.thesid=bb.parent_thesid"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail cc"
			+ " ON bb.thesid=cc.parent_thesid"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail dd"
			+ " ON cc.thesid=dd.parent_thesid where not aa.deleted and (aa.bezeichnung = '"
			+ qual + "' or aa.noteid ='" + qual
			+ "') order by Parent, Child1, Child2, Child3";
		rs = stmt.executeQuery(query);
		String parent = "", child1 = "", child2 = "";
		for (; rs.next();)
		{
			if (!parent.equalsIgnoreCase(rs.getString("Parent")))
				System.out.println(rs.getString("Parent") + " (" + rs.getString("id") +")");
			parent = rs.getString("Parent");
			if (rs.getString("Child1") != null)
			{
				if (!child1.equalsIgnoreCase(rs.getString("Child1")))
					System.out.println("\t- " + rs.getString("Child1") + " (" + rs.getString("id1") +")");
				child1 = rs.getString("Child1");
				if (rs.getString("Child2") != null)
				{
					if (!child2.equalsIgnoreCase(rs.getString("Child2")))
						System.out.println("\t\t- " + rs.getString("Child2") + " (" + rs.getString("id2") +")");
					child2 = rs.getString("Child2");
					if (rs.getString("Child3") != null)
						System.out.println("\t\t\t- " + rs.getString("Child3") + " (" + rs.getString("id3") +")");
				}
			}
		}
		rs.close();
		stmt.close();
		conn.close();
	}

	/*
	 * Uses the parent/child relationship to extract the detailed competences for a given qualification
	 */
	public static void exploreQualificationsDetail_inner(int qual) throws ClassNotFoundException, SQLException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		String query;

		query = "SELECT aa.bezeichnung Parent, bb.bezeichnung Child1, cc.bezeichnung Child2, dd.bezeichnung Child3"
			+ " FROM bisams.qualifikation_detail aa"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail bb"
			+ " ON aa.thesid=bb.parent_thesid"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail cc"
			+ " ON bb.thesid=cc.parent_thesid"
			+ " LEFT OUTER JOIN bis2.qualifikation_detail dd"
			+ " ON cc.thesid=dd.parent_thesid where not aa.deleted and aa.ebene=1 and aa.qualifikation_noteid="
			+ qual
			+ " order by Parent, Child1, Child2, Child3";
		rs = stmt.executeQuery(query);
		String parent = "", child1 = "", child2 = "";
		for (; rs.next();)
		{
			if (!parent.equalsIgnoreCase(rs.getString(1)))
				System.out.println("    -- " + rs.getString(1));
			parent = rs.getString(1);
			if (rs.getString(2) != null)
			{
				if (!child1.equalsIgnoreCase(rs.getString(2)))
					System.out.println("\t--- " + rs.getString(2));
				child1 = rs.getString(2);
				if (rs.getString(3) != null)
				{
					if (!child2.equalsIgnoreCase(rs.getString(3)))
						System.out.println("\t    ---- " + rs.getString(3));
					child2 = rs.getString(3);
					if (rs.getString(4) != null)
						System.out.println("\t\t----- " + rs.getString(4));
				}
			}
		}
		rs.close();
		stmt.close();
		conn.close();
	}

	/*
	 * This module explores the affinities with the BIS model
	 */
	public static void exploreAffinities() throws ClassNotFoundException, SQLException
				{
					String[][] queries = {
						{"Spezielle-Spezielle verwacht","SELECT a.noteid, a.bezeichnung Spezielle_1, c.noteid, c.bezeichnung Spezielle_2, b.gewicht, b.gegengewicht" +
						" from bisams.spezielle a, bisams.spezielle_verwandt b, bisams.spezielle c where a.noteid=b.spezielle_noteid and c.noteid=b.spezielle_verwandt_noteid"},
						{"Stammdaten-Stammdaten verwacht","SELECT a.noteid, a.bezeichnung stammdaten_1, c.noteid, c.bezeichnung stammdaten_2, b.gewicht, b.gegengewicht" + 
						" from bisams.stammdaten a, bisams.stammdaten_verwandt b, bisams.stammdaten c where a.noteid=b.stammdaten_noteid and c.noteid=b.stammdaten_verwandt_noteid"},
						{"Stammdaten-Spezielle verwacht","SELECT a.noteid, a.bezeichnung stammdaten, c.noteid, c.bezeichnung Spezielle, b.gewicht, b.gegengewicht" +
						" from bisams.stammdaten a, bisams.stammdaten_spezielle_verwandt b, bisams.spezielle c where a.noteid=b.stammdaten_noteid and c.noteid=b.spezielle_verwandt_noteid"},
						{"qualifikation-qualifikation verwacht","SELECT a.noteid, a.bezeichnung qualifikation_1, c.noteid, c.bezeichnung qualifikation_2, b.gewicht, b.gegengewicht" + 
						" from bisams.qualifikation a, bisams.qualifikation_verwandt b, bisams.qualifikation c where a.noteid=b.qualifikation_noteid and c.noteid=b.qualifikation_verwandt_noteid"},
						{"qualifikation_detail-qualifikation_detail verwacht","SELECT a.noteid, a.bezeichnung qualifikation_1, c.noteid, c.bezeichnung qualifikation_2, b.gewicht, b.gegengewicht" + 
						" from bisams.qualifikation_detail a, bisams.qualifikation_detail_verwandt b, bisams.qualifikation_detail c where a.noteid=b.qualifikation_detail_noteid and c.noteid=b.qualifikation_detail_verwandt_noteid"},
						{"qualifikation-Spezielle verwacht","SELECT a.noteid, a.bezeichnung stammdaten, c.noteid, c.bezeichnung Spezielle, b.gewicht, b.gegengewicht from bisams.qualifikation a, bisams.qualifikation_detail_verwandt b, bisams.qualifikation_detail c" +
						" where a.noteid=b.qualifikation_detail_noteid and c.noteid=b.qualifikation_detail_verwandt_noteid"},
						{"lehrberuf-lehrberuf verwacht","SELECT a.noteid, a.bezeichnung lehrberuf_1, c.noteid, c.bezeichnung lehrberuf_2, anrechnung_lehrjahr_1, anrechnung_lehrjahr_2, anrechnung_lehrjahr_3, anrechnung_lehrjahr_4" +
						" from bisams.lehrberuf a, bisams.lehrberuf_verwandt b, bisams.lehrberuf c where a.noteid=b.lehrberuf_noteid and c.noteid=b.lehrberuf_verwandt_noteid"},
/*						{"lehrberuf-lehrberuf verwacht","SELECT a.noteid, a.bezeichnung arbeitsumfeld_1, c.noteid, c.bezeichnung arbeitsumfeld_2" +
						" from bisams.arbeitsumfeld a, bisams.arbeitsumfeld_verwandt b, bisams.arbeitsumfeld c where a.noteid=b.arbeitsumfeld_noteid and c.noteid=b.arbeitsumfeld_verwandt_noteid"}
*/						};
					Connection conn = null;
					Statement stmt = null;
					ResultSet rs = null;
					Class.forName(JDBC_DRIVER);
					conn = DriverManager.getConnection(DB_URL, USER, PASS);
					stmt = conn.createStatement();
					String query;
				
					for (int i=0; i<queries.length; i++) {
						System.out.println(queries[i][1]); 
						query = queries[i][1];
						rs = stmt.executeQuery(query);
						for (; rs.next();)
							System.out.println("\t- '" + rs.getString(2) + "' VerSus '" + rs.getString(4) + "' | " + rs.getString(5) + " <--> " + rs.getString(6)); 
					}
					rs.close();
					stmt.close();
					conn.close();
				}

	/*
	 * This module extracts BIS data from SQL tables and translates them into an excel format to be consumed
	 * by ETM (Elise Taxonomy Manager).
	 * @para output output excel file containing extracted data
	 * @para item item for which data is to be extracted, the following values can be specified:
	 * 		all: to extract all data
	 * 		O:		to extract the occupations
	 * 		C:		to extract the competences
	 * 		OC:		to extract the relationships occupation/competence
	 * 		S:		to extract the synonyms
	 * 		OS:		to extract the relationships occupation/synonyms
	 * 		E:		to extract the education
	 * 		OE:		to extract the relationships occupation/education
	 * 		W:		to extract the working environment
	 * 		OW:		to extract the relationships occupation/working environment
	 * 		etc.
	 */
	public static void BIStoETM_xls(String output, String item) throws ClassNotFoundException, SQLException, IOException
	{
		//occup = "Software-EntwicklerIn";
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		//Create Workbook instance holding reference to .xlsx file
		XSSFWorkbook workbook = new XSSFWorkbook();
		int i = 0;

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("O")) {
			XSSFSheet occupations = workbook.createSheet("Occupations");

			query = "SELECT concat('O',noteid) noteid, bezeichnung FROM bis2.stammdaten where not deleted AND webstatusext_noteid=2 "
				+ "union "
				+ "SELECT concat('J',noteid) noteid, bezeichnung FROM bis2.spezielle";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupations.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}

			XSSFSheet occupationsRel = workbook.createSheet("OccupationsRelationships");
			query = "SELECT concat('J',noteid) noteid, concat('O',stammdaten_noteid) stammdaten_noteid FROM bis2.spezielle";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupationsRel.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
		}

		if (item.equalsIgnoreCase("all2") || item.equalsIgnoreCase("C2")) {
			XSSFSheet competences = workbook.createSheet("Competences");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation where not deleted AND webstatus_noteid=2 "
				+ "union "
				+ "SELECT concat('C',noteid) noteid, bezeichnung FROM bis2.qualifikation_detail where not deleted";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = competences.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
	
			i=0;
			XSSFSheet competencesRel = workbook.createSheet("CompetencesRelationships");
			System.out.println("Technical vocational skills");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 "
					+ "AND qualifikationsbereich_noteid in (SELECT noteid FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1)";
			rs = stmt.executeQuery(query);
			XSSFRow row = competencesRel.createRow(i);
			row.createCell(0).setCellValue("Technical vocational skills");
			i++;
			for (; rs.next();)
			{
				System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");
	
				row = competencesRel.createRow(i);
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
			
			System.out.println("Interdisciplinary professional skills");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 "
				+ "AND qualifikationsbereich_noteid in (SELECT noteid FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=0)";
			rs = stmt.executeQuery(query);
			row = competencesRel.createRow(i);
			row.createCell(0).setCellValue("Interdisciplinary professional skills");
			i++;
			for (; rs.next();)
			{
				System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");
				row = competencesRel.createRow(i);
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
	
			}
			System.out.println("Certificates and diplomas");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 "
				+ "AND qualifikationsbereich_noteid in (SELECT noteid FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=2)";
			rs = stmt.executeQuery(query);
			row = competencesRel.createRow(i);
			row.createCell(0).setCellValue("Certificates and diplomas");
			i++;
			for (; rs.next();)
			{
				System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");
				row = competencesRel.createRow(i);
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
	
			i=0;
			XSSFSheet competencesHierarchy = workbook.createSheet("CompetencesHierarchy");
			query = "SELECT noteid FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				System.out.print("\nP" + rs.getString(1));
				row = competencesHierarchy.createRow(i);
				row.createCell(0).setCellValue('P' + rs.getString(1));
	
				query = "SELECT aa.noteid level4, bb.noteid level5,cc.noteid level6, dd.noteid level7, e.noteid level8, f.noteid level9"
					+ " FROM bisams.qualifikation_detail aa"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail bb"
					+ " ON aa.thesid=bb.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail cc"
					+ " ON bb.thesid=cc.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail dd ON cc.thesid=dd.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail e ON e.thesid=dd.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail f ON f.thesid=e.parent_thesid"
					+ " where not aa.deleted and aa.parent_thesid =" + rs.getString(1)
					+ " order by level4, level5, level6, level7, level8, level9";
				rs2 = stmt2.executeQuery(query);
				//System.out.println(query);
				for (; rs2.next();)
				{
					//if (!level4.equalsIgnoreCase(rs2.getString("level4")))
					//	System.out.print(rs2.getString("level4"));
					if (rs2.getString("level4") != null)
					{
						System.out.print("\tC" + rs2.getString("level4"));
						row.createCell(1).setCellValue('C' + rs2.getString("level4"));
						if (rs2.getString("level5") != null) {
							System.out.print("\tC" + rs2.getString("level5"));
							row.createCell(2).setCellValue('C' + rs2.getString("level5"));
							if (rs2.getString("level6") != null) {
								System.out.print("\tC" + rs2.getString("level6") );
								row.createCell(3).setCellValue('C' + rs2.getString("level6"));
								if (rs2.getString("level7") != null) {
									System.out.print("\tC" + rs2.getString("level7") );
									row.createCell(4).setCellValue('C' + rs2.getString("level7"));
									if (rs2.getString("level8") != null) {
										System.out.print("\tC" + rs2.getString("level8") );
										row.createCell(5).setCellValue('C' + rs2.getString("level8"));
										if (rs2.getString("level9") != null) {
											System.out.print("\tC" + rs2.getString("level9") );
											row.createCell(6).setCellValue('C' + rs2.getString("level9"));
										}
									}
								}
							}
						}
					}
					System.out.println();
					i++;
					row = competencesHierarchy.createRow(i);
				}
			}
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("S")) {
			XSSFSheet occupations = workbook.createSheet("Synonyms");

			query = "SELECT concat('S',noteid) noteid, bezeichnung FROM bis2.Synonyme";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupations.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}

			XSSFSheet occupationsRel = workbook.createSheet("OccupationsSynonyms");
			query = "SELECT concat('S',noteid) noteid, concat('O',stammdaten_noteid) stammdaten_noteid FROM bis2.Synonyme";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupationsRel.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
		}

	
		FileOutputStream fileOut = new FileOutputStream(output);
		workbook.write(fileOut);
		fileOut.flush();
		fileOut.close();
		workbook.close();
		if (rs2!=null) {
			rs2.close();
			stmt2.close();
		}
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void BIStoETM(String output, String item) throws ClassNotFoundException, SQLException, IOException
	{
		//occup = "Software-EntwicklerIn";
		Connection conn = null;
		Statement stmt = null, stmt2 = null;
		ResultSet rs = null, rs2=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		String query;

		//Create Workbook instance holding reference to .xlsx file
		XSSFWorkbook workbook = new XSSFWorkbook();
		int i = 0;

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("O")) {
			XSSFSheet occupations = workbook.createSheet("Occupations");

			query = "SELECT concat('O',noteid) noteid, bezeichnung FROM bis2.stammdaten where not deleted AND webstatusext_noteid=2 "
				+ "union "
				+ "SELECT concat('J',noteid) noteid, bezeichnung FROM bis2.spezielle";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupations.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}

			XSSFSheet occupationsRel = workbook.createSheet("OccupationsRelationships");
			query = "SELECT concat('J',noteid) noteid, concat('O',stammdaten_noteid) stammdaten_noteid FROM bis2.spezielle";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupationsRel.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
		}

		if (item.equalsIgnoreCase("all3") || item.equalsIgnoreCase("C3")) {
			XSSFSheet competences = workbook.createSheet("Competences");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation where not deleted AND webstatus_noteid=2 "
				+ "union "
				+ "SELECT concat('C',noteid) noteid, bezeichnung FROM bis2.qualifikation_detail where not deleted";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = competences.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
	
			i=0;
			XSSFSheet competencesRel = workbook.createSheet("CompetencesRelationships");
			System.out.println("Technical vocational skills");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 "
					+ "AND qualifikationsbereich_noteid in (SELECT noteid FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1)";
			rs = stmt.executeQuery(query);
			XSSFRow row = competencesRel.createRow(i);
			row.createCell(0).setCellValue("Technical vocational skills");
			i++;
			for (; rs.next();)
			{
				System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");
	
				row = competencesRel.createRow(i);
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
			
			System.out.println("Interdisciplinary professional skills");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 "
				+ "AND qualifikationsbereich_noteid in (SELECT noteid FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=0)";
			rs = stmt.executeQuery(query);
			row = competencesRel.createRow(i);
			row.createCell(0).setCellValue("Interdisciplinary professional skills");
			i++;
			for (; rs.next();)
			{
				System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");
				row = competencesRel.createRow(i);
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
	
			}
			System.out.println("Certificates and diplomas");
			query = "SELECT concat('P',noteid) noteid, bezeichnung FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2 "
				+ "AND qualifikationsbereich_noteid in (SELECT noteid FROM bis2.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=2)";
			rs = stmt.executeQuery(query);
			row = competencesRel.createRow(i);
			row.createCell(0).setCellValue("Certificates and diplomas");
			i++;
			for (; rs.next();)
			{
				System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");
				row = competencesRel.createRow(i);
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
	
			i=0;
			XSSFSheet competencesHierarchy = workbook.createSheet("CompetencesHierarchy");
			query = "SELECT noteid FROM bis2.qualifikation WHERE NOT deleted AND webstatus_noteid=2";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				System.out.print("\nP" + rs.getString(1));
				row = competencesHierarchy.createRow(i);
				row.createCell(0).setCellValue('P' + rs.getString(1));
	
				query = "SELECT aa.noteid level4, bb.noteid level5,cc.noteid level6, dd.noteid level7, e.noteid level8, f.noteid level9"
					+ " FROM bisams.qualifikation_detail aa"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail bb"
					+ " ON aa.thesid=bb.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail cc"
					+ " ON bb.thesid=cc.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail dd ON cc.thesid=dd.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail e ON e.thesid=dd.parent_thesid"
					+ " LEFT OUTER JOIN bis2.qualifikation_detail f ON f.thesid=e.parent_thesid"
					+ " where not aa.deleted and aa.parent_thesid =" + rs.getString(1)
					+ " order by level4, level5, level6, level7, level8, level9";
				rs2 = stmt2.executeQuery(query);
				//System.out.println(query);
				for (; rs2.next();)
				{
					//if (!level4.equalsIgnoreCase(rs2.getString("level4")))
					//	System.out.print(rs2.getString("level4"));
					if (rs2.getString("level4") != null)
					{
						System.out.print("\tC" + rs2.getString("level4"));
						row.createCell(1).setCellValue('C' + rs2.getString("level4"));
						if (rs2.getString("level5") != null) {
							System.out.print("\tC" + rs2.getString("level5"));
							row.createCell(2).setCellValue('C' + rs2.getString("level5"));
							if (rs2.getString("level6") != null) {
								System.out.print("\tC" + rs2.getString("level6") );
								row.createCell(3).setCellValue('C' + rs2.getString("level6"));
								if (rs2.getString("level7") != null) {
									System.out.print("\tC" + rs2.getString("level7") );
									row.createCell(4).setCellValue('C' + rs2.getString("level7"));
									if (rs2.getString("level8") != null) {
										System.out.print("\tC" + rs2.getString("level8") );
										row.createCell(5).setCellValue('C' + rs2.getString("level8"));
										if (rs2.getString("level9") != null) {
											System.out.print("\tC" + rs2.getString("level9") );
											row.createCell(6).setCellValue('C' + rs2.getString("level9"));
										}
									}
								}
							}
						}
					}
					System.out.println();
					i++;
					row = competencesHierarchy.createRow(i);
				}
			}
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("S")) {
			XSSFSheet occupations = workbook.createSheet("Synonyms");

			query = "SELECT concat('S',noteid) noteid, bezeichnung FROM bis2.Synonyme";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupations.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}

			XSSFSheet occupationsRel = workbook.createSheet("OccupationsSynonyms");
			query = "SELECT concat('S',noteid) noteid, concat('O',stammdaten_noteid) stammdaten_noteid FROM bis2.Synonyme";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
				System.out.println( rs.getString(2) + "\t" + rs.getString(1));
				XSSFRow row = occupationsRel.createRow(i);
				row.createCell(0).setCellValue(rs.getString(2));
				row.createCell(1).setCellValue(rs.getString(1));
				i++;
			}
		}

		
		FileOutputStream fileOut = new FileOutputStream(output);
		workbook.write(fileOut);
		fileOut.flush();
		fileOut.close();
		workbook.close();
		if (rs2!=null) {
			rs2.close();
			stmt2.close();
		}
		rs.close();
		stmt.close();
		conn.close();
	}

	public static void BIStoETM_csv(String output, String item) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		String query;

	    BufferedWriter writer;

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("O")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation.csv")));
			System.out.print( "Genereting BIS ISCO occupations: ");

			//query = "SELECT distinct code, name, description FROM bissimple3.occupation where code like 'J%' or code like 'O%' order by name";
			query = "SELECT distinct code, name, category, description, employment_opportunities, spelling FROM bissimple3.occupation order by name";
			rs = stmt.executeQuery(query);
			//writer.write("code\tname\tbushtabe");
			writer.write("code\tname\tbushtabe\thaupttaetigkeit\tbeschaeftigungsmoeglichkeiten");
			for (; rs.next();)
			{
			    //writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2) + "\t" + (rs.getString(3)!=null?rs.getString(3):""));
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2) 
			    + "\t" + (rs.getString(3)!=null?rs.getString(3).replaceAll("\r", " ").replaceAll("\n", " "):"")
			    + "\t" + (rs.getString(4)!=null?rs.getString(4).replaceAll("\r", " ").replaceAll("\n", " "):"")
			    + "\t" + (rs.getString(5)!=null?rs.getString(5).replaceAll("\r", " ").replaceAll("\n", " "):"")
			    //+ "\t" + (rs.getString(6)!=null?rs.getString(6).replaceAll("\t", " "):"")
			    );
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation-hierarchy.csv")));
			query = "SELECT distinct code, parent FROM bissimple3.occupation where parent is not null"
				+ " union"
				+ " SELECT distinct code, null parent FROM bissimple3.occupation where parent is null and code not in (SELECT code FROM bissimple3.occupation where parent is not null)"
				+ " order by parent, code";
			rs = stmt.executeQuery(query);
			int i =0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");
				writer.write((rs.getString("parent")!=null?rs.getString("parent") + "\t" : "") + rs.getString("code")); 
				i++;
			}
			System.out.println("\n\t- " + i + " occupations");
		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("OI")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"isco.csv")));
			System.out.print( "Genereting BIS ISCO occupations: ");

			query = "SELECT distinct code, name FROM bissimple3.isco_berufsgattungen order by code";
			rs = stmt.executeQuery(query);
			writer.write("code\tname");
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(1) + ": " + rs.getString(2));
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"isco-hierarchy.csv")));
			query = "SELECT "
				+ "a.code as level1,b.code as level2,c.code as level3,d.code as level4 "
				+ "FROM bissimple3.isco_berufsgattungen a "
				+ "LEFT JOIN bissimple3.isco_berufsgattungen b ON b.parent = a.code "
				+ "LEFT JOIN bissimple3.isco_berufsgattungen c ON c.parent = b.code "
				+ "LEFT JOIN bissimple3.isco_berufsgattungen d ON d.parent = c.code "
				+ "where a.parent = 'null' "
				+ "order by level1, level2, level3, level4";
			rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");
				writer.write(rs.getString("level1") + "\t" + rs.getString("level2") + "\t" + rs.getString("level3") + "\t" + rs.getString("level4"));
				i++;
			}
			System.out.println();
		    writer.close();
		}

		if (item.equalsIgnoreCase("all3") || item.equalsIgnoreCase("OI3")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation.csv")));
			System.out.print( "Genereting BIS ISCO occupations: ");

			query = "SELECT distinct code, name FROM bissimple3.occupation_isco order by name";
			rs = stmt.executeQuery(query);
			writer.write("code\tname");
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(1) + ": " + rs.getString(2));
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation-hierarchy.csv")));
			query = "SELECT "
				+ "a.code as level1,b.code as level2,c.code as level3,d.code as level4,e.code as level5,f.code as level6 "
				+ "FROM bissimple3.occupation_isco a "
				+ "LEFT JOIN bissimple3.occupation_isco b ON b.parent = a.code "
				+ "LEFT JOIN bissimple3.occupation_isco c ON c.parent = b.code "
				+ "LEFT JOIN bissimple3.occupation_isco d ON d.parent = c.code "
				+ "LEFT JOIN bissimple3.occupation_isco e ON e.parent = d.code "
				+ "LEFT JOIN bissimple3.occupation_isco f ON f.parent = e.code "
				+ "where a.parent = 'null' "
				+ "order by level1, level2, level3, level4, level5, level6";
			rs = stmt.executeQuery(query);
			String l1,l2,l3,l4,l5,l6;
			l1=l2=l3=l4=l5=l6="";
			for (; rs.next();)
			{
				if (rs.getString("level1") != null )	{
					if (!rs.getString("level1").equalsIgnoreCase(l1))
						if (rs.isFirst())
							writer.write(rs.getString("level1"));
						else
							writer.write("\n" + rs.getString("level1"));
					l1= rs.getString("level1");
					if (rs.getString("level2") != null) {
						if (!rs.getString("level2").equalsIgnoreCase(l2))
							writer.write("\n" + rs.getString("level1") + "\t" + rs.getString("level2"));
						l2= rs.getString("level2");
						if (rs.getString("level3") != null) {
							if (!rs.getString("level3").equalsIgnoreCase(l3))
								writer.write("\n" + rs.getString("level1") + "\t" + rs.getString("level2") + "\t" + rs.getString("level3"));
							l3= rs.getString("level3");
							if (rs.getString("level4") != null) {
								if (!rs.getString("level4").equalsIgnoreCase(l4))
									writer.write("\n" + rs.getString("level1") + "\t" + rs.getString("level2") + "\t" + rs.getString("level3") + "\t" + rs.getString("level4"));
								l4= rs.getString("level4");
								if (rs.getString("level5") != null) {
									if (!rs.getString("level5").equalsIgnoreCase(l5))
										writer.write("\n" + rs.getString("level1") + "\t" + rs.getString("level2") + "\t" + rs.getString("level3") + "\t" + rs.getString("level4") + "\t" + rs.getString("level5"));
									l5= rs.getString("level5");
									if (rs.getString("level6") != null) {
										if (!rs.getString("level6").equalsIgnoreCase(l6))
											writer.write("\n" + rs.getString("level1") + "\t" + rs.getString("level2") + "\t" + rs.getString("level3") + "\t" + rs.getString("level4") + "\t" + rs.getString("level5") + "\t" + rs.getString("level6"));
										l6= rs.getString("level6");
									}
								}
							}
						}
					}
				}
			}
			System.out.println();
		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("C")) {
			System.out.print( "Genereting Competences: ");
		    writer = new BufferedWriter(new FileWriter(new File(output+"competence.csv")));
			writer.write("code\tname");
			query = "SELECT code, name FROM bissimple3.competence order by name";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				System.out.print( ". ");
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
			}
	
		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"competence-hierarchy.csv")));
			query = "select distinct * from bissimple3.competence_flat_9l";
			rs = stmt.executeQuery(query);
			int i=0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");

				writer.write(rs.getString("level1") + "\t" + rs.getString("level2"));
				if (rs.getString("level3") != null) 
					writer.write( "\t" + rs.getString("level3"));
				if (rs.getString("level4") != null) 
					writer.write( "\t" + rs.getString("level4"));
				if (rs.getString("level5") != null) 
					writer.write("\t" + rs.getString("level5"));
				if (rs.getString("level6") != null) 
					writer.write("\t" + rs.getString("level6"));
				if (rs.getString("level7") != null) 
					writer.write("\t" + rs.getString("level7"));
				if (rs.getString("level8") != null) 
					writer.write("\t" + rs.getString("level8"));
				if (rs.getString("level9") != null) 
					writer.write("\t" + rs.getString("level9"));
				i++;
			}

			System.out.println();
			writer.close();
		}

		if (item.equalsIgnoreCase("all1") || item.equalsIgnoreCase("A")) {
			System.out.print( "Genereting Apprenticeship: ");
		    writer = new BufferedWriter(new FileWriter(new File(output+"apprenticeship.csv")));

			query = "SELECT code, name "
				+ "FROM bissimple3.apprenticeship order by name";
			//query = "SELECT code, name FROM bissimple3.apprenticeship order by name";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			writer.write("code\tname");
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"apprenticeship-hierarchy.csv")));
			query = "SELECT code "
				+ "FROM bissimple3.apprenticeship order by name";
			//query = "SELECT code FROM bissimple3.apprenticeship ";
			rs = stmt.executeQuery(query);
			int i=0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");
				System.out.print( ". ");
				writer.write(rs.getString(1));
				i++;
					
			}
			System.out.println();
		    writer.close();
		}

		if (item.equalsIgnoreCase("all1") || item.equalsIgnoreCase("A3")) {
			System.out.print( "Genereting Apprenticeship: ");
		    writer = new BufferedWriter(new FileWriter(new File(output+"apprenticeship.csv")));

			query = "SELECT code, name "
				+ "FROM bissimple3.interestarea order by name "
				+ "union "
				+ "SELECT code, name "
				+ "FROM bissimple3.apprenticeship order by name";
			//query = "SELECT code, name FROM bissimple3.apprenticeship order by name";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			writer.write("code\tname");
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"apprenticeship-hierarchy.csv")));
			query = "SELECT interestarea, apprenticeship FROM bissimple3.apprenticeshipinterestarea "
				+ "union "
				+ "select 'uknown' interestarea, code apprenticeship from bissimple3.apprenticeship "
				+ "where code not in (SELECT apprenticeship FROM bissimple3.apprenticeshipinterestarea) "
				+ "order by interestarea";
			//query = "SELECT code FROM bissimple3.apprenticeship ";
			rs = stmt.executeQuery(query);
			String l1="";
			for (; rs.next();)
			{
				if (!rs.isFirst())
					writer.write("\n");
				System.out.print( ". ");
				if (!rs.getString(1).equalsIgnoreCase(l1)) {
					writer.write(rs.getString(1) + "\n");
					l1 = rs.getString(1);
				}
				if (rs.getString(2)!=null)
					writer.write(rs.getString(1) + "\t" + rs.getString(2));
					
			}
			System.out.println();
		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("A2")) {
			System.out.print( "Genereting Apprenticeship: ");
		    writer = new BufferedWriter(new FileWriter(new File(output+"apprenticeship.csv")));

			query = "SELECT code, name "
				+ "FROM bissimple3.interestarea "
				+ "union "
				+ "SELECT code, name "
				+ "FROM bissimple3.apprenticeship order by name";
			//query = "SELECT code, name FROM bissimple3.apprenticeship order by name";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			writer.write("code\tname");
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"apprenticeship-hierarchy.csv")));
			query = "SELECT interestarea, apprenticeship FROM bissimple3.apprenticeshipinterestarea "
				+ "union "
				+ "select 'uknown' interestarea, code apprenticeship from bissimple3.apprenticeship "
				+ "where code not in (SELECT apprenticeship FROM bissimple3.apprenticeshipinterestarea) "
				+ "order by interestarea";
			//query = "SELECT code FROM bissimple3.apprenticeship ";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				if (!rs.isFirst())
					writer.write("\n");
				System.out.print( ". ");
					writer.write(rs.getString(1) + "\t" + rs.getString(2));
					
			}
			System.out.println();
		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("E")) {
			System.out.print( "Genereting Education: ");
		    writer = new BufferedWriter(new FileWriter(new File(output+"education.csv")));

			query = "SELECT code, name "
				+ "FROM bissimple3.education order by code, name";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			writer.write("code\tname");
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"education-hierarchy.csv")));
			query = "SELECT a.code as level1,b.code as level2,c.code as level3 "
				+ "FROM bissimple3.education a "
				+ "LEFT JOIN bissimple3.education b ON b.category = a.code "
				+ "LEFT JOIN bissimple3.education c ON c.category = b.code "
				+ "where a.category = 'parent' "
				+ "order by level1, level2, level3";
			rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");
				System.out.print( ". ");
/*				if (rs.getString(1).equalsIgnoreCase("parent"))
				    writer.write (rs.getString(2));
				else
*/					writer.write (rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
				i++;
			}
			System.out.println();
		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("WE")) {
			System.out.print( "Genereting Working environment: ");
		    writer = new BufferedWriter(new FileWriter(new File(output+"working_env.csv")));

			query = "SELECT code, name "
				+ "FROM bissimple3.working_env order by name";
			//System.out.println(query);
			rs = stmt.executeQuery(query);
			writer.write("code\tname");
			for (; rs.next();)
			{
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
				System.out.print(". ");
			}

		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"working_env-hierarchy.csv")));
			query = "SELECT category, code FROM bissimple3.working_env "
				+ "order by category";
			rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");
				System.out.print( ". ");
				if (rs.getString(1).equalsIgnoreCase("parent"))
				    writer.write (rs.getString(2));
				else
					writer.write (rs.getString(1) + "\t" + rs.getString(2));
				i++;
			}
			System.out.println();
		    writer.close();

		    
		}

/*		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("O")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation.csv")));

			writer.write("code\tname");
			query = "SELECT code, name FROM bissimple3.occupationCategory "
				+ "union "
				+ "SELECT code, name FROM bissimple3.occupationField "
				+ "union "
				+ "SELECT code, name FROM bissimple3.occupation";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				System.out.println( rs.getString(1) + "\t" + rs.getString(2));
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(1) + ": " + rs.getString(2));
			}
		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation-hierarchy.csv")));
			query = "SELECT a.category as level1, a.code as level2, b.occupation as level3, c.code as level4 "
				+ "FROM bissimple3.occupationField a "
				+ "LEFT JOIN bissimple3.occupation_field b ON b.field = a.code LEFT JOIN bissimple3.occupation c ON c.parent = b.occupation "
				+ "order by level1, level2, level3, level4";
			rs = stmt.executeQuery(query);
			String l1="", l2="", l3="", l4="";
			for (; rs.next();)
			{
				//if (!rs.isFirst())
				//	writer.write("\n");
				System.out.println(rs.getString(1) + "\t" + rs.getString(2));
				if (!rs.getString(1).equalsIgnoreCase(l1))
					if (rs.isFirst())
						writer.write(rs.getString(1));
					else
						writer.write("\n" + rs.getString(1));
				l1=rs.getString(1);
					
					if (!rs.getString(2).equalsIgnoreCase(l2)) 
						writer.write("\n" + rs.getString(1) + "\t" + rs.getString(2));
					l2=rs.getString(2);					
				
						if (!rs.getString(3).equalsIgnoreCase(l3))
							writer.write("\n" + rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
						l3=rs.getString(3);		
							
							//if (rs.getString(4)!=null)	{
								//writer.write("\n");
							if (rs.getString(4)!=null && !rs.getString(4).equalsIgnoreCase(l4))
									writer.write("\n" + rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4));
							//else
							//	writer.write(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4));
								l4=rs.getString(4);	
							
			}
		    writer.close();
		}
		
*/		// occupation with flat hierarchy
		if (item.equalsIgnoreCase("all1") || item.equalsIgnoreCase("O2")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation.csv")));

			writer.write("code\tname");
			query = "SELECT distinct code, name FROM bissimple3.occupation order by name";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				System.out.println( rs.getString(1) + "\t" + rs.getString(2));
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(1) + ": " + rs.getString(2));
			}
		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation-hierarchy.csv")));
			query = "SELECT a.parent as level1, a.code as level2 "
				+ "FROM bissimple3.occupation a  where a.code like 'J%' "
				+ "order by level1, level2";
			rs = stmt.executeQuery(query);
			String l1="", l2="", l3="", l4="";
			for (; rs.next();)
			{
				//if (!rs.isFirst())
				//	writer.write("\n");
				System.out.println(rs.getString(1) + "\t" + rs.getString(2));
				if (!rs.getString(1).equalsIgnoreCase(l1))
					if (rs.isFirst())
						writer.write(rs.getString(1));
					else
						writer.write("\n" + rs.getString(1));
				l1=rs.getString(1);
					
					if (!rs.getString(2).equalsIgnoreCase(l2)) 
						writer.write("\n" + rs.getString(1) + "\t" + rs.getString(2));
					l2=rs.getString(2);					
				
							
			}
		    writer.close();
		}

		// categories with berufsfelder
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("K")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"berufsbereich.csv")));

			writer.write("code\tname");
			query = "SELECT code, name FROM bissimple3.occupationfield order by name";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				System.out.println( rs.getString(1) + "\t" + rs.getString(2));
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
			}
		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"berufsbereich-hierarchy.csv")));
			query = "SELECT category, code FROM bissimple3.occupationfield a  where code like 'F%' order by category, code";
			rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");
				System.out.println(rs.getString(1) + "\t" + rs.getString(2));
						writer.write(rs.getString(1) + "\t" + rs.getString(2));
				i++;
							
			}
		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("S")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"synonym.csv")));

			writer.write("code\tname");
			query = "SELECT code, name FROM bissimple3.synonym order by name";
			rs = stmt.executeQuery(query);
			for (; rs.next();)
			{
				//System.out.println( rs.getString(1) + "\t" + rs.getString(2));
			    writer.write ("\n" + rs.getString(1) + "\t" + rs.getString(2));
			}
		    writer.close();
		    writer = new BufferedWriter(new FileWriter(new File(output+"synonym-hierarchy.csv")));
			query = "SELECT code FROM bissimple3.synonym";
			rs = stmt.executeQuery(query);
			int i = 0;
			for (; rs.next();)
			{
				if (i>0)
					writer.write("\n");
				//System.out.println(rs.getString(1));
				writer.write(rs.getString(1));
				i++;
			}
		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("relations")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"relations.csv")));
		    System.out.println("Generating relations data: ");

			query = "SELECT concat('berufliche-basiskompetenzen\toccupation\t', occupation, '\tcompetence\t', competence) "
				+ "FROM bissimple3.occupationcompetence where competencetype='Essential'";
			// occupation education 1
			query = "select concat('occupation-requires-education\toccupation\t', occupation, '\teducation\t', education) FROM bissimple3.occupationeducation";
			
			writer.write(processRelationQuery(query, stmt, "occupation-requires-education").toString());

			// occupation category
			query = "SELECT occupation, field FROM bissimple3.occupation_field";
			rs = stmt.executeQuery(query);
			int i=0;
			for (; rs.next();)
			{
			    writer.write ("\noccupation-in-category\toccupation\t" + rs.getString(1) + "\tberufsbereich\t" + rs.getString(2));
				//System.out.println( rs.getString(1) + "\t" + rs.getString(2));
				i++;
			}
			System.out.println("\t- " + i + " occupation-in-category"); 
			
			// apprenticeship category
			query = "SELECT apprenticeship, field FROM bissimple3.apprenticeship_field";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
			    writer.write ("\napprenticeship-in-category\tapprenticeship\t" + rs.getString(2) + "\tberufsbereich\t" + rs.getString(1));
				//System.out.println( rs.getString(1) + "\t" + rs.getString(2));
				i++;
			}
			System.out.println("\t- " + i + " apprenticeship-in-category"); 
			
			// occupation working environment
			query = "SELECT occupation, working_env FROM bissimple3.occupationworking_env order by occupation";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
			    writer.write ("\noccupation-workingconditions\toccupation\t" + rs.getString(1) + "\tworking_env\t" + rs.getString(2));
				//System.out.println( rs.getString(1) + "\t" + rs.getString(2));
				i++;
			}
			System.out.println("\t- " + i + " occupation-workingconditions"); 

			// occupation apprenticeship
			query = "SELECT occupation, apprenticeship FROM bissimple3.occupationapprenticeship where occupation in (select code from bissimple3.occupation) order by occupation";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
			    writer.write ("\noccupation-apprenticeship\toccupation\t" + rs.getString(1) + "\tapprenticeship\t" + rs.getString(2));
				//System.out.println( rs.getString(1) + "\t" + rs.getString(2));
				i++;
			}
			System.out.println("\t- " + i + " occupation-apprenticeship"); 

			// occupation apprenticeship
			query = "SELECT berufsuntergruppe, isco FROM bissimple3.berufsuntergruppe_isco order by isco, berufsuntergruppe";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
			    writer.write ("\noccupation-related-isco\toccupation\t" + rs.getString(1) + "\tisco\t" + rs.getString(2));
				//System.out.println( rs.getString(1) + "\t" + rs.getString(2));
				i++;
			}
			writer.write ("\n");
			System.out.println("\t- " + i + " occupation-related-isco"); 
/**/
			// occupation essential competence
			query = "SELECT concat('berufliche-basiskompetenzen\toccupation\t', occupation, '\tcompetence\t', competence) "
				+ "FROM bissimple3.occupationcompetence where competencetype='Essential' or competencetype='Mandatory'";
			writer.write(processRelationQuery(query, stmt, "berufliche-basiskompetenzen").toString());

			// occupation mandatory competence
			writer.write("\n");
			query = "SELECT concat('fachliche-berufliche-kompetenzen\toccupation\t', occupation, '\tcompetence\t', competence) "
				+ "FROM bissimple3.occupationcompetence a, bissimple3.competence b where a.competence=b.code and b.type='0' "
				+ "and a.competencetype='Optional' "
				+ "and concat(occupation, competence) not in (SELECT concat(occupation, competence) "
				+ "FROM bissimple3.occupationcompetence where competencetype='Essential' or competencetype='Mandatory')";
			writer.write(processRelationQuery(query, stmt, "fachliche-berufliche-kompetenzen").toString());


			// occupation optional competence
			writer.write("\n");
			query = "SELECT concat('uberfachliche-berufliche-kompetenzen\toccupation\t', occupation, '\tcompetence\t', competence) "
				+ "FROM bissimple3.occupationcompetence a, bissimple3.competence b where a.competence=b.code and b.type='1' "
				+ "and a.competencetype='Optional' "
				+ "and concat(occupation, competence) not in (SELECT concat(occupation, competence) "
				+ "FROM bissimple3.occupationcompetence where competencetype='Essential' or competencetype='Mandatory')";
			writer.write(processRelationQuery(query, stmt, "uberfachliche-berufliche-kompetenzen").toString());

			// occupation optional competence
			writer.write("\n");
			query = "SELECT concat('zertifikate-kompetenzen\toccupation\t', occupation, '\tcompetence\t', competence) "
				+ "FROM bissimple3.occupationcompetence a, bissimple3.competence b where a.competence=b.code and b.type='2' "
				+ "and a.competencetype='Optional' "
				+ "and concat(occupation, competence) not in (SELECT concat(occupation, competence) "
				+ "FROM bissimple3.occupationcompetence where competencetype='Essential' or competencetype='Mandatory')";
			writer.write(processRelationQuery(query, stmt, "zertifikate-kompetenzen").toString());

			// Occupation Competence based on advertisement
			writer.write ("\n");
			query = "SELECT occupation, competence FROM bissimple3.occupationcompetence_adv e order by occupation";
			rs = stmt.executeQuery(query);
			i=0;
			for (; rs.next();)
			{
			    writer.write ("in-inseraten-besonders-gefragte-berufliche-kompetenzen\toccupation\t" + rs.getString(1) + "\tcompetence\t" + rs.getString(2) + "\n");
				//System.out.println( rs.getString(1) + "\t" + rs.getString(2));
				i++;
			}
			System.out.println("\t- " + i + " occupationAdvertisingCompetence"); 


		    writer.close();
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("relationsWithTwoScores")) {
		    writer = new BufferedWriter(new FileWriter(new File(output+"relationsWithTwoScores.csv")));
		    System.out.println("Generating affinity data: ");

			// occupation manual affinities
			query = "SELECT concat('verwandte-berufsuntergruppen\toccupation\t', occupation_a,'\toccupation\t',occupation_b,'\t',gewicht,'\t',gegengewicht) affinityline "
				+ "FROM bissimple3.occupationaffinity where occupation_b like 'O%'";
			writer.write(processAffinityQuery(query, stmt, "verwandte-berufsuntergruppen").toString());
			
			writer.write("\n");
			query = "SELECT concat('verwandte-spezialisierungen\toccupation\t', occupation_a,'\toccupation\t',occupation_b,'\t',gewicht,'\t',gegengewicht) affinityline "
				+ "FROM bissimple3.occupationaffinity where occupation_b like 'J%'";
			writer.write(processAffinityQuery(query, stmt, "verwandte-berufsuntergruppen").toString());

			writer.write("\n");
			query = "SELECT concat('verwandte-kompetenzen\tcompetence\t', competence_a,'\tcompetence\t',competence_b,'\t',gewicht,'\t',gegengewicht) affinityline "
				+ "FROM bissimple3.competenceaffinity where competence_b like 'P%'";
			writer.write(processAffinityQuery(query, stmt, "verwandte-kompetenzen").toString());

			writer.write("\n");
			query = "SELECT concat('verwandte-kompetenzen-detail\tcompetence\t', competence_a,'\tcompetence\t',competence_b,'\t',gewicht,'\t',gegengewicht) affinityline "
				+ "FROM bissimple3.competenceaffinity where competence_b like 'C%'";
			writer.write(processAffinityQuery(query, stmt, "verwandte-kompetenzen-detail").toString());

		    writer.close();
		}


		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("labels")) {
		    System.out.println("Generating Labels: ");
		    
		    System.out.print("\t- occupation synonyms: ");
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation-synonyms-labels.csv")));
			writer.write("code\tsynonyms\n");
			query = "SELECT distinct b.occupation code, a.name FROM bissimple3.synonym a, bissimple3.occupationsynonym b "
				+ "where a.code=b.synonym order by code, a.name"; 
			writer.write(processLabelQuery(query, stmt).toString());
			writer.close();
			
		    System.out.print("\t- occupation spelling: ");		
		    writer = new BufferedWriter(new FileWriter(new File(output+"occupation-schreibweisen-labels.csv")));
			writer.write("code\tschreibweisen\n");
			query = "SELECT distinct occupation code, trim(spelling) name FROM bissimple3.occupation_spelling "
					+ "order by code, name"; // 
			writer.write(processLabelQuery(query, stmt).toString());
		

		    writer.close();
		}

		stmt.close();
		conn.close();
	}

	public static StringBuilder processAffinityQuery(String query, Statement stmt, String description) throws ClassNotFoundException, SQLException
	{
	ResultSet rs = stmt.executeQuery(query);
	StringBuilder stringBuilder =  new StringBuilder();
	int i=0;
	for (; rs.next();)
	{
		if (i>0)
			stringBuilder.append("\n");
		
		stringBuilder.append(rs.getString(1));
		i++;
	}
	System.out.println("\t- " + i + " " + description); 
	rs.close();
	return stringBuilder;
	
	}

	public static StringBuilder processLabelQuery(String query, Statement stmt) throws ClassNotFoundException, SQLException
	{
	ResultSet rs = stmt.executeQuery(query);
	StringBuilder stringBuilder =  new StringBuilder();
	int i=0;
	for (; rs.next();)
	{
		if (i>0)
			stringBuilder.append("\n");
		
		stringBuilder.append(rs.getString(1) + "\t" + rs.getString(2));
		i++;
	}
	System.out.println(i + " labels"); 
	rs.close();
	return stringBuilder;
	
	}

	public static StringBuilder processRelationQuery(String query, Statement stmt, String description) throws ClassNotFoundException, SQLException
	{
	ResultSet rs = stmt.executeQuery(query);
	StringBuilder stringBuilder =  new StringBuilder();
	int i=0;
	for (; rs.next();)
	{
		if (i>0)
			stringBuilder.append("\n");
		
		stringBuilder.append(rs.getString(1));
		i++;
	}
	System.out.println("\t- " + i + " " + description); 
	rs.close();
	return stringBuilder;
	
	}

	/*
	 * This module explores the BIS Categories.
	 */
	public static void exploreBISOccupation(String occup) throws ClassNotFoundException, SQLException
	{
		//occup = "Software-EntwicklerIn";
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmt3 = null;
		ResultSet rs = null, rs2 = null, rs3 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		stmt3 = conn.createStatement();
		String query;
		Map<Integer, String> qualDic = new HashMap<Integer, String>(); // Dictionary to hold distinct qualifications 

		System.out.println("Occupation: " + occup);
		query = "SELECT distinct a.noteid, a.bezeichnung FROM bis2.stammdaten a where not a.deleted AND a.webstatusext_noteid=2 and a.bezeichnung='"
			+ occup + "'";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("****** Occupation: " + rs.getString(2) + " (" + rs.getString(1) + ") *********");
			System.out.println("\t- Spellings: ");
			query = "SELECT bezeichnung, typ FROM bis2.stammdaten_schreibweisen where stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				System.out.println("\t\t* " + rs2.getString(2) + ": " + rs2.getString(1));
			
			System.out.println("\t- SYNONYMS: ");
			query = "SELECT noteid, bezeichnung FROM bis2.synonyme where stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();) 
			{
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				query = "SELECT bezeichnung, typ FROM bis2.synonyme_schreibweisen where synonyme_noteid=" + rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
					System.out.println("\t\t\t* " + rs3.getString(2) + ": " + rs3.getString(1));
			}
			
			System.out.println("\t- Specializations: ");
			query = "SELECT noteid, bezeichnung FROM bis2.spezielle where stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();) {
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

				query = "SELECT bezeichnung, typ FROM bis2.spezielle_schreibweisen where spezielle_noteid=" + rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
					System.out.println("\t\t\t* " + rs3.getString(2) + ": " + rs3.getString(1));
			
				query = "SELECT a.noteid, a.bezeichnung, a.ebene+3 FROM bis2.qualifikation_detail a where not deleted and noteid in (select qualifikation_detail_noteid from bis2.qualifikation_detail_spezielle where spezielle_noteid ="
					+ rs2.getString(1)
					+ ")";
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();) {
					System.out.println("\t\t   +++ " + rs3.getString(1) + ": " + rs3.getString(2) + " (level " + rs3.getString(3) + ")");
				}
			}

			System.out.println("\t- Arbeitsumfeld:");
			query = "SELECT noteid, bezeichnung FROM bis2.arbeitsumfeld where noteid in (SELECT arbeitsumfeld_noteid FROM bis2.stammdaten_arbeitsumfeld where stammdaten_noteid=" + rs.getString(1) + ")";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
			}

			System.out.println("\t- Deutskenntenisse:");
			query = "SELECT * FROM bis2.stammdaten_deutschniveau where stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(5) + "\t- " + rs2.getString(6) + "\n\t\t" + rs2.getString(7));
			}

			
/*			System.out.println("\t- Work Environemnt:");
			query = "SELECT a.noteid, a.bezeichnung FROM bis2.arbeitsumfeld a, bis2.stammdaten_arbeitsumfeld b where not a.deleted and a.noteid=b.arbeitsumfeld_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
*/
/*			System.out.println("\t- Basic Competences:");
			query = "SELECT a.noteid, a.bezeichnung FROM bis2.qualifikation a, bis2.stammdaten_qualifikation_basis b where not a.deleted and a.webstatus_noteid=2 and a.noteid=b.qualifikation_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();) {
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
				qualDic.put(rs2.getInt(1), rs2.getString(2));
			}
*/


			System.out.println("\t- Competences:");
			query = "SELECT a.noteid, a.bezeichnung FROM bis2.qualifikation a, bis2.stammdaten_qualifikation b where not a.deleted and a.webstatus_noteid=2 and a.noteid=b.qualifikation_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();) {
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
				qualDic.put(rs2.getInt(1), rs2.getString(2));
			}

			System.out.println("\t- Competences Detail:");
			query = "SELECT a.noteid, a.bezeichnung, a.ebene+3 FROM bis2.qualifikation_detail a, bis2.stammdaten_qualifikation_detail b where not a.deleted and a.noteid=b.qualifikation_detail_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2) + " (level " + rs2.getString(3) + ")");


/*			System.out.println("\t- Competences Detail Basic:");
			query = "SELECT a.noteid, a.bezeichnung, a.ebene+3 FROM bis2.qualifikation_detail a, bis2.stammdaten_qualifikation_detail_basis b where not a.deleted and a.noteid=b.qualifikation_detail_noteid and stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2) + " (level " + rs2.getString(3) + ")");
				//exploreQualificationsDetail(rs2.getInt(1));
			}
*/
			System.out.println("\t- Basis Competences:");
			query = "SELECT a.noteid, a.bezeichnung, '3' level FROM bis2.qualifikation a, bis2.stammdaten_qualifikation_basis b where not a.deleted and a.webstatus_noteid=2 and a.noteid=b.qualifikation_noteid and stammdaten_noteid=" + rs.getString(1) +
					" union " + 
					"SELECT a.noteid, a.bezeichnung, a.ebene+3 FROM bis2.qualifikation_detail a, bis2.stammdaten_qualifikation_detail_basis b where not a.deleted and a.noteid=b.qualifikation_detail_noteid and stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2) + " (level " + rs2.getString(3) + ")");
				//exploreQualificationsDetail(rs2.getInt(1));
			}

			System.out.println("\t- Ausbildung:\n\t ** Lehre");
			query = "SELECT noteid, bezeichnung_verordnung FROM bis2.lehrberuf where noteid in (SELECT lehrberuf_noteid FROM bis2.lehrberuf_stammdaten where stammdaten_noteid=" + rs.getString(1) + ")";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
			}

			System.out.println("\t- Inseraten:");
			query = " select noteid, bezeichnung from bis2.qualifikation where noteid in (SELECT qualifikation_noteid FROM bis2.match_stammdaten_qualifikation where stammdaten_noteid=" + rs.getString(1) + ") "
				+ "union "
				+ "select noteid, bezeichnung from bis2.qualifikation_detail where noteid in (SELECT qualifikation_detail_noteid FROM bis2.match_stammdaten_qualifikation_detail where stammdaten_noteid=" + rs.getString(1) + ")";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
			}

			System.out.println("\t- ISCO-08:");
			query = "SELECT code, titel FROM bis2.isco_berufsgattung where noteid in (SELECT isco_berufsgattung_noteid FROM bis2.stammdaten_isco_berufsgattung where stammdaten_noteid=" + rs.getString(1) + ")";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
			}

			System.out.println("\t- Verwandte Beruf:");
			query = "SELECT noteid, bezeichnung FROM bis2.stammdaten where noteid in (SELECT stammdaten_verwandt_noteid FROM bis2.stammdaten_verwandt where stammdaten_noteid=" + rs.getString(1) + ")";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
			}

			System.out.println("\t- AMS-6-Steller:");
			query = "SELECT Nummer, bezeichnung FROM bis2.amssechssteller where noteid in (SELECT amssechssteller_noteid FROM bis2.stammdaten_amssechssteller where stammdaten_noteid=" + rs.getString(1) + ")";
			//System.out.println(query);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(1) + ": " + rs2.getString(2));
			}

	
			System.out.println("\nEXPLORING COMPETENCES:");
			Iterator<Map.Entry<Integer, String>> it = qualDic.entrySet().iterator();
			while (it.hasNext())
			{
				Map.Entry<Integer, String> entry = it.next();
				System.out.println("  - " + entry.getValue() + " (" + entry.getKey() + ")");
				exploreQualificationsDetail_inner(entry.getKey());
			}
			rs2.close();

		}
		rs.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	/*
	 * This module counts the number of records in each table for a given database (BIS)
	 */
	public static void DBcounts(String dbName)
	{
		try
		{

			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null;
			ResultSet rs2 = null;

			System.out.println(new Date());
			rs = stmt.executeQuery("use " + dbName);
			rs = stmt.executeQuery("show tables");
			int i = 1;
			while (rs.next())
			{
				rs2 = stmt2.executeQuery("select count(*) from " + rs.getString(1));
				if (rs2.next())
					System.out.println(i++ + "- " + rs.getString(1) + ": \n\t -->" + rs2.getString(1) + " records");
				//if (rs2.getInt(1) == 0)
				//	stmt2.executeUpdate("drop table " + rs.getString(1));
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

	/* Loads data properties into the system
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