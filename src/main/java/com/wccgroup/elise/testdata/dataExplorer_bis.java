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

/*
 * BIS/ASM data explorer based on the BIS data model and data, aims at exploring and understanding 
 * the modeling of the following components within BIS, namely:
 * Qualifications and competences,
 * Educations, training and certificates
 * Afinities and synonyms, etc.
 */
public class dataExplorer_bis
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
			System.out.println("\t- 10- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			exploreCompetences("BarkeeperIn"); // LederfärberIn Koch, Köchin DatenerfasserIn LederfärberIn Java-ProgrammiererIn "Pfarrerskoch, Pfarrersköchin" 
			break;
		case "2":
			exploreCategories();
			break;
		case "3":
			exploreCategoriesCounts();
			break;
		case "4":
			exploreQualifications();
			break;
		case "5":
			exploreQualificationsDetail("Gerben"); //MS Office//Java//Nahrungsmittel//87//"Bauplanungskenntnisse"); 
			break;
		case "6":
			exploreAffinities(); //"Bauplanungskenntnisse"); 
			break;
		case "7":
			exploreOccupation2("BarkeeperIn"); //Software-EntwicklerIn//RaumpflegerIn//TischlerIn//LagerarbeiterIn//Küchenhilfskraft//SchlosserIn im Metallbereich//ElektroinstallationstechnikerIn//Einzelhandelskaufmann/-frau//LebensmittelverkäuferIn//TaxichauffeurIn//Koch, Köchin //Restaurantfachmann/-frau//"Bauplanungskenntnisse"); 
			break;
		case "8":
			DBcounts("bisams"); // perform statistics on the data
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
		query = "SELECT noteid, bezeichnung FROM bissimple.stammdatenkategorien WHERE NOT deleted AND webstatus_noteid=2";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println(rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bissimple.berufsfelder WHERE NOT deleted AND webstatus_noteid=2 AND stammdatenkategorien_noteid="
				+ rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				query = "SELECT a.noteid, a.bezeichnung FROM bissimple.stammdaten a, bissimple.stammdaten_berufsfelder b where not a.deleted AND a.webstatusext_noteid=2 and a.noteid=b.stammdaten_noteid and b.berufsfelder_noteid="
					+ rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
				{
					System.out.println("\t\t- " + rs3.getString(2) + " (" + rs3.getString(1) + ")");
					query = "SELECT noteid, bezeichnung FROM bissimple.spezielle where stammdaten_noteid=" + rs3.getString(1);
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
		query = "SELECT noteid, bezeichnung FROM bissimple.stammdatenkategorien WHERE NOT deleted AND webstatus_noteid=2";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println(rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bissimple.berufsfelder WHERE NOT deleted AND webstatus_noteid=2 AND stammdatenkategorien_noteid="
				+ rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				query = "SELECT a.noteid, a.bezeichnung FROM bissimple.stammdaten a, bissimple.stammdaten_berufsfelder b where not a.deleted AND a.webstatusext_noteid=2 and a.noteid=b.stammdaten_noteid and b.berufsfelder_noteid="
					+ rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
				{
					System.out.println("\t\t- " + rs3.getString(2) + " (" + rs3.getString(1) + "): ");
					query = "SELECT count(*) FROM bissimple.spezielle where stammdaten_noteid=" + rs3.getString(1);
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
		Statement stmt = null, stmt2 = null, stmt3 = null, stmt4 = null;
		ResultSet rs = null, rs2 = null, rs3 = null, rs4 = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		stmt3 = conn.createStatement();
		stmt4 = conn.createStatement();
		String query;

		System.out.println("Counts of Competences per ");
		query = "SELECT noteid, bezeichnung FROM bissimple.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println(rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT bezeichnung FROM bissimple.qualifikation WHERE qualifikationsbereich_noteid="
				+ rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			while (rs2.next())
			{
				System.out.println("\t+ " + rs2.getString(1) + " qualifications)");
/*				query = "SELECT a.noteid, a.bezeichnung FROM bissimple.stammdaten a, bissimple.stammdaten_berufsfelder b where not a.deleted AND a.webstatusext_noteid=2 and a.noteid=b.stammdaten_noteid and b.berufsfelder_noteid="
					+ rs2.getString(1);
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();)
				{
					System.out.println("\t\t- " + rs3.getString(2) + " (" + rs3.getString(1) + "): ");
					query = "SELECT count(*) FROM bissimple.spezielle where stammdaten_noteid=" + rs3.getString(1);
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
		query = "SELECT noteid, bezeichnung FROM bissimple.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bissimple.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
		System.out.println("SOFT SKILLS");
		query = "SELECT noteid, bezeichnung FROM bissimple.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=0 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bissimple.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
		System.out.println("CERTIFICATE");
		query = "SELECT noteid, bezeichnung FROM bissimple.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=2 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bissimple.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
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
		query = "SELECT noteid, bezeichnung FROM bissimple.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=1 order by bezeichnung";
		rs = stmt.executeQuery(query);
			query = "SELECT noteid, bezeichnung FROM bissimple.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ qual
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
/*		System.out.println("SOFT SKILLS");
		query = "SELECT noteid, bezeichnung FROM bissimple.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=0 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bissimple.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
				+ rs.getString(1)
				+ "  order by bezeichnung";
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
			{
				System.out.println("\t\t+ " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

			}
		}
		System.out.println("CERTIFICATE");
		query = "SELECT noteid, bezeichnung FROM bissimple.qualifikationsbereich WHERE NOT deleted AND webstatus_noteid=2 and fachlich_flag=2 order by bezeichnung";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("\t+ " + rs.getString(2) + " (" + rs.getString(1) + ")");

			query = "SELECT noteid, bezeichnung FROM bissimple.qualifikation WHERE NOT deleted AND webstatus_noteid=2 AND qualifikationsbereich_noteid="
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
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail bb"
			+ " ON aa.thesid=bb.parent_thesid"
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail cc"
			+ " ON bb.thesid=cc.parent_thesid"
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail dd"
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
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail bb"
			+ " ON aa.thesid=bb.parent_thesid"
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail cc"
			+ " ON bb.thesid=cc.parent_thesid"
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail dd"
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
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail bb"
			+ " ON aa.thesid=bb.parent_thesid"
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail cc"
			+ " ON bb.thesid=cc.parent_thesid"
			+ " LEFT OUTER JOIN bissimple.qualifikation_detail dd"
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
	 * This module explores the BIS Categories.
	 */
	public static void exploreOccupation(String occup) throws ClassNotFoundException, SQLException
	{
		//occup = "Software-EntwicklerIn";
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
		Map<Integer, String> qualDic = new HashMap<Integer, String>(); // Dictionary to hold distinct qualifications 

		System.out.println("Occupation: " + occup);
		query = "SELECT distinct a.noteid, a.bezeichnung FROM bissimple.stammdaten a, bissimple.stammdaten_berufsfelder b where not a.deleted AND a.webstatusext_noteid=2 and a.noteid=b.stammdaten_noteid and a.bezeichnung='"
			+ occup + "'";
		rs = stmt.executeQuery(query);
		for (; rs.next();)
		{
			System.out.println("****** Occupation: " + rs.getString(2) + " (" + rs.getString(1) + ") *********");
/*			System.out.println("\t- SYNONYMS: ");
			query = "SELECT noteid, bezeichnung FROM bissimple.synonyme where stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
*/			
			System.out.println("\t- Specializations: ");
			query = "SELECT noteid, bezeichnung FROM bissimple.spezielle where stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();) {
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");

				query = "SELECT * FROM bissimple.qualifikation_detail where not deleted and noteid in (select qualifikation_detail_noteid from bissimple.qualifikation_detail_spezielle where spezielle_noteid ="
					+ rs2.getString(1)
					+ ")";
				rs3 = stmt3.executeQuery(query);
				for (; rs3.next();) {
					System.out.println("\t\t\t- " + rs3.getString(1) + ": " + rs3.getString(4) + " (level " + rs3.getString("ebene") + ")");
				}
			}

/*			System.out.println("\t- Work Environemnt:");
			query = "SELECT a.noteid, a.bezeichnung FROM bissimple.arbeitsumfeld a, bissimple.stammdaten_arbeitsumfeld b where not a.deleted and a.noteid=b.arbeitsumfeld_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
*/
			System.out.println("\t- Basic Competences:");
			query = "SELECT a.noteid, a.bezeichnung FROM bissimple.qualifikation a, bissimple.stammdaten_qualifikation_basis b where not a.deleted and a.webstatus_noteid=2 and a.noteid=b.qualifikation_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();) {
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				qualDic.put(rs2.getInt(1), rs2.getString(2));
			}



			System.out.println("\t- Competences:");
			query = "SELECT a.noteid, a.bezeichnung FROM bissimple.qualifikation a, bissimple.stammdaten_qualifikation b where not a.deleted and a.webstatus_noteid=2 and a.noteid=b.qualifikation_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();) {
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				qualDic.put(rs2.getInt(1), rs2.getString(2));
			}

			System.out.println("\t- Competences Detail:");
			query = "SELECT a.noteid, a.bezeichnung FROM bissimple.qualifikation_detail a, bissimple.stammdaten_qualifikation_detail b where not a.deleted and a.noteid=b.qualifikation_detail_noteid and stammdaten_noteid=" + rs.getString(1);
			//System.out.println(query); //TODO: remove println
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();)
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");


			System.out.println("\t- Competences Detail Basic:");
			query = "SELECT a.noteid, a.bezeichnung FROM bissimple.qualifikation_detail a, bissimple.stammdaten_qualifikation_detail_basis b where not a.deleted and a.noteid=b.qualifikation_detail_noteid and stammdaten_noteid=" + rs.getString(1);
			rs2 = stmt2.executeQuery(query);
			for (; rs2.next();){
				System.out.println("\t\t- " + rs2.getString(2) + " (" + rs2.getString(1) + ")");
				//exploreQualificationsDetail(rs2.getInt(1));
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
	 * This module explores the BIS Categories.
	 */
	public static void exploreOccupation2(String occup) throws ClassNotFoundException, SQLException
	{
		//occup = "Software-EntwicklerIn";
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