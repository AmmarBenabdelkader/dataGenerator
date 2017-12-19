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
 * BIS to WTDB (WCC Taxonomy Database) is a tool that converts data from BIS to a cleaned and simplified format that can be used internally at WCC for various purposes
 * such as testing the taxonomy manager, generating test data, and perform benchmarking tests
 */
public class Bis2WTDB
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
		BIStoWTDB("D");  // cleans the database
		
		Date start= new Date();
		System.out.println("\n*** Generating WCC Taxonomy Data Set ***");
		BIStoWTDB("O"); 
		BIStoWTDB("C"); 
		BIStoWTDB("E"); 
		BIStoWTDB("OI"); 
		BIStoWTDB("A"); 
		BIStoWTDB("WE"); 
		BIStoWTDB("Other"); 
		BIStoWTDB("OL"); 
		 
		//BIStoWTDB("occupationAffinities"); 
		//BIStoWTDB("competenceAffinities");
/**/		
		System.out.println("\nStart at: " + start + "\nEnded at: " + new Date());	
	}


	/*
	 * This module extracts BIS data from SQL tables and translates them into a simplified and clean format to be used internally at WCC for various purposes
	 * @para item for which data is to be extracted, the following values can be specified:
	 * 		all: to extract all data at once
	 * 		D:		truncate all the data including relationships 
	 * 		B:		to extract the occupations categories (berufsbereich)
	 * 		O:		to extract the occupations
	 * 		E:		to extract the education
	 * 		C:		to extract the competences
	 * 		OC:		to extract the relationships occupation/competence
	 * 		S:		to extract the synonyms
	 * 		OS:		to extract the relationships occupation/synonyms
	 * 		OE:		to extract the relationships occupation/education
	 * 		W:		to extract the working environment
	 * 		OW:		to extract the relationships occupation/working environment
	 * 		etc.
	 */
	public static void BIStoWTDB(String item) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		String query;
		
		//Occupation Links
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("D")) {
			query = "Delete From bissimple3.occupationapprenticeship";
				System.out.println("removing occupationapprenticeship data: " + stmt.executeUpdate(query));
			query = "Delete From bissimple3.occupationEducation";
				System.out.println("removing occupationEducation:\t" + stmt.executeUpdate(query));
			query = "Delete From bissimple3.occupationCompetence";
				System.out.println("removing occupationCompetence data:\t" + stmt.executeUpdate(query));			
			query = "Delete From bissimple3.occupationSynonym";
				System.out.println("removing occupationSynonym data:\t" + stmt.executeUpdate(query));
			
			query = "Delete From bissimple3.occupationAffinity";
				System.out.println("removing occupationAffinity data:\t" + stmt.executeUpdate(query));
			
			query = "Delete From bissimple3.competenceAffinity";
				System.out.println("removing competenceAffinity data:\t" + stmt.executeUpdate(query));
			query = "Delete From bissimple3.occupation_field";
				System.out.println("removing occupation_field:\t" + stmt.executeUpdate(query));
				
			query = "Delete From bissimple3.occupationField";
				System.out.println("removing occupationField:\t" + stmt.executeUpdate(query));
				
			query = "Delete From bissimple3.occupationcompetence_adv";
				System.out.println("removing occupationcompetence_adv:\t" + stmt.executeUpdate(query));
				
			query = "Delete From bissimple3.occupationworking_env";
				System.out.println("removing occupationworking_env:\t" + stmt.executeUpdate(query));
												
			query = "Delete From bissimple3.occupation";
				System.out.println("removing occupation:\t" + stmt.executeUpdate(query));
				
			query = "Delete From bissimple3.apprenticeshipInterestarea";
				System.out.println("removing apprenticeshipInterestarea data: " + stmt.executeUpdate(query));
				
			query = "delete from bissimple3.interestarea";
				System.out.println("removing interestarea data: " + stmt.executeUpdate(query));
				
			query = "delete from bissimple3.apprenticeshipcompetence";
				System.out.println("removing apprenticeshipcompetence data: " + stmt.executeUpdate(query));
				
			query = "delete from bissimple3.apprenticeship";
				System.out.println("removing apprenticeship data: " + stmt.executeUpdate(query));
				
			query = "Delete From bissimple3.competence";
				System.out.println("removing competence data:\t" + stmt.executeUpdate(query));
				
			query = "DELETE FROM bissimple3.education";
				System.out.println("removing education data: " + stmt.executeUpdate(query));
				
			query = "DELETE FROM bissimple3.synonym";
				System.out.println("removing synonym data: " + stmt.executeUpdate(query));
				
				query = "DELETE FROM bissimple3.working_env";
				System.out.println("removing working_env data: " + stmt.executeUpdate(query));
				
				query = "delete from bissimple3.berufsuntergruppe_isco";
				System.out.println("deleting berufsuntergruppe_isco data :\t" + stmt.executeUpdate(query));
				
				query = "delete from bissimple3.isco_Berufsgattungen";
				System.out.println("deleting isco_Berufsgattungen data :\t" + stmt.executeUpdate(query));
				
				query = "delete from bissimple3.occupation_spelling";
				System.out.println("deleting occupation_spelling data :\t" + stmt.executeUpdate(query));
				
				
		}
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("O")) {
			//************** occupation_isco **************
			
			System.out.println("Extracting Beruf data:");
			query = "insert into bissimple3.occupationField (SELECT concat('K', noteid) code, bezeichnung name, 'root' parent "
				+ "FROM bis4.stammdatenkategorien where not deleted and webstatus_noteid=2 )";
			System.out.println("\t- Berufsbereich (bis L1):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationField (SELECT concat('F', noteid) code, bezeichnung name, concat('K', stammdatenkategorien_noteid) parent "
				+ "FROM bis4.berufsfelder where not deleted and webstatus_noteid=2 )";
			System.out.println("\t- Berufsfelder data (bis L2):\t" + stmt.executeUpdate(query));
						
			//stammdaten -> occupation_isco
			query = "insert into bissimple3.occupation ("
				+ "SELECT concat('O',a.noteid) code, a.bezeichnung name, Haupttaetigkeit description, Beschaeftigungsmoeglichkeiten, null, null, null parent FROM bis4.stammdaten a "
				+ "where not a.deleted and a.webstatusext_noteid in ('2'))";
				//+ "where not a.deleted and a.webstatusext_noteid in ('2', '1'))";
			System.out.println("\t- Berufsgruppen data (bis L3):\t" + stmt.executeUpdate(query));
						
			//spezielle -> occupation_isco
			query = "insert into bissimple3.occupation (SELECT concat('J',a.noteid) code, a.bezeichnung name, null description, null, null, "
				+ "b.bezeichnung category, concat('O',a.stammdaten_noteid) parent "
				+ "FROM bis4.spezielle a,  bis4.speziellekategorien b where a.speziellekategorien_noteid = b.noteid "
				+ "and a.stammdaten_noteid in (SELECT a.noteid FROM bis4.stammdaten a where not a.deleted and a.webstatusext_noteid in ('2')))";
				//+ "where not a.deleted and a.webstatusext_noteid in ('2', '1')))";
				//+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid and b.code like '5223%'))";
			System.out.println("\t- Berufsgruppen data (bis L4):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_field (SELECT concat('O',stammdaten_noteid) occupation, concat('F',berufsfelder_noteid) field "
				+ "FROM bis4.stammdaten_berufsfelder where not deleted "
				+ "and concat('F',berufsfelder_noteid) in (select code from bissimple3.occupationField) "
				+ "and concat('O',stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-berufenbereich links:\t" + stmt.executeUpdate(query));
			
			//Occupation Spelling
			
			query = "insert into bissimple3.occupation_spelling (SELECT distinct concat('O',stammdaten_noteid) code, typ, bezeichnung "
				+ "FROM bis4.stammdaten_schreibweisen where primary_marker=1 and concat('O',stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-schreibweisen:\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_spelling (SELECT distinct concat('J',spezielle_noteid) code, typ, bezeichnung "
				+ "FROM bis4.spezielle_schreibweisen where primary_marker=1 and concat('J',spezielle_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- spezielle-schreibweisen:\t" + stmt.executeUpdate(query));
			
/*			query = "update bissimple3.occupation set spelling = (select spelling from bissimple3.occupation_spelling where occupation=code)";
			System.out.println("\t- Berufsgruppen-schreibweisen updates:\t" + stmt.executeUpdate(query));
			
			query = "update bissimple3.occupation set spelling = (select spelling from bissimple3.occupation_spelling where occupation=code)";
			System.out.println("\t- spezielle-schreibweisen updates:\t" + stmt.executeUpdate(query));
			
			query = "SET group_concat_max_len=15000";
			System.out.println("\t- SET group_concat_max_len=15000:\t" + stmt.executeUpdate(query));
			query = "insert into bissimple3.occupation_spelling (SELECT concat('O',stammdaten_noteid) code, GROUP_CONCAT(concat(typ, ': ' , bezeichnung) SEPARATOR ' | ') "
				+ "FROM bis4.stammdaten_schreibweisen where concat('O',stammdaten_noteid) in (select code from bissimple.occupation) group by stammdaten_noteid)";
			System.out.println("\t- Berufsgruppen-schreibweisen:\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_spelling (SELECT concat('J',spezielle_noteid) code, GROUP_CONCAT(concat(typ, ': ' , bezeichnung) SEPARATOR ' | ') "
				+ "FROM bis4.spezielle_schreibweisen where concat('J',spezielle_noteid) in (select code from bissimple.occupation) group by spezielle_noteid)";
			System.out.println("\t- spezielle-schreibweisen:\t" + stmt.executeUpdate(query));
			
			query = "update bissimple3.occupation set spelling = (select spelling from bissimple3.occupation_spelling where occupation=code)";
			System.out.println("\t- Berufsgruppen-schreibweisen updates:\t" + stmt.executeUpdate(query));
			
			query = "update bissimple3.occupation set spelling = (select spelling from bissimple3.occupation_spelling where occupation=code)";
			System.out.println("\t- spezielle-schreibweisen updates:\t" + stmt.executeUpdate(query));
*/		}

		if (item.equalsIgnoreCase("all1") || item.equalsIgnoreCase("O1")) {
			//link occupation -> field
			query = "insert into bissimple3.occupationField (SELECT concat('K', noteid) code, bezeichnung name, 'root' parent "
				+ "FROM bis4.stammdatenkategorien where not deleted and webstatus_noteid=2 )";
			System.out.println("inserting occupation categories data (bis L1):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationField (SELECT concat('F', noteid) code, bezeichnung name, concat('K', stammdatenkategorien_noteid) parent "
				+ "FROM bis4.berufsfelder where not deleted and webstatus_noteid=2 )";
			System.out.println("inserting occupation fields data (bis L2):\t" + stmt.executeUpdate(query));
			
			//spezielle -> occupation
			query = "insert into bissimple3.occupation (SELECT concat('J',noteid) code, bezeichnung name, concat('O',stammdaten_noteid) parent "
				+ "FROM bis4.spezielle where stammdaten_noteid in (SELECT a.noteid FROM bis4.stammdaten a,  bis4.berufsfelder b, bis4.stammdaten_berufsfelder c "
				+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.berufsfelder_noteid=b.noteid and b.noteid in ('308', '309', '310')))";
				//+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.berufsfelder_noteid=b.noteid and b.stammdatenkategorien_noteid = '89'))";
			System.out.println("inserting occupation data (bis L4):\t" + stmt.executeUpdate(query));
			
			//stammdaten -> occupation
			query = "insert into bissimple3.occupation ("
				+ "SELECT concat('O',a.noteid) code, a.bezeichnung name, concat('F', b.noteid) parent FROM bis4.stammdaten a,  bis4.berufsfelder b, bis4.stammdaten_berufsfelder c "
				+ "where not a.deleted and a.webstatusext_noteid in ('2') and a.noteid=c.stammdaten_noteid and c.berufsfelder_noteid=b.noteid)";
			System.out.println("inserting occupation data (bis L3):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_field (SELECT concat('O',stammdaten_noteid) occupation, concat('F',berufsfelder_noteid) field "
				+ "FROM bis4.stammdaten_berufsfelder where not deleted "
				+ "and concat('F',berufsfelder_noteid) in (select code from bissimple3.occupationField) "
				+ "and concat('O',stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("inserting occupation fields links:\t" + stmt.executeUpdate(query));
			
/*			//************** occupation_isco **************
			query = "DROP table if exists bissimple3.occupation_isco";
			stmt.executeUpdate(query);
			
			//spezielle -> occupation_isco
			query = "create table bissimple3.occupation_isco (SELECT concat('J',noteid) code, bezeichnung name, concat('O',stammdaten_noteid) parent "
				+ "FROM bis4.spezielle where stammdaten_noteid in (SELECT a.noteid FROM bis4.stammdaten a,  bis4.isco_berufsgattung b, bis4.stammdaten_isco_berufsgattung c "
				+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid and b.code like '5223%'))";
			System.out.println("inserting occupation_isco data (bis L4):\t" + stmt.executeUpdate(query));
			
			//stammdaten -> occupation_isco
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT concat('O',a.noteid) code, a.bezeichnung name, b.code parent FROM bis4.stammdaten a,  bis4.isco_berufsgattung b, bis4.stammdaten_isco_berufsgattung c "
				+ "where not a.deleted and a.webstatusext_noteid in ('2', '1') and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (bis L3):\t" + stmt.executeUpdate(query));
			
			//spezielle -> occupation_isco
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgattung a,  bis4.isco_berufsuntergruppe b "
				+ "where not a.deleted and a.berufsuntergruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L4):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_isco (SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsuntergruppe a,  bis4.isco_berufsgruppe b where not a.deleted and a.berufsgruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L3):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgruppe a,  bis4.isco_berufshauptgruppe b "
				+ "where not a.deleted and a.berufshauptgruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L2):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT code, titel name, 'null' parent FROM bis4.isco_berufshauptgruppe where not deleted)";
			System.out.println("inserting occupation_isco data (isco L1):\t" + stmt.executeUpdate(query));
*/			
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("OI")) {
			//************** occupation_isco **************

			System.out.println("Extracting isco_Berufsgattungen data:");
			
			query = "insert into bissimple3.isco_Berufsgattungen ("
				+ "SELECT code, titel name, 'null' parent FROM bis4.isco_berufshauptgruppe where not deleted)";
			System.out.println("\t- isco_Berufsgattungen (isco L1):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.isco_Berufsgattungen ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgruppe a,  bis4.isco_berufshauptgruppe b "
				+ "where not a.deleted and a.berufshauptgruppe_noteid=b.noteid)";
			System.out.println("\t- isco_Berufsgattungen (isco L2):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.isco_Berufsgattungen (SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsuntergruppe a,  bis4.isco_berufsgruppe b where not a.deleted and a.berufsgruppe_noteid=b.noteid)";
			System.out.println("\t- isco_Berufsgattungen data (isco L3):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.isco_Berufsgattungen ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgattung a,  bis4.isco_berufsuntergruppe b "
				+ "where not a.deleted and a.berufsuntergruppe_noteid=b.noteid)";
			System.out.println("\t- isco_Berufsgattungen (isco L4):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.berufsuntergruppe_isco ("
				+ "SELECT concat('O',c.stammdaten_noteid) berufsuntergruppe, b.code isco FROM bis4.isco_berufsgattung b, bis4.stammdaten_isco_berufsgattung c "
				+ "where not c.deleted and c.isco_berufsgattung_noteid=b.noteid and concat('O',c.stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- berufsuntergruppe_isco links:\t" + stmt.executeUpdate(query));
		}

		if (item.equalsIgnoreCase("all1") || item.equalsIgnoreCase("OI1")) {
			//************** occupation_isco **************
			query = "DROP table if exists bissimple3.occupation_isco";
			stmt.executeUpdate(query);
			
			//spezielle -> occupation_isco
			query = "create table bissimple3.occupation_isco (SELECT concat('J',noteid) code, bezeichnung name, concat('O',stammdaten_noteid) parent "
				+ "FROM bis4.spezielle where stammdaten_noteid in (SELECT a.noteid FROM bis4.stammdaten a,  bis4.isco_berufsgattung b, bis4.stammdaten_isco_berufsgattung c "
				+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid))";
				//+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid and b.code like '5223%'))";
			System.out.println("inserting occupation_isco data (bis L4):\t" + stmt.executeUpdate(query));
			
			//stammdaten -> occupation_isco
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT concat('O',a.noteid) code, a.bezeichnung name, b.code parent FROM bis4.stammdaten a,  bis4.isco_berufsgattung b, bis4.stammdaten_isco_berufsgattung c "
				+ "where not a.deleted and a.webstatusext_noteid in ('2', '1') and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (bis L3):\t" + stmt.executeUpdate(query));
			
			//spezielle -> occupation_isco
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgattung a,  bis4.isco_berufsuntergruppe b "
				+ "where not a.deleted and a.berufsuntergruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L4):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_isco (SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsuntergruppe a,  bis4.isco_berufsgruppe b where not a.deleted and a.berufsgruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L3):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgruppe a,  bis4.isco_berufshauptgruppe b "
				+ "where not a.deleted and a.berufshauptgruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L2):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation_isco ("
				+ "SELECT code, titel name, 'null' parent FROM bis4.isco_berufshauptgruppe where not deleted)";
			System.out.println("inserting occupation_isco data (isco L1):\t" + stmt.executeUpdate(query));
			
		}

		if (item.equalsIgnoreCase("all1") || item.equalsIgnoreCase("O3")) {
			//************** occupation_isco **************
			
			//link occupation -> field
			query = "insert into bissimple3.occupationField (SELECT concat('K', noteid) code, bezeichnung name, 'root' parent "
				+ "FROM bis4.stammdatenkategorien where not deleted and webstatus_noteid=2 )";
			System.out.println("inserting occupation categories data (bis L1):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationField (SELECT concat('F', noteid) code, bezeichnung name, concat('K', stammdatenkategorien_noteid) parent "
				+ "FROM bis4.berufsfelder where not deleted and webstatus_noteid=2 )";
			System.out.println("inserting occupation fields data (bis L2):\t" + stmt.executeUpdate(query));
			
			//spezielle -> occupation_isco
			query = "insert into bissimple3.occupation (SELECT concat('J',noteid) code, bezeichnung name, Haupttatigkeit description, concat('O',stammdaten_noteid) parent "
				+ "FROM bis4.spezielle where stammdaten_noteid in (SELECT a.noteid FROM bis4.stammdaten a,  bis4.isco_berufsgattung b, bis4.stammdaten_isco_berufsgattung c "
				+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid))";
				//+ "where not a.deleted and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid and b.code like '5223%'))";
			System.out.println("inserting occupation_isco data (bis L4):\t" + stmt.executeUpdate(query));
			
			//stammdaten -> occupation_isco
			query = "insert into bissimple3.occupation ("
				+ "SELECT concat('O',a.noteid) code, a.bezeichnung name, null description, b.code parent FROM bis4.stammdaten a,  bis4.isco_berufsgattung b, bis4.stammdaten_isco_berufsgattung c "
				+ "where not a.deleted and a.webstatusext_noteid in ('2', '1') and a.noteid=c.stammdaten_noteid and c.isco_berufsgattung_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (bis L3):\t" + stmt.executeUpdate(query));
			
			//spezielle -> occupation_isco
			query = "insert into bissimple3.occupation ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgattung a,  bis4.isco_berufsuntergruppe b "
				+ "where not a.deleted and a.berufsuntergruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L4):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation (SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsuntergruppe a,  bis4.isco_berufsgruppe b where not a.deleted and a.berufsgruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L3):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation ("
				+ "SELECT a.code, a.titel name, b.code parent FROM bis4.isco_berufsgruppe a,  bis4.isco_berufshauptgruppe b "
				+ "where not a.deleted and a.berufshauptgruppe_noteid=b.noteid)";
			System.out.println("inserting occupation_isco data (isco L2):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupation ("
				+ "SELECT code, titel name, 'null' parent FROM bis4.isco_berufshauptgruppe where not deleted)";
			System.out.println("inserting occupation_isco data (isco L1):\t" + stmt.executeUpdate(query));
			
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("C")) {
			System.out.println("Extracting Competence data:");
			stmt.executeUpdate("insert into bissimple3.competence values ('N1','Fachliche berufliche Kompetenzen',null,1,0)");
			stmt.executeUpdate("insert into bissimple3.competence values ('N0','Überfachliche berufliche Kompetenzen',null,1,1)");
			stmt.executeUpdate("insert into bissimple3.competence values ('N2','Zertifikate und Ausbildungsabschlüss',null,1,2)");
			stmt.executeUpdate("insert into bissimple3.competence values ('N3','lehrberuf Kompetenzen',null,1,3)");
			query = "insert into bissimple3.competence ("
				+ "SELECT concat('Q',thesid) code, bezeichnung, concat('N',fachlich_flag),2,0 "
				+ "FROM bis4.qualifikationsbereich where not deleted and webstatus_noteid=2 and fachlich_flag in ('0','1','2','3') "
				+ "order by fachlich_flag, bezeichnung )";
			System.out.println("\t- qualifikation fachlich_flag (level 1-2):\t" + stmt.executeUpdate(query));
			
			
/*			query = "insert into bissimple3.competence ("
				+ "SELECT concat('Q',thesid) noteid, bezeichnung, concat('N',fachlich_flag) Parent, 2 _level, 0 zertifikat "
				+ "FROM bis4.qualifikationsbereich "
				+ "where not deleted AND webstatus_noteid=2)";
			System.out.println("\t- qualifikationsbereich (level 2):\t" + stmt.executeUpdate(query));
*/			
			query = "insert into bissimple3.competence ("
				+ "SELECT concat('P',a.thesid) noteid, a.bezeichnung, concat('Q',b.thesid) Parent, 3 _level, a.zertifikat "
				+ "FROM bis4.qualifikation a, bis4.qualifikationsbereich b "
				+ "where not a.deleted AND a.webstatus_noteid=2 "
				+ "and a.qualifikationsbereich_noteid=b.noteid)";
			System.out.println("\t- qualifikation (level 3):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competence ("
				+ "SELECT concat('C',thesid) code, bezeichnung name, concat('P',parent_thesid) parent, ebene+3 _level, zertifikat "
				+ "FROM bis4.qualifikation_detail where not deleted and ebene=1)";
			System.out.println("\t- qualifikation_detail (level 4):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competence ("
				+ "SELECT concat('C',thesid) code, bezeichnung name, concat('C',parent_thesid) parent, ebene+3 _level, zertifikat "
				+ "FROM bis4.qualifikation_detail where not deleted and ebene>1)";
			System.out.println("\t- qualifikation_detail (level 4-11):\t" + stmt.executeUpdate(query));
			
			// updating all the competences with their type (propagation from parent at level 1)
			for (int i=1; i<=9; i++) {
				stmt.executeUpdate("drop table if exists bissimple3.competence_copy");
				stmt.executeUpdate("create table bissimple3.competence_copy (select * from bissimple3.competence)");
				query = "update bissimple3.competence set type=(select type from bissimple3.competence_copy "
					+ "where bissimple3.competence.parent = bissimple3.competence_copy.code and _level=" + i + ") where _level=" + (i+1);
				System.out.println("\t- updating competence type at level " +  i +  ":\t" + stmt.executeUpdate(query));
			}
		}
		
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("Other")) {

			System.out.println("Extracting additional data:");
			query = "insert into bissimple3.synonym ("
				+ "SELECT concat('S',noteid) code, bezeichnung name FROM bis4.Synonyme)";
			System.out.println("\t- synonym:\t" + stmt.executeUpdate(query));

			query = "Insert into bissimple3.occupationSynonym ("
				+ "SELECT concat('O',stammdaten_noteid) occupation, concat('S',noteid) synonym FROM bis4.Synonyme "
				+ "where concat('S',noteid) in (select code from bissimple3.synonym) "
				+ "and concat('O',stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-Synonym (links):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationcompetence_adv ("
				+ "SELECT distinct concat('O',b.stammdaten_noteid) occupation, concat('C',a.thesid) competence, b.quali_typ competenceType, a.zertifikat "
				+ "FROM bis4.qualifikation_detail a, bis4.snapshot_quali_stammberuf b "
				+ "where a.bezeichnung=b.quali_bezeichnung and quali_typ='Detailqualifikation' and not a.deleted "
				+ ")";
				System.out.println("\t- occupationcompetence_adv: (Detailqualifikation)\t" + stmt.executeUpdate(query));
				
			query = "insert into bissimple3.occupationcompetence_adv ( "
				+ "SELECT distinct concat('O',b.stammdaten_noteid) occupation, concat('P',a.thesid) competence, b.quali_typ competenceType, a.zertifikat "
				+ "FROM bis4.qualifikation a, bis4.snapshot_quali_stammberuf b  "
				+ "where a.bezeichnung=b.quali_bezeichnung and quali_typ='Qualifikation'  and not a.deleted "
				+ ")";
				System.out.println("\t- occupationcompetence_adv (Qualifikation):\t" + stmt.executeUpdate(query));
		}
		
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("A")) {
			System.out.println("Extracting Lehrberufe data: ");

			query = "insert into bissimple3.interestarea ("
				+ "SELECT concat('F',noteid) code, bezeichnung name FROM bis4.themengebiet where not deleted)";
			System.out.println("\t- Themengebiet data: " + stmt.executeUpdate(query));

			query = "insert into bissimple3.apprenticeship ("
				+ "SELECT concat('A',noteid) code, bezeichnung name FROM bis4.lehrberuf "
				//+ "where not deleted AND lehrberuf_nachfolger_noteid is not null)"
				+ "where not deleted)";
			System.out.println("\t- Lehrberuf data: " + stmt.executeUpdate(query));
			
 			query = "insert into bissimple3.apprenticeshipInterestarea ("
				+ "SELECT concat('F',themengebiet_noteid) interestArea, concat('A',lehrberuf_noteid) apprenticeship "
				+ "FROM bis4.lehrberuf_themengebiet where concat('A',lehrberuf_noteid) in (select code from bissimple3.apprenticeship))";
			System.out.println("\t- lehrberuf-themengebiet links: " + stmt.executeUpdate(query));
			
 			query = "insert into bissimple3.apprenticeship_field ("
				+ "SELECT concat('F',berufsfelder_noteid) berufsfelder, concat('A',lehrberuf_noteid) apprenticeship "
				+ "FROM bis4.lehrberuf_berufsfelder where concat('A',lehrberuf_noteid) in (select code from bissimple3.apprenticeship))";
			System.out.println("\t- lehrberuf-berufsfelder links: " + stmt.executeUpdate(query));
			
						
			query = "insert into bissimple3.occupationapprenticeship ("
				+ "SELECT concat('A',noteid) apprenticeship, concat('O',stammdaten_noteid) occupation FROM bis4.lehrberuf "
				+ "where not deleted "
				+ "and stammdaten_noteid is not null and concat('O', stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-lehrberuf (bis level3): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationapprenticeship ("
				+ "SELECT concat('A',noteid) apprenticeship, concat('J',spezielle_noteid) occupation FROM bis4.lehrberuf "
				+ "where not deleted "
				+ "and spezielle_noteid is not null and concat('J', spezielle_noteid) in (select code from bissimple3.occupation)) ";
			System.out.println("\t- Berufsgruppen-lehrberuf (bis level4): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.apprenticeshipCompetence ("
				+ "SELECT concat('A',a.lehrberuf_noteid) apprenticeship, concat('C',b.thesid) competence, 'Mandatory' competenceType, zertifikat "
				+ "FROM bis4.lehrberuf_qualifikation_detail a, bis4.qualifikation_detail b "
				+ "where a.qualifikation_detail_noteid=b.noteid and not b.deleted "
				+ "and concat('A',a.lehrberuf_noteid) in (select code from bissimple3.apprenticeship) "
				+ "and concat('C',b.thesid) in (select code from bissimple3.competence))";
			System.out.println("\t- Lehrberuf-qualifikation_detail (links):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.apprenticeshipCompetence ("
				+ "SELECT concat('A',a.lehrberuf_noteid) apprenticeship, concat('P',b.thesid) competence, 'Optional' competenceType, zertifikat "
				+ "FROM bis4.lehrberuf_qualifikation a, bis4.qualifikation b "
				+ "where a.qualifikation_noteid=b.noteid and not b.deleted "
				+ "and concat('A',a.lehrberuf_noteid) in (select code from bissimple3.apprenticeship)    "            
				+ ")";
			System.out.println("\t- Lehrberuf-qualifikation (links):\t" + stmt.executeUpdate(query));
			
		}
		
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("E")) {
			System.out.println("Extracting Education data: ");

/*			query = "insert into bissimple3.education ("
				+ "SELECT concat('L',reihung) code, name, 'parent' category "
				+ "FROM bis4.qualifikationsniveau where not deleted order by reihung)";
			System.out.println("\t- ausbildungskategorie: " + stmt.executeUpdate(query));
*/
			query = "insert into bissimple3.education ("
				+ "SELECT concat('N',noteid) code, bezeichnung name, 'parent' category "
				+ "FROM bis4.ausbildungskategorie_neu where not deleted)";
			System.out.println("\t- ausbildungskategorie: " + stmt.executeUpdate(query));
						
			query = "insert into bissimple3.education ("
				+ "SELECT concat('K',noteid) code, bezeichnung name, concat('N',ausbildungskategorie_noteid) parent "
				+ "FROM bis4.ausbildungsunterkategorie_neu where not deleted)";
			System.out.println("\t- ausbildungskategorie: " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.education ("
				+ "SELECT concat('E',noteid) code, bezeichnung name, concat('K',ausbildungsunterkategorie_noteid) category "
				+ "FROM bis4.ausbildung_neu "
				+ "where not deleted)";
			//+ "where not deleted and concat('K',ausbildungsunterkategorie_noteid) in (select code from bissimple3.education))";
			System.out.println("\t- ausbildung: " + stmt.executeUpdate(query));

			query = "insert into bissimple3.occupationEducation ("
				+ "SELECT concat('E',ausbildung_noteid) education, concat('O',stammdaten_noteid) occupation, ausbildungsflag "
				+ "FROM bis4.stammdaten_ausbildung_neu "
				+ "where not deleted "
				+ "and concat('E',ausbildung_noteid) in (select code from bissimple3.education) "
				+ "and concat('O',stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-ausbildung (links): " + stmt.executeUpdate(query));
			
			// delete education links to non existing educations (?? records) 
			//query = "delete FROM bissimple3.occupationeducation where education not in (select code from bissimple3.education)";
			//System.out.println("removing education links to non existing educations in the DB:\t" + stmt.executeUpdate(query));
			
			// delete education links to non existing occupations (?? records) 
			//query = "delete FROM bissimple3.occupationeducation where occupation not in (select code from bissimple3.occupation)";
			//System.out.println("removing education links to non existing occupations in the DB:\t" + stmt.executeUpdate(query));
						
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("WE")) {
			System.out.println("Extracting working conditions data: ");
			query = "insert into bissimple3.working_env ("
				+ "SELECT concat('WK',noteid) code, bezeichnung name, 'parent' category FROM bis4.Arbeitsumfeldkategorien where not deleted)";
			System.out.println("\t- Arbeitsumfeldkategorien: " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.working_env ("
				+ "SELECT concat('WE',noteid) code, bezeichnung name, concat('WK',Arbeitsumfeldkategorien_noteid) category "
				+ "FROM bis4.Arbeitsumfeld "
				+ "where not deleted)";
			System.out.println("\t- Arbeitsumfeld: " + stmt.executeUpdate(query));

			query = "insert into bissimple3.occupationworking_env ("
				+ "SELECT concat('O',stammdaten_noteid) occupation, concat('WE',arbeitsumfeld_noteid) working_env FROM bis4.stammdaten_arbeitsumfeld where deleted=0 "
				+ "and concat('WE',arbeitsumfeld_noteid) in (select code from bissimple3.working_env) and concat('O',stammdaten_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-Arbeitsumfeld (links): " + stmt.executeUpdate(query));

					
		}

		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("OL")) {
			//Occupation Education
			//Occupation Apprenticeship
									
			System.out.println("extrating additional data links:");

			query = "insert into bissimple3.occupationCompetence ("
				+ "SELECT concat('O',a.stammdaten_noteid) occupation, concat('C',b.thesid) competence, 'Essential' competenceType, zertifikat, null, null "
				+ "FROM bis4.stammdaten_qualifikation_detail_basis a, bis4.qualifikation_detail b "
				+ "where a.qualifikation_detail_noteid=b.noteid and not b.deleted "
				+ "and concat('O',a.stammdaten_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('C',b.thesid) in (select code from bissimple3.competence))";
			System.out.println("\t- Berufsgruppen-qualifikation (qualifikation_detail_basis):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationCompetence ("
				+ "SELECT concat('O',a.stammdaten_noteid) occupation, concat('P',b.thesid) competence, 'Optional' competenceType, zertifikat, null, null "
				+ "FROM bis4.stammdaten_qualifikation a, bis4.qualifikation b "
				+ "where a.qualifikation_noteid=b.noteid and not b.deleted and webstatus_noteid=2 "
				+ "and concat('O',a.stammdaten_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('P',b.thesid) in (select code from bissimple3.competence))";
			System.out.println("\t- Berufsgruppen-qualifikation (qualifikation):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationCompetence ("
				+ "SELECT concat('O',a.stammdaten_noteid) occupation, concat('P',b.thesid) competence, 'Essential' competenceType, zertifikat, null, null "
				+ "FROM bis4.stammdaten_qualifikation_basis a, bis4.qualifikation b "
				+ "where a.qualifikation_noteid=b.noteid and not b.deleted and webstatus_noteid=2 "
				+ "and concat('O',a.stammdaten_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('P',b.thesid) in (select code from bissimple3.competence))";
			System.out.println("\t- Berufsgruppen-qualifikation (qualifikation_basis):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationCompetence ("
				+ "SELECT concat('O',a.stammdaten_noteid) occupation, concat('C',b.thesid) competence, 'Optional' competenceType, zertifikat, null, null "
				+ "FROM bis4.stammdaten_qualifikation_detail a, bis4.qualifikation_detail b "
				+ "where a.qualifikation_detail_noteid=b.noteid and not b.deleted "
				+ "and concat('O',a.stammdaten_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('C',b.thesid) in (select code from bissimple3.competence))";
			System.out.println("\t- Berufsgruppen-qualifikation (qualifikation_detail):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationCompetence ("
				+ "SELECT concat('J',a.spezielle_noteid) occupation, concat('P',b.thesid) competence, 'Mandatory' competenceType, zertifikat, null, null "
				+ "FROM bis4.qualifikation_spezielle a, bis4.qualifikation b "
				+ "where a.qualifikation_noteid=b.noteid and not b.deleted and webstatus_noteid=2 "
				+ "and concat('J',a.spezielle_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('P',b.thesid) in (select code from bissimple3.competence))";
			System.out.println("\t- Berufsgruppen-qualifikation (qualifikation_spezielle):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationCompetence ("
				+ "SELECT concat('J',a.spezielle_noteid) occupation, concat('C',b.thesid) competence, 'Mandatory' competenceType, zertifikat, null, null "
				+ "FROM bis4.qualifikation_detail_spezielle a, bis4.qualifikation_detail b "
				+ "where a.qualifikation_detail_noteid=b.noteid and not b.deleted "
				+ "and concat('J',a.spezielle_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('C',b.thesid) in (select code from bissimple3.competence))";
			System.out.println("\t- Berufsgruppen-qualifikation (qualifikation_detail_spezielle):\t" + stmt.executeUpdate(query));

			//Occupation Affinity
			query = "insert into bissimple3.occupationAffinity ("
				+ "SELECT concat('O',stammdaten_noteid) occupation_A, concat('O',stammdaten_verwandt_noteid) occupation_B, gewicht, gegengewicht "
				+ "FROM bis4.stammdaten_verwandt "
				+ "where concat('O',stammdaten_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('O',stammdaten_verwandt_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-Affinity (bis level3):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationAffinity ("
				+ "SELECT concat('J',spezielle_noteid) occupation_A, concat('J',spezielle_verwandt_noteid) occupation_B, gewicht, gegengewicht "
				+ "FROM bis4.spezielle_verwandt "
				+ "where concat('J',spezielle_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('J',spezielle_verwandt_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-Affinity (bis level4):\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationAffinity ("
				+ "SELECT concat('O',stammdaten_noteid) occupation_A, concat('J',spezielle_verwandt_noteid) occupation_B, gewicht, gegengewicht "
				+ "FROM bis4.stammdaten_spezielle_verwandt "
				+ "where concat('O',stammdaten_noteid) in (select code from bissimple3.occupation) "
				+ "and concat('J',spezielle_verwandt_noteid) in (select code from bissimple3.occupation))";
			System.out.println("\t- Berufsgruppen-Affinity (bis level3/4):\t" + stmt.executeUpdate(query));

			//Competence Affinity
			query = "insert into bissimple3.competenceAffinity ("
				+ "SELECT concat('P', b.thesid) Competence_A, concat('P', c.thesid) Competence_B, a.gewicht, a.gegengewicht "
				+ "FROM bis4.qualifikation_verwandt a, bis4.qualifikation b, bis4.qualifikation c "
				+ "where a.qualifikation_noteid = b.noteid and a.qualifikation_verwandt_noteid = c.noteid and not a.deleted)";
			System.out.println("\t- competenceAffinity data:\t" + stmt.executeUpdate(query));
			
  			query = "insert into bissimple3.competenceAffinity (SELECT concat('C', b.thesid) Competence_A, concat('C', c.thesid) Competence_B, a.gewicht, a.gegengewicht "
				+ "FROM bis4.qualifikation_detail_verwandt a, bis4.qualifikation_detail b, bis4.qualifikation_detail c "
				+ "where a.qualifikation_detail_noteid = b.noteid and a.qualifikation_detail_verwandt_noteid = c.noteid  and not a.deleted )";
			System.out.println("inserting competenceAffinity data:\t" + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceAffinity (SELECT concat('P', b.thesid) Competence_A, concat('C', c.thesid) Competence_B, a.gewicht, a.gegengewicht "
				+ "FROM bis4.qualifikation_verwandt_detail a, bis4.qualifikation b, bis4.qualifikation_detail c "
				+ "where a.qualifikation_noteid = b.noteid and a.qualifikation_detail_verwandt_noteid = c.noteid and not a.deleted)"; 
			System.out.println("inserting competenceAffinity data:\t" + stmt.executeUpdate(query));

			// delete inconsistent competence affinities links
			query = "Delete FROM bissimple3.competenceAffinity where competence_a not in (select code from bissimple.competence)";
			System.out.println("removing inconsistent competence affinities A:\t" + stmt.executeUpdate(query));
			query = "Delete FROM bissimple3.competenceAffinity where competence_b not in (select code from bissimple.competence)";
			System.out.println("removing inconsistent competence affinities B:\t" + stmt.executeUpdate(query));

			// delete inconsistent occupation-competence links
			query = "Delete FROM bissimple3.occupationcompetence where occupation not in (select code from bissimple3.occupation) "
				+ "or competence not in (select code from bissimple3.competence)";
			System.out.println("removing inconsistent occupation-competence links:\t" + stmt.executeUpdate(query));

			// delete ComptenceLinks to occupations that are not under isco code 522 (14295 records) 
			//query = "Delete FROM bissimple3.occupationcompetence where occupation not in (select code from bissimple3.occupation where (parent like '522%' or code like 'J%'))";
			//System.out.println("removing occupationCompetence which do not have occupations under isco code 522:\t" + stmt.executeUpdate(query));

			//query = "Delete FROM bissimple3.competence where (_level>5 and code not in (select competence from bissimple3.occupationcompetence))";
			//System.out.println("removing competence which do not have a link to occupations:\t" + stmt.executeUpdate(query));
		
	
		}
		
		// Hierarchy-based occupation affinities (auto)
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("occupationAffinities")) {
			query = "Delete From bissimple3.occupationaffinity_h";
			System.out.println("removing occupationAffinity_H data:\t" + stmt.executeUpdate(query));
			
			System.out.println("Extracting Occupation Affinities: ");
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.occupation, b.occupation, 50 Affinity, 'AR1: Same Berufsobergruppe' Remark "
				+ "from bissimple3.occupation_field a, bissimple3.occupation_field b "
				+ "where a.field = b.field "
				+ "and a.occupation!=b.occupation)";
			System.out.println("\t- AR1 affinities (Same Berufsobergruppe): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.code, a.parent, 75 Affinity, 'AR2: Same Berufsuntergruppe' Remark from bissimple3.occupation a "
				+ "where parent is not null)";
			System.out.println("\t- AR2 affinities (Same Berufsuntergruppe): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.parent, a.code, 75 Affinity, 'AR2: Same Berufsuntergruppe' Remark from bissimple3.occupation a "
				+ "where parent is not null)";
			System.out.println("\t- AR2 affinities (Same Berufsuntergruppe): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.code, b.code, 90 Affinity, 'AR3: Bushtabe 90' Remark "
				+ "from bissimple3.occupation a, bissimple3.occupation b where a.parent = b.parent "
				+ "and a.category = b.category and a.code!=b.code)";
			System.out.println("\t- AR3 affinities (Bushtabe 90%): " + stmt.executeUpdate(query));
	
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.code, b.code, 90 Affinity, 'AR4: Bushtabe 75' Remark "
				+ "from bissimple3.occupation a, bissimple3.occupation b where a.parent = b.parent "
				+ "and a.category != b.category and a.code!=b.code)";
			System.out.println("\t- AR4 affinities (Bushtabe 75%): " + stmt.executeUpdate(query));
						
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.code, b.code, 50 Affinity, 'AR5: inherited 50' Remark "
				+ "from bissimple3.occupation a, bissimple3.occupation b, bissimple3.occupation_field c, bissimple3.occupation_field d "
				+ "where a.parent = c.occupation and b.parent = d.occupation "
				+ "and c.field=d.field)";
			System.out.println("\t- AR5 affinities (inherited 50%): " + stmt.executeUpdate(query));
						
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.code, b.code, 50 Affinity, 'Rule6: Same Berufsobergruppe' Remark "
				+ "from bissimple3.occupation a, bissimple3.occupation b, bissimple3.occupation_field c, bissimple3.occupation_field d "
				+ "where c.field = d.field and a.code=c.occupation and b.parent=d.occupation and a.code!=b.parent)";
			System.out.println("\t- AR6 affinities (inherited 50%): " + stmt.executeUpdate(query));
						
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select b.code, a.code, 50 Affinity, 'Rule6: Same Berufsobergruppe' Remark "
				+ "from bissimple3.occupation a, bissimple3.occupation b, bissimple3.occupation_field c, bissimple3.occupation_field d "
				+ "where c.field = d.field and a.code=c.occupation and b.parent=d.occupation and a.code!=b.parent)";
			System.out.println("\t- AR6 affinities (inherited 50%): " + stmt.executeUpdate(query));
						
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select a.code, b.code, c.gewicht Affinity, 'Rule7: inherited manual affinities' Remark "
				+ "FROM bissimple3.occupation a, bissimple3.occupation b, bis4.stammdaten_verwandt c "
				+ "where a.parent = concat('O',c.stammdaten_noteid) and b.parent = concat('O',c.stammdaten_verwandt_noteid))";
			System.out.println("\t- MR7 inherited manual affinities: " + stmt.executeUpdate(query));
						
			query = "insert into bissimple3.occupationaffinity_h ("
				+ "select b.code, a.code, c.gegengewicht Affinity, 'Rule7: inherited manual affinities' Remark "
				+ "FROM bissimple3.occupation a, bissimple3.occupation b, bis4.stammdaten_verwandt c "
				+ "where a.parent = concat('O',c.stammdaten_noteid) and b.parent = concat('O',c.stammdaten_verwandt_noteid))";
			System.out.println("\t- MR7 inherited manual affinities: " + stmt.executeUpdate(query));
						
		}

		// Hierarchy-based competence affinities (auto)
		if (item.equalsIgnoreCase("all") || item.equalsIgnoreCase("competenceAffinities")) {
			query = "Delete From bissimple3.competenceaffinity_h";
			System.out.println("removing competenceAffinity_H data:\t" + stmt.executeUpdate(query));
			
			//A
			System.out.println("Extracting Competence Affinities: ");
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 100 affinity, 'level9 - level8' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent WHERE a._level=9)";
			System.out.println("\t- Competence affinities (100%: level9 - level8): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 100 affinity, 'level9 - level7' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "WHERE a._level=9)";
			System.out.println("\t- Competence affinities (100%: level9 - level7): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 100 affinity, 'level9 - level6' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "left outer join bissimple3.competence d on d.code=c.parent "
				+ "WHERE a._level=9)";
			System.out.println("\t- Competence affinities (100%: level9 - level6): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 100 affinity, 'level8 - level7' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent WHERE a._level=8)";
			System.out.println("\t- Competence affinities (100%: level8 - level7): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 100 affinity, 'level8 - level6' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "WHERE a._level=8)";
			System.out.println("\t- Competence affinities (100%: level8 - level6): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 80 affinity, 'level8 - level5' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "left outer join bissimple3.competence d on d.code=c.parent "
				+ "WHERE a._level=8)";
			System.out.println("\t- Competence affinities (80%: level8 - level5): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 100 affinity, 'level7 - level6' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent WHERE a._level=7)";
			System.out.println("\t- Competence affinities (100%: level7 - level6): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 80 affinity, 'level7 - level5' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "WHERE a._level=7)";
			System.out.println("\t- Competence affinities (80%: level7 - level5): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 60 affinity, 'level7 - level4' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "left outer join bissimple3.competence d on d.code=c.parent "
				+ "WHERE a._level=7)";
			System.out.println("\t- Competence affinities (60%: level7 - level4): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 80 affinity, 'level6 - level5' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent WHERE a._level=6)";
			System.out.println("\t- Competence affinities (80%: level6 - level5): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 60 affinity, 'level6 - level4' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "WHERE a._level=6)";
			System.out.println("\t- Competence affinities (60%: level6 - level4): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 40 affinity, 'level6 - level3' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "left outer join bissimple3.competence d on d.code=c.parent "
				+ "WHERE a._level=6)";
			System.out.println("\t- Competence affinities (40%: level6 - level3): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 60 affinity, 'level5 - level4' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent WHERE a._level=5)";
			System.out.println("\t- Competence affinities (60%: level5 - level4): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 40 affinity, 'level5 - level3' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent "
				+ "left outer join bissimple3.competence c on c.code=b.parent "
				+ "WHERE a._level=5)";
			System.out.println("\t- Competence affinities (40%: level5 - level3): " + stmt.executeUpdate(query));
			
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 40 affinity, 'level4 - level3' remark FROM bissimple3.competence a "
				+ "left outer join bissimple3.competence b on b.code=a.parent WHERE a._level=4)";
			System.out.println("\t- Competence affinities (40%: level4 - level3): " + stmt.executeUpdate(query));
			
			//B
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 60 affinity, 'B1: sibling affinities under level 3' remark "
				+ "FROM bissimple3.competence a, bissimple3.competence b where b.parent=a.parent and a._level=4)";
			System.out.println("\t- Competence affinities (sibling under level 4): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 80 affinity, 'B2: sibling affinities under level 4' remark "
				+ "FROM bissimple3.competence a, bissimple3.competence b where b.parent=a.parent and a._level=5)";
			System.out.println("\t- Competence affinities (sibling under level 5): " + stmt.executeUpdate(query));
			
			query = "insert into bissimple3.competenceaffinity_h ("
				+ "SELECT a.code comp_A, b.code comp_B, 100 affinity, 'B3: sibling affinities under levels 5-9' remark "
				+ "FROM bissimple3.competence a, bissimple3.competence b where b.parent=a.parent and a._level>5)";
			System.out.println("\t- Competence affinities (sibling under level 6-9): " + stmt.executeUpdate(query));
			
						
		}

		stmt.close();
		conn.close();
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