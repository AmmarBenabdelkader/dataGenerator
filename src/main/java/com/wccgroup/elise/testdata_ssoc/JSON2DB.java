/**
 * @author abenabdelkader
 *
 * taxonomy.java
 * Nov 8, 2016
 */
package com.wccgroup.elise.testdata_ssoc;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.logging.*;

import java.sql.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.*;
import java.util.Date;
/**
 * @author abenabdelkader
 *
 */


public class JSON2DB {
	static String JDBC_DRIVER = "";
	static String DB_URL = "";
	static String USER = "";
	static String PASS = "";

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

		Date date = new Date();
		JDBC_DRIVER = "com.mysql.jdbc.Driver";
		DB_URL = "jdbc:mysql://localhost/ssoc2?useUnicode=true&characterEncoding=utf-8";
		USER = "root";
		PASS = "";
		

		System.out.println("\t*** JSON to Database converter - ****");

		//processJSON2DB("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\taxonomy\\SSOC2\\data\\TM-to-Actonomy2.json");
		
		genJobTitles();
		//matchJobTitles();
		//generateCVSfromDB(path);
		//processOccupationalInterests(path);
		//ActonomyReader("Financial%20Planning%20Manager");
		System.out.println("\nTotal duration: " + (new Date().getTime() - date.getTime())/1000 + "s");



	}

	public static void genJobTitles() throws SQLException, IOException
	{
		System.out.println("\nGenerating data from DB");
		try
		{
			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			//CV Job Titles
			//String query = "select distinct * from ssoc_temp.jobtitles order by jobtitle asc;";  
			//unmapped CV Job Titles
			//String query = "select distinct * from ssoc_temp.jobtitles where jobtitle not in (SELECT distinct cv_jobtitle FROM ssoc_temp.ssoc_actonomy_occupation) order by jobtitle asc;";  
			//String query = "SELECT distinct * FROM ssoc_temp.actonomy_terms where parent is not null order by name";
			//String query = "SELECT distinct a.cv_jobtitle, a.actonomy_code, a.score, b.name FROM ssoc_temp.ssoc_actonomy_occupation a, ssoc_temp.actonomy_terms b where a.actonomy_code=b.code order by cv_jobtitle";
			//String query = "SELECT distinct * FROM ssoc_temp.function_groups";
			String query = "SELECT distinct * FROM ssoc_temp.actonomy_synonyms order by actonomy_term;";
			ResultSet results = stmt.executeQuery(query);
			while (results.next())
			{
				//System.out.println(results.getString(1) + "\t" + results.getString(2) + "\t" + results.getString(4) + "\t" + results.getString(5));
				//System.out.println(results.getString(1) + "\t" + results.getString(2) + "\t" + results.getString(4) + "\t" + results.getString(5));
				//System.out.println(results.getString(1) + "\t" + results.getString(2) + "\t" + "\t" + results.getString(3));
				//System.out.println(results.getString(1) + "\t" + results.getString(2) + "\t" + results.getString(3) + "\t" + results.getString(4));
				//System.out.println(results.getString(1) + "\t" + results.getString(2));
				System.out.println(results.getString(1) + "\t" + results.getString(2));
			
			}


			results.close();
			conn.close();
			stmt.close();
			stmt2.close();

		}
		catch (ClassNotFoundException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	public static void matchJobTitles() throws SQLException, IOException
	{
		System.out.println("\nGenerating CSV data for TM");
		try
		{
			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			String query = "select id, jobtitle from ssoc_temp.jobtitles where ssoc_code is null";
			ResultSet results = stmt.executeQuery(query);
			int i = 0;
/*			while (results.next())
			{
				query = "select code, name from ssoc.occupation";
				ResultSet rs = stmt2.executeQuery(query);
				//System.out.println(results.getString(1) + ": " + results.getString(2));
				while (rs.next()) {
					if (results.getString(2).contains(rs.getString(2))) {
						System.out.println(results.getString(1) + ": " + results.getString(2) + ": " + rs.getString(2));
						stmt3.executeUpdate("update ssoc_temp.jobtitles set ssoc_code='" + rs.getString(1) + "' where id=" + results.getString(1));
						i++;
					}
				}
			rs.close();
			}
			stmt3.executeUpdate("update ssoc_temp.jobtitles set matchtype='partial A' where ssoc_code is not null and matchtype is null");

*/			
			query = "select id, jobtitle from ssoc_temp.jobtitles where ssoc_code is null";
			results = stmt.executeQuery(query);
			i = 0;
			while (results.next())
			{
				query = "select code, name from ssoc.occupation";
				ResultSet rs = stmt2.executeQuery(query);
				//System.out.println(results.getString(1) + ": " + results.getString(2));
				while (rs.next()) {
					if (rs.getString(2).contains(results.getString(2))) {
						System.out.println(results.getString(1) + ": " + results.getString(2) + ": " + rs.getString(2));
						stmt3.executeUpdate("update ssoc_temp.jobtitles set ssoc_code='" + rs.getString(1) + "' where id=" + results.getString(1));
						i++;
					}
				}
			rs.close();
			}
			stmt3.executeUpdate("update ssoc_temp.jobtitles set matchtype='partial B' where ssoc_code is not null and matchtype is null");

			System.out.println("\t- " + i + " jobtitles are matched");

			results.close();
			conn.close();
			stmt.close();
			stmt2.close();
			results.close();

		}
		catch (ClassNotFoundException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	public static void generateCVSfromDB(String path) throws SQLException, IOException
	{
		System.out.println("\nGenerating CSV data for TM");
		try
		{
			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			String query = "select distinct code,name from ssoc2.actonomy_terms order by parent desc, name asc";
			ResultSet results = stmt.executeQuery(query);
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path+"actonomy_terms.csv")));
			writer.write("code\tname");
			int i = 0;
			while (results.next())
			{
				writer.write("\n" + results.getString(1) + "\t" + results.getString(2));
				i++;
			}
			writer.close();
			System.out.println("\t- " + i + " actonomy_terms");

			query = "select distinct parent, code from ssoc2.actonomy_terms where parent is not null order by parent desc, name asc";
			results = stmt.executeQuery(query);
			writer = new BufferedWriter(new FileWriter(new File(path+"actonomy_terms-hierarchy.csv")));
			i = 0;
			while (results.next())
			{
				if (i>0)
					writer.write("\n");
				
				writer.write(results.getString(1) + "\t" + results.getString(2));
				i++;
			}
			writer.close();


			query = "SELECT distinct * FROM ssoc2.ssoc_actonomy_occupation where actonomy_code not like '%C#master%' and actonomy_code not like '%O#master%'";
			results = stmt.executeQuery(query);
			writer = new BufferedWriter(new FileWriter(new File(path+"relationsWithScore2.csv")));
			i = 0;
			while (results.next())
			{
				if (i>0)
					writer.write("\n");
				
				writer.write("occupation-actonomy-terms\toccupation\t" + results.getString(1) + "\tactonomy_terms\t" + results.getString(2) + "\t" + results.getString(3));
				i++;
			}

			query = "SELECT distinct left(ssoc_code,4), functiongroup_code, avg(score) FROM ssoc2.ssoc_actonomy_occupation a, ssoc2.function_groups b where a.actonomy_code=b.function_code group by functiongroup_code";
			results = stmt.executeQuery(query);
			while (results.next())
			{
				if (i>0)
					writer.write("\n");
				
				writer.write("occupation-actonomy-terms\toccupation\t" + results.getString(1) + "\tactonomy_terms\t" + results.getString(2) + "\t" + (int) (Float.parseFloat(results.getString(3))));
				i++;
			}
			writer.close();
			System.out.println("\t- " + i + " occupation_actonomy_terms");

			query = "SELECT distinct * FROM ssoc2.actonomy_synonyms where actonomy_term not like '%C#master%' and actonomy_term not like '%O#master%'";
			results = stmt.executeQuery(query);
			writer = new BufferedWriter(new FileWriter(new File(path+"actonomy_terms-synonymlabels-labels.csv")));
			writer.write("code\tsynonymlabels");
			i = 0;
			while (results.next())
			{
				writer.write("\n" + results.getString(1) + "\t" + results.getString(2));
				i++;
			}
			writer.close();
			System.out.println("\t- " + i + " actonomy_terms-synonymlabels");

			query = "SELECT distinct * FROM ssoc2.function_groups";
			results = stmt.executeQuery(query);
			writer = new BufferedWriter(new FileWriter(new File(path+"relations2.csv")));
			i = 0;
			while (results.next())
			{
				if (i>0)
					writer.write("\n");
				
				writer.write("actonomy-function-groups\tactonomy_terms\t" + results.getString(1) + "\tactonomy_terms\t" + results.getString(2));
				i++;
			}
			writer.close();
			System.out.println("\t- " + i + " actonomy-function-groups");

			conn.close();
			stmt.close();
			results.close();

		}
		catch (ClassNotFoundException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	public static void processOccupationalInterests(String path) throws SQLException, IOException
	{
		System.out.println("\nprocessOccupationalInterests");
		String r[]={"R","I","A","S","E","C"};
		for ( int j=0; j<6; j++) {
			System.out.print(r[j] + "\t");
			for ( int j2=j+1; j<6; j++) {
				System.out.print("\t" + r[j] + r[j2]);
				for ( int j3=j2+1; j<6; j++) {
					System.out.println("\t" + r[j] + r[j2] + r[j3]);
					System.out.print(r[j] + "\t");
					System.out.print("\t" + r[j] + r[j2]);
					for (int j4=1; j4<4; j4++) {
					//System.out.println(r[j] + "\t" + "\t" + r[j] + r[j2] + "\t" + r[j] + r[j2] + r[j4]);
					//System.out.println(r[j] + "\t" + "\t" + r[j] + r[j2] + "\t" + r[j] + r[j2] + r[j4]);
					//System.out.println(r[j] + "\t" + "\t" + r[j] + r[j2] + "\t" + r[j] + r[j2] + r[j3+2]);
					//System.out.println(r[j] + "\t" + "\t" + r[j] + r[j2] + "\t" + r[j] + r[j2] + r[j3+3]);
					//System.out.println(r[j] + "\t" + "\t" + r[j] + r[j2] + "\t" + r[j] + r[j2] + r[j3+4]);
					}
				}
			}
		}
		if (1==1)
			return;
				
		try
		{
			Class.forName(JDBC_DRIVER);
			Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			String query = "SELECT c.ssoc_code, element_name, AVG(CONVERT(data_value*10,UNSIGNED INTEGER)) score FROM onet.interests a, onet.content_model_reference b, ssoc2.ssoc2015_onet c where a.element_id=b.element_id and a.onetsoc_code=c.onet_code and length(c.ssoc_code)=4 and scale_id='OI' group by c.ssoc_code,element_name order by c.ssoc_code asc, score desc, element_name asc";
			ResultSet results = stmt.executeQuery(query);
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path+"relationsWithScore3.csv")));
			int i = 0;
			String occupation = "";
			String interest = "";
			int l=1;
			int score=0;
			while (results.next())
			{
				if (!occupation.equalsIgnoreCase(results.getString(1))) {
					writer.write("\noccupational-interest-groups\toccupation\t" + occupation + "\tinterest_groups\t" + interest + "\t" + score/6);
					occupation = results.getString(1);
					interest = "";
					score=0;
					l=1;
				}
				if (l<=3) {
					interest += results.getString(2).substring(0, 1);
					int temp = (int) Float.parseFloat(results.getString("score").substring(0, 2));
					if (l==1)
						score = score + temp*3;
					else
						if (l==2)
							score = score + temp*2;
						else
							if (l==3)
								score = score + temp;
					//System.out.println(results.getString("score") + "\t" + temp + "\t" + score);
							
						
				}
				l++;
				i++;
			}
			writer.close();
			System.out.println("\t- " + i + " actonomy_terms\t- " + path+"relationsWithScore3.csv");


			conn.close();
			stmt.close();
			results.close();

		}
		catch (ClassNotFoundException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	/* Reads the data from Json and convert it to mySQL database*/
	public static void ActonomyReader(String file) throws SQLException, ClassNotFoundException
	{
		Class.forName(JDBC_DRIVER);
		Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		Statement stmt = conn.createStatement();
		HashMap<String,String> fGroups=new HashMap<String,String>(); 
		HashMap<String,String> functions=new HashMap<String,String>(); 
		HashMap<String,String> competences=new HashMap<String,String>(); 
		HashMap<String,String> functionLabels=new HashMap<String,String>(); 
		HashMap<String,String> functionGroups=new HashMap<String,String>(); 
			StringBuilder links = new StringBuilder();
			StringBuilder actonomySynonyms = new StringBuilder();
			StringBuilder function_Groups = new StringBuilder();
			function_Groups.append("insert into ssoc_temp.function_groups values ");
			actonomySynonyms.append("insert into ssoc_temp.actonomy_synonyms values ");
			links.append("insert into ssoc_temp.ssoc_actonomy_occupation values ");
		ResultSet rs = stmt.executeQuery("SELECT id, jobtitle FROM ssoc2.jobtitles where length(jobtitle)>3");
		int counter = 0;
		int j = 0;
		int start=5000;
		int end = 9000;
		while (rs.next() && counter<=end) {
			while (counter<start) {
				rs.next();
				counter++;
			}
			if (rs.getString("jobtitle").trim().length()<5)
				continue;
			
			file = "http://demos.savannah.wcc.nl:14080/terms/"+ rs.getString("jobtitle").replaceAll(" ", "%20").replaceAll("/", "%20").trim() + "?categories=FUNCTION";
			System.out.println(counter + ": " + rs.getString("jobtitle") + (" (id: " + rs.getString("id")));
			StringBuilder stringBuilder = new StringBuilder();
			try
			{
			URL url = new URL(file);
			URLConnection yc = url.openConnection();
			BufferedReader in;
				in = new BufferedReader(new InputStreamReader(
					yc.getInputStream()));
			String line;
			stringBuilder.append("{\n\"nodes\":\n");
			while ((line = in.readLine()) != null) {
				stringBuilder.append(line + '\n');
			}
			stringBuilder.append("\n}");
			in.close();
			}
			catch (IOException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			//System.out.println(stringBuilder);
			/*		if (1==1)
			return;
			 */		try {
				 JSONParser parser = new JSONParser();
				 Object obj;
				 obj = parser.parse(stringBuilder.toString());
				 JSONObject jsonObject = (JSONObject) obj;


				 JSONArray nodes = (JSONArray) jsonObject.get("nodes");
				 JSONObject object1;
				 //System.out.println("\nA- Creating right part");
				 if (nodes==null) 
					 return;
				 for (int i = 0; i < nodes.size(); i++) {
					 object1 = (JSONObject)nodes.get(i);
					 if(object1.get("category").toString().equalsIgnoreCase("FUNCTION") || object1.get("category").toString().equalsIgnoreCase("ABSTRACT_FUNCTION")) {
						 functions.put(object1.get("id").toString(), object1.get("label").toString());
						 links.append("('" + rs.getString("jobtitle") + "', '" + object1.get("id") + "', " + (int) (Float.parseFloat(object1.get("score").toString())*100) + "),");
					 }
					 /*				 else {
					 if (object1.get("category").toString().equalsIgnoreCase("COMPETENCE")) {
						 competences.put(object1.get("id").toString(), object1.get("label").toString() );
						 links.append("('" + object1.get("code") + "', '" + object1.get("id") + "', " + (int) (Float.parseFloat(object1.get("score").toString())*100) + "),");
					 }
					 else {
						 others.append(object1.get("id") + "\t" + object1.get("label") + "\n");
						 links.append("('" + object1.get("code") + "', '" + object1.get("id") + "', " + (int) (Float.parseFloat(object1.get("score").toString())*100) + "),");
					 }
				 }
					  */
					 JSONArray groups = (JSONArray) object1.get("groups");
					 if (groups==null) 
						 return;
					 for (j = 0; j < groups.size(); j++) {
						 //i++;
						 JSONObject group = (JSONObject)groups.get(j);
						 //System.out.print("\t" + group.get("id") + "#" + group.get("keyword") + "#" + group.get("category"));
						 if(group.get("category").toString().equalsIgnoreCase("DOMAIN") && object1.get("category").toString().equalsIgnoreCase("FUNCTION")) 
							 functionLabels.put(object1.get("id").toString(), group.get("keyword").toString() );
						 //else 
						 //labels.append("function-other-labels\t" + right.get("id") + "\t" + group.get("keyword") + "\n");
						 if(group.get("category").toString().equalsIgnoreCase("FUNCTION_GROUP") && object1.get("category").toString().equalsIgnoreCase("FUNCTION")) {
							 fGroups.put(group.get("id").toString(), group.get("keyword").toString());
							 functionGroups.put(object1.get("id").toString() + "_#_" + group.get("id").toString(), group.get("keyword").toString());
							 function_Groups.append("('" + object1.get("id")+ "','" +  group.get("id") + "'),");
						 }
					 }

					 JSONObject labelObjs = (JSONObject) object1.get("labelsSynonyms");
					 JSONArray synonyms = (JSONArray) labelObjs.get("ENG");
					 if (synonyms!=null) {
						 //System.out.println("Size of lables for '" + right.get("id") + "': " + synonyms.size());
						 for (int l = 0; l < synonyms.size(); l++) {
							 synonyms.get(l);
							 //functionLabels.put(right.get("id").toString(), synonyms.get(l).toString() );
							 actonomySynonyms.append("('" + object1.get("id") + "',\"" + synonyms.get(l).toString() + "\"),");
						 }
					 }
				 }
			 }
			 catch (ParseException e)
			 {
				 e.printStackTrace();
			 }
			 counter++;
		}

				 //System.out.println("Deleting Actonomy Terms: " + stmt.executeUpdate("delete from ssoc_temp.actonomy_terms") + " Data objects deleted");

				 //Actonomy Function Groups
				 StringBuilder query = new StringBuilder();
				 query.append("insert into ssoc_temp.actonomy_terms (code, name, parent) values ");
				 query.append("('FunctionGroup','Function Groups', null)");
				 for(Map.Entry group:fGroups.entrySet()) {  
					 query.append(",('" + group.getKey() + "',\"" + group.getValue() + "\",'FunctionGroup')");
				 }  
				 //System.out.println("\tGenerating Actonomy Terms: " + stmt.executeUpdate(query.toString()) + " Function Groups");
				 System.out.println("\tGenerating Actonomy Terms: \n" + query.toString());

				 //Actonomy Functions
				 query = new StringBuilder();
				 query.append("insert into ssoc_temp.actonomy_terms (code, name, parent) values ");
				 query.append("('Function','Functions', null)");
				 for(Map.Entry function:functions.entrySet()) { 
					 query.append(",('" + function.getKey() + "',\"" + function.getValue() + "\",'Function')");
				 }  
				 //System.out.println("\tGenerating Actonomy Terms: " + stmt.executeUpdate(query.toString()) + " Functions");
				 System.out.println("\tGenerating Actonomy Terms: \n" + query.toString());

				 //Actonomy Competence
				 query = new StringBuilder();
				 query.append("insert into ssoc_temp.actonomy_terms (code, name, parent) values ");
				 query.append("('Competence','Competence',null)");
				 for(Map.Entry competence:competences.entrySet()) {  
					 query.append(",('" + competence.getKey() + "',\"" + competence.getValue() + "\",'Competence')");
				 }  
				 //System.out.println("\tGenerating Actonomy Terms: " + stmt.executeUpdate(query.toString()) + " Competencies");
				 System.out.println("\tGenerating Actonomy Terms: \n" + query.toString());


				 // Links from functions to functionGroups
				 //System.out.println("Deleting Relations: " + stmt.executeUpdate("delete from ssoc2.function_groups") + " function_groups deleted");
				 query = new StringBuilder();
				 query.append("insert into ssoc_temp.function_groups values ");
				 for(Map.Entry functionGroup:functionGroups.entrySet()) {  
					 String key1 = functionGroup.getKey().toString().substring(0, functionGroup.getKey().toString().indexOf("_#_"));
					 String key2 = functionGroup.getKey().toString().substring(functionGroup.getKey().toString().indexOf("_#_")+3,functionGroup.getKey().toString().length());
					 query.append("('" + key1 + "','" + key2 + "'),");
				 }  
				 //System.out.println("\tGenerating Relations: " + stmt.executeUpdate(query.toString().substring(0, query.length()-1)) + " Functions Groups Links");					
				 System.out.println("\tGenerating Relations: \n" + query.toString().substring(0, query.length()-1));					

				 //System.out.println("Deleting Relations: " + stmt.executeUpdate("delete from ssoc_temp.actonomy_synonyms") + " actonomy_synonyms deleted");
				 //System.out.println("\tGenerating Relations: " + stmt.executeUpdate(actonomySynonyms.toString().substring(0, actonomySynonyms.length()-1)) + " Actonomy Synonyms");					
				 System.out.println("\tGenerating Relations: \n" + actonomySynonyms.toString().substring(0, actonomySynonyms.length()-1));					

				 System.out.println(links);
				 //System.out.println("Deleting Relations: " + stmt.executeUpdate("delete from ssoc_temp.ssoc_actonomy_occupation") + " ssoc_actonomy_occupation deleted");
				 System.out.println("\tGenerating Actonomy Synonyms: \n" + links.toString().substring(0, links.length()-1));					
				 //System.out.println("\tGenerating Relations: " + stmt.executeUpdate(links.toString().substring(0, links.length()-1)) + " Actonomy Synonyms");					

			 conn.close();
			 stmt.close();
		

	}

	/* Reads the data from Json and convert it to mySQL database*/
	public static void processJSON2DB(String file) throws SQLException, IOException
	{
		HashMap<String,String> fGroups=new HashMap<String,String>(); 
		HashMap<String,String> functions=new HashMap<String,String>(); 
		HashMap<String,String> competences=new HashMap<String,String>(); 
		HashMap<String,String> sectors=new HashMap<String,String>(); 
		HashMap<String,String> functionLabels=new HashMap<String,String>(); 
		HashMap<String,String> functionGroups=new HashMap<String,String>(); 
		Date date = new Date();
		StringBuilder others = new StringBuilder();
		StringBuilder links = new StringBuilder();
		StringBuilder actonomySynonyms = new StringBuilder();
		StringBuilder function_Groups = new StringBuilder();
		function_Groups.append("insert into function_groups values ");
		actonomySynonyms.append("insert into actonomy_synonyms values ");
		links.append("insert into ssoc_actonomy_occupation values ");
		int j = 0;
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		StringBuilder stringBuilder = new StringBuilder();
		while ((line = br.readLine()) != null) {
			stringBuilder.append(line + '\n');
		}
		br.close();
		try {
			JSONParser parser = new JSONParser();
			Object obj;
			obj = parser.parse(stringBuilder.toString());
			JSONObject jsonObject = (JSONObject) obj;


			JSONArray nodes = (JSONArray) jsonObject.get("nodes");
			JSONObject object1;
			System.out.println("\nA- Creating left part");
			if (nodes==null) 
				return;
			for (int i = 0; i < nodes.size(); i++) {
				object1 = (JSONObject)nodes.get(i);
				JSONObject left = (JSONObject) object1.get("left");
				JSONObject right = (JSONObject) object1.get("right");
				if(right.get("category").toString().equalsIgnoreCase("FUNCTION") || right.get("category").toString().equalsIgnoreCase("ABSTRACT_FUNCTION")) {
					functions.put(right.get("id").toString(), right.get("label").toString());
					links.append("('" + left.get("code") + "', '" + right.get("id") + "', " + (int) (Float.parseFloat(right.get("score").toString())*100) + "),");
				}
				else {
					if (right.get("category").toString().equalsIgnoreCase("COMPETENCE")) {
						competences.put(right.get("id").toString(), right.get("label").toString() );
						links.append("('" + left.get("code") + "', '" + right.get("id") + "', " + (int) (Float.parseFloat(right.get("score").toString())*100) + "),");
					}
					else {
						others.append(right.get("id") + "\t" + right.get("label") + "\n");
						links.append("('" + left.get("code") + "', '" + right.get("id") + "', " + (int) (Float.parseFloat(right.get("score").toString())*100) + "),");
					}
				}

				JSONArray groups = (JSONArray) right.get("groups");
				if (groups==null) 
					return;
				for (j = 0; j < groups.size(); j++) {
					//i++;
					JSONObject group = (JSONObject)groups.get(j);
					//System.out.print("\t" + group.get("id") + "#" + group.get("keyword") + "#" + group.get("category"));
					if(group.get("category").toString().equalsIgnoreCase("DOMAIN") && right.get("category").toString().equalsIgnoreCase("FUNCTION")) 
						functionLabels.put(right.get("id").toString(), group.get("keyword").toString() );
					//else 
					//labels.append("function-other-labels\t" + right.get("id") + "\t" + group.get("keyword") + "\n");
					if(group.get("category").toString().equalsIgnoreCase("FUNCTION_GROUP") && right.get("category").toString().equalsIgnoreCase("FUNCTION")) {
						fGroups.put(group.get("id").toString(), group.get("keyword").toString());
						functionGroups.put(right.get("id").toString() + "_#_" + group.get("id").toString(), group.get("keyword").toString());
						function_Groups.append("('" + right.get("id")+ "','" +  group.get("id") + "'),");
					}
				}

				JSONObject labelObjs = (JSONObject) right.get("labelsSynonyms");
				JSONArray synonyms = (JSONArray) labelObjs.get("ENG");
				if (synonyms!=null) {
					//System.out.println("Size of lables for '" + right.get("id") + "': " + synonyms.size());
					for (int l = 0; l < synonyms.size(); l++) {
						synonyms.get(l);
						//functionLabels.put(right.get("id").toString(), synonyms.get(l).toString() );
						actonomySynonyms.append("('" + right.get("id") + "',\"" + synonyms.get(l).toString() + "\"),");
					}
				}
				/*				iterator1 = labels.iterator();
				JSONObject label;
				while (iterator1.hasNext()) {
					//i++;
					label = iterator1.next();
					//System.out.print("\t" + group.get("id") + "#" + group.get("keyword") + "#" + group.get("category"));
					if(label.get("category").toString().equalsIgnoreCase("DOMAIN") && right.get("category").toString().equalsIgnoreCase("FUNCTION")) 
						functionLabels.put(right.get("id").toString(), label.get("keyword").toString() );
					//else 
						//labels.append("function-other-labels\t" + right.get("id") + "\t" + group.get("keyword") + "\n");
				}
				//System.out.println();
				 */	
			}
			try
			{
				Class.forName(JDBC_DRIVER);
				Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
				Statement stmt = conn.createStatement();
				System.out.println("Deleting Actonomy Terms: " + stmt.executeUpdate("delete from ssoc2.actonomy_terms") + " Data objects deleted");

				//Actonomy Function Groups
				StringBuilder query = new StringBuilder();
				query.append("insert into ssoc2.actonomy_terms (code, name, parent) values ");
				query.append("('FunctionGroup','Function Groups', null)");
				for(Map.Entry group:fGroups.entrySet()) {  
					query.append(",('" + group.getKey() + "',\"" + group.getValue() + "\",'FunctionGroup')");
				}  
				System.out.println("\tGenerating Actonomy Terms: " + stmt.executeUpdate(query.toString()) + " Function Groups");

				//Actonomy Functions
				query = new StringBuilder();
				query.append("insert into ssoc2.actonomy_terms (code, name, parent) values ");
				query.append("('Function','Functions', null)");
				for(Map.Entry function:functions.entrySet()) { 
					query.append(",('" + function.getKey() + "',\"" + function.getValue() + "\",'Function')");
				}  
				System.out.println("\tGenerating Actonomy Terms: " + stmt.executeUpdate(query.toString()) + " Functions");

				//Actonomy Competence
				query = new StringBuilder();
				query.append("insert into ssoc2.actonomy_terms (code, name, parent) values ");
				query.append("('Competence','Competence',null)");
				for(Map.Entry competence:competences.entrySet()) {  
					query.append(",('" + competence.getKey() + "',\"" + competence.getValue() + "\",'Competence')");
				}  
				System.out.println("\tGenerating Actonomy Terms: " + stmt.executeUpdate(query.toString()) + " Competencies");


				// Links from functions to functionGroups
				System.out.println("Deleting Relations: " + stmt.executeUpdate("delete from ssoc2.function_groups") + " function_groups deleted");
				query = new StringBuilder();
				query.append("insert into function_groups values ");
				for(Map.Entry functionGroup:functionGroups.entrySet()) {  
					String key1 = functionGroup.getKey().toString().substring(0, functionGroup.getKey().toString().indexOf("_#_"));
					String key2 = functionGroup.getKey().toString().substring(functionGroup.getKey().toString().indexOf("_#_")+3,functionGroup.getKey().toString().length());
					query.append("('" + key1 + "','" + key2 + "'),");
				}  
				System.out.println("\tGenerating Relations: " + stmt.executeUpdate(query.toString().substring(0, query.length()-1)) + " Functions Groups Links");					

				System.out.println("Deleting Relations: " + stmt.executeUpdate("delete from ssoc2.actonomy_synonyms") + " actonomy_synonyms deleted");
				System.out.println("\tGenerating Relations: " + stmt.executeUpdate(actonomySynonyms.toString().substring(0, actonomySynonyms.length()-1)) + " Actonomy Synonyms");					

				System.out.println(links);
				System.out.println("Deleting Relations: " + stmt.executeUpdate("delete from ssoc2.ssoc_actonomy_occupation") + " ssoc_actonomy_occupation deleted");
				System.out.println("\tGenerating Relations: " + stmt.executeUpdate(links.toString().substring(0, links.length()-1)) + " Actonomy Synonyms");					

				//System.out.println("\nActonomy Functions Groups: \nfunction_code\tfunctionGroup_code functionGroup_label");
				//System.out.println(fGroups);

				System.out.println("\nActonomy Functions Labels: \nfunction code\tlabel");
				//System.out.println(others);
		
				conn.close();
				stmt.close();
				
			}
			catch (ClassNotFoundException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}

	}

}