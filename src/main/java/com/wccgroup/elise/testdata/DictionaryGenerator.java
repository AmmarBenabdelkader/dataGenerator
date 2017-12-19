/**
 * @author abenabdelkader
 *
 * testing.java
 * Oct 7, 2015
 */
package com.wccgroup.elise.testdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
//STEP 1. Import required packages
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

//import com.wccgroup.elise.nes.Offered.Candidate;

public class DictionaryGenerator
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";

	static String lang = "en_us";
	static int maxList = 50;

	static String Offered = "", OfferedKey = "";
	static String[] subOffered;
	// = new ArrayList<String>() {{
	/*	    add("Languages");
		    add("Skill");
		    add("Education");
		    add("work_experience");
		}};
		
	*/ static String Demanded = "", DemandedKey = "", subDemandedKey = "";
	static String[] subDemanded; // = new ArrayList<String>() {{
	/*		    add("Job");
			    add("Desired_Skills");
			}};
	*/
	static String[] product; // = {"skills","jobs"};

	static String path; //= "c:/temp/EliseData/testDictionary";  // assume that path c:/temp/EliseData already exists
	static String[] folderList; // = {"list","listaffinity", "product", "textaffinity","unit"};
	static String[] fileList; // = {"cmf.ed","convert.ed", "dataobject.ed", "language.ed","list.ed","listaffinity.ed","phonetization.ed","preprocess.ed","product.ed","table.ed","textaffinity.ed","udf.ed","unit.ed","view.ed"};

	public static void main(String[] args)
	{

		/*	 try {
				readFile("C:\\temp\\EliseData\\download_nl_sixpp2.csv");
			} catch (FileNotFoundException | UnsupportedEncodingException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			 
			 if (1==1)
				 return;
		*/
		//String listContent = ""; //"yesno\n\t@" + lang + ",\t\"yesno\"\n"; //content for lists list.ed
		Map<String, String> listContent = new HashMap<String, String>();
		readProperties();
		String[] listsTable = null, listsTableO = null, listsTableD = null;
		if (Offered.contains("["))
		{
			listsTableO = Offered.substring(Offered.indexOf("[") + 1, Offered.length() - 1).split(" ");
			Offered = Offered.substring(0, Offered.indexOf("["));

		}
		if (Demanded.contains("["))
		{
			listsTableD = Demanded.substring(Demanded.indexOf("[") + 1, Demanded.length() - 1).split(" ");
			Demanded = Demanded.substring(0, Demanded.indexOf("["));

		}
		if (createFolders() == false)
		{
			System.out.println("Not all folders are properly created");
		}
		try
		{
			languageContent();
			dataobjectContent();
			productContent();
			viewContent();
		}
		catch (IOException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//if (1==1)
		//	 return;

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);
			String domain = "", relation = "";
			String domainAttr = "";

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			DatabaseMetaData databaseMetaData = conn.getMetaData();
			stmt = conn.createStatement();

			// JDBC2Elise data type mappings
			Map<Integer, String> JDBC2Elise = new HashMap<Integer, String>()
			{
				/**
				* 
				*/
				private static final long serialVersionUID = 1L;

				{
					put(1, "enum");
					put(-1, "text");
					put(4, "num");
					put(91, "date");
					put(-6, "text");
					put(-7, "text");
					put(12, "text");
					put(3, "num");
				}
			};

			String tableDetails = "#\n# " + Offered + " details\n#\n";
			Map<String, String> tablesAttr = new HashMap<String, String>(); // stores attributes of tables
			ResultSet rsList;
			ResultSet rs = databaseMetaData.getColumns(null, null, Offered, null);
			boolean found = false;

			while (rs.next())
			{ //loop through all columns of the Offered object 'Candidate'
				//System.out.println("domain for " + rs.getString(4) + " is: " + rs.getString(5) );
				domain = rs.getInt(5) == 1 ? rs.getString(4) : JDBC2Elise.get(rs.getInt(5));
				tableDetails += "- " + rs.getString(4); // column name
				found = false;
				for (String rel : listsTableO)
				{
					if (rel.equalsIgnoreCase(rs.getString(4)))
					{
						found = true;
					}
				}

				if (rs.getInt(5) == 1 || found)
				{ // if data type is enum, set, or list create the list of values
					tableDetails += ",\tdomain(list),\tvalues("
						+ rs.getString(4)
						+ "),\t"
						+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@"
						+ lang
						+ ",\t\""
						+ rs.getString(4).replace("_", " ")
						+ "\"\n";
					listContent.put(
						rs.getString(4),
						rs.getString(4) + "\n\t@" + lang + ",\t\"" + rs.getString(4).replace("_", " ") + "\"\n");
					//System.out.println("Query: select distinct " + rs.getString(4) + " from " + Offered + " limit " + maxList);
					rsList = stmt.executeQuery("select distinct " + rs.getString(4) + " from " + Offered + " limit " + maxList);
					createListValues(rs.getString(4), rsList);
				}
				else
				{
					tableDetails += ",\tdomain("
						+ domain
						+ "),\t"
						+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@"
						+ lang
						+ ",\t\""
						+ rs.getString(4).replace("_", " ")
						+ "\"\n";
				}

			}
			//String foreignKeys = "";
			String primaryKeys = "";

			for (String rel : subOffered)
			{ //loop through all columns of the subOffered object 'Candidate'
				if (rel.contains("["))
				{
					listsTable = rel.substring(rel.indexOf("[") + 1, rel.length() - 1).split(" ");
					rel = rel.substring(0, rel.indexOf("["));

				}
				primaryKeys = " ";
				rs = databaseMetaData.getImportedKeys(conn.getCatalog(), null, rel);
				while (rs.next())
				{
					//foreignKeys += "##" + rs.getString("FKCOLUMN_NAME") + "##";                            
					primaryKeys += "##" + rs.getString("PKCOLUMN_NAME") + "##";
				}
				//System.out.println("\n\t foreignKeys for " + relation + ": " + foreignKeys );
				System.out.println("\n\t primaryKeys for " + rel + ": " + primaryKeys);

				tableDetails += "\n- "
					+ rel
					+ ",\tWEIGHT(0,0,0,0,0,0,100)"
					+ "\n\t@"
					+ lang
					+ ",\t\""
					+ rel.replace("_", " ")
					+ "\""
					+ "\n-- "
					+ (rel.indexOf("_") > 0 ? rel.substring(rel.indexOf("_") + 1, rel.length()) + String.valueOf(Offered.charAt(0))
						: rel + String.valueOf(Offered.charAt(0)))
					+ "\n\t@"
					+ lang
					+ ",\t\"Individual "
					+ rel.replace("_", " ")
					+ "\"\n";

				rs = databaseMetaData.getColumns(null, null, rel, null);

				while (rs.next())
				{
					domain = rs.getInt(5) == 1 ? rs.getString(4) : JDBC2Elise.get(rs.getInt(5));
					if (OfferedKey.equalsIgnoreCase(rs.getString(4)) && primaryKeys.contains(rs.getString(4)))
					{
						continue;
					}

					if (listsTable != null)
					{
						found = false;
						for (String subrel : listsTable)
						{
							if (subrel.equalsIgnoreCase(rs.getString(4)))
							{
								found = true;
							}
						}
					}

					if (rs.getInt(5) == 1 || found)
					{ // if data type is enum, set, or list create the list of values
						tableDetails += "--- "
							+ rs.getString(4)
							+ ",\tdomain(list),\tvalues("
							+ rs.getString(4)
							+ "),\t"
							+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@"
							+ lang
							+ ",\t\""
							+ rs.getString(4).replace("_", " ")
							+ "\"\n";
						listContent.put(
							rs.getString(4),
							rs.getString(4) + "\n\t@" + lang + ",\t\"" + rs.getString(4).replace("_", " ") + "\"\n");
						System.out.println("Query: select distinct " + rs.getString(4) + " from " + rel + " limit " + maxList);
						rsList = stmt.executeQuery("select distinct " + rs.getString(4) + " from " + rel + " limit " + maxList);
						createListValues(rs.getString(4), rsList);
					}
					else
					{
						tableDetails += "--- "
							+ rs.getString(4)
							+ ",\tdomain("
							+ JDBC2Elise.get(rs.getInt(5))
							+ "),\t"
							+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,10)\n\t@"
							+ lang
							+ ",\t\""
							+ rs.getString(4)
							+ "\"\n";
					}

				}

				listsTable = null;
			}
			System.out.println(tableDetails);
			write2file2("product/" + product[0] + ".ed", tableDetails);

			/* Demanded *********************/
			tableDetails = "#\n# " + Demanded + " details\n#\n";
			tablesAttr.clear(); // stores attributes of tables
			rs = databaseMetaData.getColumns(null, null, Demanded, null);

			while (rs.next())
			{ //loop through all columns of the Offered object 'Candidate'
				//System.out.println("domain for " + rs.getString(4) + " is: " + rs.getString(5) );
				domain = rs.getInt(5) == 1 ? rs.getString(4) : JDBC2Elise.get(rs.getInt(5));

				if (subDemandedKey.equalsIgnoreCase(rs.getString(4)))
				{
					continue;
				}

				found = false;
				for (String rel : listsTableD)
				{
					if (rel.equalsIgnoreCase(rs.getString(4)))
					{
						found = true;
					}
				}
				tableDetails += "- " + rs.getString(4); // column name

				if (rs.getInt(5) == 1 || found)
				{ // if data type is enum, set, or list create the list of values
					tableDetails += ",\tdomain(list),\tvalues("
						+ rs.getString(4)
						+ "),\t"
						+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@"
						+ lang
						+ ",\t\""
						+ rs.getString(4).replace("_", " ")
						+ "\"\n";
					listContent.put(
						rs.getString(4),
						rs.getString(4) + "\n\t@" + lang + ",\t\"" + rs.getString(4).replace("_", " ") + "\"\n");
					//System.out.println("Query: select distinct " + domain + " from " + Offered + " limit 20");
					rsList = stmt.executeQuery("select distinct " + rs.getString(4) + " from " + Demanded + " limit " + maxList);
					createListValues(rs.getString(4), rsList);
				}
				else
				{
					tableDetails += ",\tdomain("
						+ domain
						+ "),\t"
						+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@"
						+ lang
						+ ",\t\""
						+ rs.getString(4).replace("_", " ")
						+ "\"\n";
				}

			}

			String foreignKeys = "";
			primaryKeys = "";
			for (String rel : subDemanded)
			{
				if (rel.contains("["))
				{
					listsTable = rel.substring(rel.indexOf("[") + 1, rel.length() - 1).split(" ");
					rel = rel.substring(0, rel.indexOf("["));

				}
				primaryKeys = " ";
				rs = databaseMetaData.getImportedKeys(conn.getCatalog(), null, rel);
				while (rs.next())
				{
					foreignKeys += "##" + rs.getString("FKCOLUMN_NAME") + "##";
					primaryKeys += "##" + rs.getString("PKCOLUMN_NAME") + "##";
				}
				System.out.println("\n\t foreignKeys for " + rel + ": " + foreignKeys);
				System.out.println("\n\t primaryKeys for " + rel + ": " + primaryKeys);

				tableDetails += "\n- "
					+ rel
					+ ",\tWEIGHT(0,0,0,0,0,0,100)"
					+ "\n\t@"
					+ lang
					+ ",\t\""
					+ rel.replace("_", " ")
					+ "\""
					+ "\n-- "
					+ (rel.indexOf("_") > 0 ? rel.substring(rel.indexOf("_") + 1, rel.length()) + String.valueOf(Demanded.charAt(0))
						: rel + String.valueOf(Demanded.charAt(0)))
					+ "\n\t@"
					+ lang
					+ ",\t\"Individual "
					+ rel.replace("_", " ")
					+ "\"\n";

				rs = databaseMetaData.getColumns(null, null, rel, null);

				while (rs.next())
				{
					//System.out.println("domain for " + rs.getString(4) + " is: " + rs.getString(5) );
					domain = rs.getInt(5) == 1 ? rs.getString(4) : JDBC2Elise.get(rs.getInt(5));
					if (DemandedKey.equalsIgnoreCase(rs.getString(4)) && primaryKeys.contains(rs.getString(4)))
					{
						continue;
					}

					if (listsTable != null)
					{
						found = false;
						for (String subrel : listsTable)
						{
							if (subrel.equalsIgnoreCase(rs.getString(4)))
							{
								found = true;
							}
						}
					}

					if (rs.getInt(5) == 1 || found)
					{
						tableDetails += "--- "
							+ rs.getString(4)
							+ ",\tdomain(list),\tvalues("
							+ domain
							+ "),\t"
							+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@"
							+ lang
							+ ",\t\""
							+ rs.getString(4).replace("_", " ")
							+ "\"\n";
						listContent.put(
							rs.getString(4),
							rs.getString(4) + "\n\t@" + lang + ",\t\"" + rs.getString(4).replace("_", " ") + "\"\n");
						System.out.println("Query: select distinct " + rs.getString(4) + " from " + rel + " limit " + maxList);
						rsList = stmt.executeQuery("select distinct " + rs.getString(4) + " from " + rel + " limit " + maxList);
						createListValues(rs.getString(4), rsList);
					}
					else
					{
						/*                    	tableDetails += ",\tdomain(" + domain + "),\t" + 
						        	"WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@" + lang + ",\t\"" + rs.getString(4).replace("_", " ") + "\"\n";
						*/
						tableDetails += "--- "
							+ rs.getString(4)
							+ ",\tdomain("
							+ JDBC2Elise.get(rs.getInt(5))
							+ "),\t"
							+ "WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,10)\n\t@"
							+ lang
							+ ",\t\""
							+ rs.getString(4)
							+ "\"\n";
					}

					/*                       	if (listsTable==null)
					   		continue;
					   	
					   	for (String subrel : listsTable) {
					        System.out.println("\n\t List table for " + subrel + " - " + rel );
					    	tableDetails += "--- " + rel + ",\tdomain(list),\tvalues(" + subrel + "),\t" + 
					            	"WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,1)\n\t@" + lang + ",\t\"" + subrel.replace("_", " ") + "\"\n";
					    	listContent.put(subrel, subrel + "\n\t@" + lang + ",\t\"" + subrel.replace("_", " ") + "\"\n");
					    	//System.out.println("Query: select distinct " + domain + " from " + Offered + " limit 20");
							rsList = stmt.executeQuery("select distinct " + subrel + " from " + rel + " limit 20");
							createListValues( subrel, rsList);
					   	
					    }
					 */ listsTable = null;
				}
				/*           	domain = "domain(text),\t";
				            	domainAttr = "";
				            	if (rel.indexOf("[")>0) {
				            		domain = "list";
				            		domainAttr = rel.substring(rel.indexOf("[")+1, rel.indexOf("]"));
				            		rel = rel.substring(0, rel.indexOf("["));
				            		domain = "domain(list),\tvalues(" + rel + "),\t";
				            		
				            		rsList = stmt.executeQuery("select distinct " + domainAttr + " from " + rel + " limit 20");
				            		createListValues(rel, rsList); //select distinct domainAttr from relation
					//listContent += rel + "\n\t@" + lang + ",\t\"" + rel + "\"\n";
					listContent.put(rel, rel + "\n\t@" + lang + ",\t\"" + rel.replace("_", " ") + "\"\n");
				            	}
				System.out.println(domainAttr );
				            	
				foreignKeys = "";
				rs = databaseMetaData.getImportedKeys(conn.getCatalog(), null, rel);
				while (rs.next()) {             
				    foreignKeys += "##" + rs.getString("FKCOLUMN_NAME") + "##";                            
				}
				//System.out.println("\n\t foreignKeys for " + relation + ": " + foreignKeys );
				
				            	tableDetails += "\n- " + rel + ",\tWEIGHT(0,0,0,0,0,0,100)" + 
				            			"\n\t@" + lang + ",\t\"" + rel.replace("_", " ") + "\"" +
				            			"\n-- " + rel + 
				            			"\n\t@" + lang + ",\t\"Individual " + rel.replace("_", " ") + "\"\n";
				
				            	
				rs = databaseMetaData.getColumns(null, null,  rel, null);
				
				    while(rs.next()){
				    	if (!foreignKeys.contains("##" + rs.getString(4) +"##"))
				    	tableDetails += "--- " + rs.getString(4) + ", domain(" + JDBC2Elise.get(rs.getInt(5)) + "),\t" + 
				            	"WEIGHT(0,0,0,100),\tMININSTANCES(0,0),\tMAXINSTANCES(1,10)\n\t@" + lang + ",\t\"" + rs.getString(4) + "\"\n";
				    }
				*/ }
			System.out.println(tableDetails);
			write2file2("product/" + product[1] + ".ed", tableDetails);
			write2file("list.ed", listContent);

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
	}//end main

	static boolean createFolders()
	{
		boolean result = true;
		File dir = new File(path);
		if (!dir.exists())
		{
			result = dir.mkdir();
		}

		for (int i = 0; i < folderList.length; i++)
		{
			dir = new File(path + "/" + folderList[i]);
			if (!dir.exists())
			{
				if (dir.mkdir() == false)
				{
					result = false;
				}
			}
		}
		File f;
		//f.getParentFile().mkdirs(); 
		try
		{
			for (int i = 0; i < fileList.length; i++)
			{
				f = new File(path + "/" + fileList[i]);
				f.createNewFile();
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	static void languageContent() throws IOException
	{

		write2file2("language.ed", lang + "\n\t@" + lang + ",\t\"English\"");
	}

	static void dataobjectContent() throws IOException
	{
		String content = Offered
			+ ",\toffered("
			+ product[0]
			+ "),\tdemanded("
			+ product[1]
			+ ")\n"
			+ "\t@"
			+ lang
			+ ",\t\""
			+ Offered
			+ "\"\n"
			+ Demanded
			+ ",\toffered("
			+ product[1]
			+ "),\tdemanded("
			+ product[0]
			+ ")\n"
			+ "\t@"
			+ lang
			+ ",\t\""
			+ Demanded
			+ "\"";

		write2file2("dataobject.ed", content);
	}

	static void productContent() throws IOException
	{
		String content = product[0]
			+ "\n"
			+ "\t@"
			+ lang
			+ ",\t\""
			+ product[0]
			+ "\"\n"
			+ product[1]
			+ "\n"
			+ "\t@"
			+ lang
			+ ",\t\""
			+ product[1]
			+ "\"";

		write2file2("product.ed", content);
	}

	static void viewContent() throws IOException
	{
		String content = "default,\tDEFAULT\n" + "\t@" + lang + ",\t\"Default view\"";

		write2file2("view.ed", content);
	}

	static int write2file(String myfile, Map<String, String> listContent) throws IOException
	{

		File file = new File(path + "/" + myfile);

		// if file doesnt exists, then create it
		if (!file.exists())
		{
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		Iterator<Map.Entry<String, String>> it = listContent.entrySet().iterator();
		while (it.hasNext())
		{

			Map.Entry<String, String> entry = it.next();
			bw.write(entry.getValue());

		}

		bw.close();
		return 1;
	}

	static int write2file2(String myfile, String tableDetails) throws IOException
	{

		File file = new File(path + "/" + myfile);

		// if file doesnt exists, then create it
		if (!file.exists())
		{
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(tableDetails);
		bw.close();
		return 1;
	}

	static int createListValues(String myfile, ResultSet rsList) throws IOException
	{

		if (rsList == null)
		{
			return 0;
		}

		String content = "";
		int i = 0;
		try
		{
			while (rsList.next())
			{
				content += i++ + "\n\t@" + lang + ", \"" + rsList.getString(1) + "\"\n"; // column name
			}
		}
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		File file = new File(path + "/list/" + myfile + ".ed");

		// if file doesnt exists, then create it
		if (!file.exists())
		{
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(content);
		bw.close();
		return 1;
	}

	public static void readProperties()
	{
		Properties prop = new Properties();
		InputStream input = null;

		try
		{

			input = new FileInputStream("dictionary.properties");

			// load a properties file
			prop.load(input);
			JDBC_DRIVER = prop.getProperty("driver");
			DB_URL = prop.getProperty("url");
			USER = prop.getProperty("user");
			PASS = prop.getProperty("pass");
			Offered = prop.getProperty("Offered");
			Demanded = prop.getProperty("Demanded");
			subOffered = prop.getProperty("subOffered").split(" ");
			subDemanded = prop.getProperty("subDemanded").split(" ");
			folderList = prop.getProperty("folderList").split(" ");
			fileList = prop.getProperty("fileList").split(" ");
			product = prop.getProperty("product").split(" ");
			path = prop.getProperty("path");
			OfferedKey = prop.getProperty("OfferedKey");
			DemandedKey = prop.getProperty("DemandedKey");
			subDemandedKey = prop.getProperty("subDemandedKey");

		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			if (input != null)
			{
				try
				{
					input.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	public static void readFile(String csvFile) throws FileNotFoundException, UnsupportedEncodingException
	{

		BufferedReader br = null;
		PrintWriter writer = new PrintWriter("C:\\temp\\EliseData\\download_nl_sixpp3.csv", "UTF-8");
		try
		{

			String sCurrentLine;

			br = new BufferedReader(new FileReader(csvFile));

			while ((sCurrentLine = br.readLine()) != null)
			{
				if (sCurrentLine.length() > 10)
				{
					writer.println(sCurrentLine);
				}

				/*				//System.out.println(sCurrentLine.replace(" ", ""));
								for (int i=0; i<sCurrentLine.length();i++) {
									if (sCurrentLine.charAt(i)!=0) {
										//System.out.print(sCurrentLine.charAt(i));
										//System.out.print("'" + sCurrentLine.charAt(i) + "': " + (int) sCurrentLine.charAt(i));
										writer.print(sCurrentLine.charAt(i));
									}
				*/ //}
					//writer.print("\r");

			}
			writer.close();

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if (br != null)
				{
					br.close();
				}
			}
			catch (IOException ex)
			{
				ex.printStackTrace();
			}
		}

	}
}