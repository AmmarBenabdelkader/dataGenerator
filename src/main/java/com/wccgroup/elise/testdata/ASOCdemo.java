/**
* @author abenabdelkader
*
* testing.java
* Oct 7, 2015
*/
package com.wccgroup.elise.testdata;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.HttpsURLConnection;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ASOCdemo
{
	private final String USER_AGENT = "Mozilla/5.0";

	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	// Request POST parameters for http://ar.kwe.li 
	static String format = "json";
	static String lang = "en";
	static String code = "";
	static int maxConcepts = 0;
	static double fullmatch = 0.0;
	static double treshold = 0.0;
	static List<Occupation> OCs = new ArrayList<Occupation>();
	static String baseUrl = "";
	static String token = "";
	static String stopwords_ar = "";
	static String stopwords_en = "";
	static String asoc_file = "";
	static String title = "";
	static Map<String, String> ASOCtitles = new HashMap<String, String>();

	public static void main(String[] args) throws IOException
	{
		try
		{
			readProperties();
			readDBProperties();
			//generateJobEducation();
			//cleanJobTitle();
/*			String[] titles = {"مُعقب","مُعَقب","مُعَقِب","مُعَقِبٌ","نائب المدير العام","المُعَقِبٌ الأَوَلُ","الةٌ","ال نهيّان","المساعد الإداري لمدير عام الإدارة","مستشار نائب المديرالعام"};
			for (int i = 0; i < titles.length; i++)
				System.out.println(titles[i] + ": " + cleanJobTitle_clitics(titles[i]));

			checkJobTitleASOC3();
*/			//checkJobTitleMOL3();
			//createASOC();
			/*			String[] occupation = {"Sales Consultant","enthusiastic hospital nurse","محام","Consultant Gastroenterology","Professional nurse","software engineer"};
						title = stopwords(occupation[5]);
						//title = occupation[0];
						if (!title.equalsIgnoreCase(occupation[5]))
							System.out.println (occupation[5] + " --> " + title);
						OCC_Insight(title);	            
			*/ 
			//taxonomy("Nuclear Physicist");	    
			ASOC3("Nuclear Physicist");	    
				//printOccupations();
				//cmpLabelsSynonyms();
				//cmpLabelsSimilars();
				//cmpSkillsSoftSkills();
				//ExtractJobExamples();
				//ExtractJobExamples_ar();
				/* Based on ASOC MOL */
			//classify_mol("P:\\HRDF - ASOC\\RFP Q4 2015\\ASOC documents\\Mol List.xlsx", "ASOC Occupation Code", "en", "asoc.occupation_classif_new", .6, 2);
			//classify_mol("P:\\HRDF - ASOC\\RFP Q4 2015\\ASOC documents\\Mol List.xlsx", "ASOC Occupation Code", "ar", "asoc.occupation_classif_new", .6, 2);
				/* Based on the new ASOC occupations */
			//classify_newASOC("P:\\HRDF - ASOC\\RFP Q4 2015\\ASOC documents\\ASOC Index English and Arabic 20150831 (4)_REV_final.xlsx", "ASOC Occupation Code", "ar", "asoc.occupation_classif_new", .6, 1284);
			//classify_molOccupation("Logistis Specialist", "ASOC Occupation Code");
			//classify_molOccupation("محام", "ASOC Occupation Code");
		}
		catch (Exception e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (1 == 1)
		{
			return;
		}

		try
		{
			//FileInputStream file = new FileInputStream(new File("P:\\HRDF\\Data\\ESCO\\ArabicVacanciesSet - 13 08 2015.xlsx"));
			FileInputStream file = new FileInputStream(
				new File("C:\\temp\\HRDF\\Data\\ESCO\\ArabicVacanciesSet - 13 08 2015.xlsx"));

			System.out.println(
				"<!DOCTYPE html>\n<html"
					+ (lang.equalsIgnoreCase("ar") ? " dir=rtl" : "")
					+ ">\n\t<head>\n\t<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>"
					+ "<style>table, th, td {border: 1px solid black;border-collapse: collapse;}</style>"
					+ "\n\t<title>ASOC job Classification</title>\n\t<table border=1 cellspacing=0>\n\t\t<tr><td colspan=7 bgcolor=#B2F0FF>"
					+ "<center><br> ************ Results for HRDF jobs against Janzz taxonomy ************ <br>&nbsp;"
					+ "</td></tr>");
			System.out.println(new Date());
			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(1);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			int i = 1;
			title = "";
			while (rowIterator.hasNext())
			{
				//rowIterator.next();
				Row row = rowIterator.next();
				//For each row, iterate through all the columns
				if (row.getCell(0).getCellType() == 1)
				{ // String  
					title = row.getCell(0).getStringCellValue();
					OCC_Insight(stopwords(title));
					//OCC_Insight(title);
					saveOccupations(title);
					OCs.clear();

				}
				//System.out.println("title: "  + row.getCell(0).getStringCellValue());
				if (i++ == 15)
				{
					break;
				}

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

	// HTTP POST request
	private static void sendPost(String data) throws IOException
	{
		URL url = new URL("http://ar.kwe.li/xml");
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);

		String loginPassword = "nl:janzz";
		String encoded = new String(Base64.encodeBase64(StringUtils.getBytesUtf8(loginPassword)));
		conn.setRequestProperty("Authorization", "Basic " + encoded);

		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "text/plain");
		//conn.setRequestProperty("charset", "windows-1256");
		//conn.setRequestProperty("charset", "iso-8859-6");
		//conn.setRequestProperty("Content-language", "ar");
		conn.setRequestProperty("charset", "UTF-8");

		//conn.connect();
		OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
		//System.out.println(data);

		String line;
		writer.write(data);
		writer.flush();
		BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		while ((line = reader.readLine()) != null)
		{
			System.out.println(line);
		}

		writer.close();
		reader.close();
	}

	// HTTP POST request
	private static String sendPostAPI(String url) throws Exception
	{

		//String url = "https://www.janzz.jobs/japi/expand_concept?q=" + data;
		URL obj = new URL(url);
		HttpsURLConnection con = (HttpsURLConnection)obj.openConnection();

		con.setRequestProperty("Authorization", "token " + token);

		//add reuqest header
		con.setRequestMethod("GET");
		//con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

		// Send post request
		con.setDoOutput(true);

		int responseCode = con.getResponseCode();
		//System.out.println("\nSending 'GET' request to URL : " + url);
		//System.out.println("Response Code : " + responseCode);
		if (responseCode != 200)
		{
			return null;
		}

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null)
		{
			response.append(inputLine);
		}

		//print result
		//System.out.println(response.toString());

		in.close();

		return response.toString();

	}

	private static void getConceptDetails(String conceptId, int l) throws Exception
	{
		String url = baseUrl
			+ "concepts/"
			+ conceptId
			+ "?branch=occupation&format="
			+ format
			+ "&search_lang="
			+ lang
			+ "&output_lang="
			+ lang;
		String subResults = sendPostAPI(url);
		System.out.println("to get desc, oc, labels, classes: " + url + "\n" + subResults);

		JSONParser jsonParser = new JSONParser();
		if (subResults == null)
		{
			return;
		}
		JSONObject subObject = (JSONObject)jsonParser.parse(subResults);

		//System.out.println( "<td>  - Preferred label: " +  subObject.get("preferred_label") + "<br>  - Labels:");
		//System.out.println( "<td>");

		List<String> descr2 = new ArrayList<String>();
		JSONArray descrs = (JSONArray)subObject.get("descr_set");
		Iterator j = descrs.iterator();
		while (j.hasNext())
		{
			JSONObject innerObj = (JSONObject)j.next();
			if (innerObj.get("lang").toString().equalsIgnoreCase(lang))
			{
				descr2.add(innerObj.get("text").toString());
			}
		}
		OCs.get(l).setDescr(descr2);

		List<String> labels2 = new ArrayList<String>();
		JSONArray labels = (JSONArray)subObject.get("label_set");
		j = labels.iterator();
		// take each value from the json array separately
		while (j.hasNext())
		{
			JSONObject innerObj = (JSONObject)j.next();
			if (innerObj.get("lang").toString().equalsIgnoreCase(lang))
			{
				//System.out.println( "<br>- " + innerObj.get("text")); // + "\t tags: " + innerObj.get("tags"));
				labels2.add(innerObj.get("text").toString());
			}
		}
		OCs.get(l).setLabels(labels2);

		List<String> classes = new ArrayList<String>();
		JSONArray cl_sets = (JSONArray)subObject.get("classification_set");
		j = cl_sets.iterator();
		// take each value from the json array separately
		while (j.hasNext())
		{
			JSONObject innerObj = (JSONObject)j.next();
			if (code.contains(innerObj.get("classification").toString()))
			{
				//System.out.print( "<br>" + title + ": " + innerObj.get("val") + ": " + innerObj.get("classification"));
				classes.add(
					innerObj.get("val").toString()
						+ ": "
						+ innerObj.get("classification").toString()
						+ " ('"
						+ ASOCtitles.get(innerObj.get("val").toString())
						+ "')");
				if (ASOCtitles.get(innerObj.get("val").toString()) != null
					&& title.equalsIgnoreCase(ASOCtitles.get(innerObj.get("val").toString()).trim()))
				{
					OCs.get(l).setMatch(100);
				}
				;

			}
		}
		OCs.get(l).setClasses(classes);

		JSONArray subLang = (JSONArray)subObject.get("occupation_classes");
		if (subLang.size() > 0)
		{
			j = subLang.iterator();
			//System.out.print("<td>");
			// take each value from the json array separately
			for (int s = 0; s < subLang.size(); s++)
			{
				//System.out.print(subLang.get(s) + " ");
				OCs.get(l).setOC(subLang.get(s).toString());
			}
			//System.out.println("\n</td></tr>");
		}

	}

	private static void OCC_Insight(String occupation) throws Exception
	{

		String url = baseUrl
			+ "concept_fulltext?q="
			+ occupation.replace(" ", "%20")
			+ "&branch=occupation&format="
			+ format
			+ "&search_lang="
			+ lang
			+ "&output_lang="
			+ lang;
		System.out.println(url);
		String results = sendPostAPI(url);

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
		JSONArray lang = (JSONArray)jsonObject.get("results");

		// take the elements of the json array

		/*            for(int i=0; i<lang.size(); i++){
		        System.out.println("Element " + i + ": "+lang.get(i));
		    }
		*/ Iterator i = lang.iterator();
		double score = 0.0;
		String id = "";
		int l = 0;
		// take each value from the json array separatel
		while (i.hasNext() && l < maxConcepts)
		{
			JSONObject innerObj = (JSONObject)i.next();
			OCs.add(
				new Occupation(
					l,
					innerObj.get("id").toString(),
					innerObj.get("label").toString(),
					"",
					Double.parseDouble(innerObj.get("score").toString())));
			//System.out.println( "\n<tr><td> " + l + ". </td><td>" + innerObj.get("id") + "</td><td>" + innerObj.get("label") + "</td><td>" + innerObj.get("score") + "</td>");
			getConceptDetails(innerObj.get("id").toString(), l);
			if (Double.parseDouble(innerObj.get("score").toString()) > score)
			{
				score = Double.parseDouble(innerObj.get("score").toString());
				id = innerObj.get("id").toString();
			}
			l++;
		}

	}

	/* the taxonomy developer generates a complete taxonomy for a given occupation
	 * the taxonomy module expects a valid occupation name as an argument 
	 */
	private static void taxonomy(String occupation) throws Exception
	{

		String url = baseUrl
			+ "concept_fulltext?q="
			+ occupation.replace(" ", "%20")
			+ "&baranch=occupation&format="
			+ format
			+ "&search_lang="
			+ lang
			+ "&output_lang="
			+ lang;
		//System.out.println("<center>\t\t<br>**************  <b>" + occupation + "</b>  **********************" + "<br>.\n\t</td></tr>");
		System.out.println("to get id, concept, score: " + url);

		String results = sendPostAPI(url);

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
		JSONArray lang = (JSONArray)jsonObject.get("results");

		Iterator i = lang.iterator();
		//double score =0.0;
		String id = "";
		int l = 0;
		// fetch the correct ID
		while (i.hasNext())
		{
			JSONObject innerObj = (JSONObject)i.next();
			//if (innerObj.get("label").toString().equalsIgnoreCase(occupation) && Double.parseDouble(innerObj.get("score").toString())>=fullmatch) {
			if (Double.parseDouble(innerObj.get("score").toString()) >= fullmatch)
			{
				//System.out.println( "\n<b>" + innerObj.get("id") + ": " + innerObj.get("label") + "</b>");
				OCs.add(
					new Occupation(
						l,
						innerObj.get("id").toString(),
						innerObj.get("label").toString(),
						"",
						Double.parseDouble(innerObj.get("score").toString())));
				getConceptDetails(innerObj.get("id").toString(), l);
				OCC_expand(innerObj.get("label").toString(), l);
				getSkills(innerObj.get("id").toString(), l);
				//getSoftSkills(innerObj.get("id").toString(), l);
				l++;
			}
		}
		//System.out.println("</ul></td></tr></table></html>");
	}

	/* the taxonomy developer generates a complete taxonomy for a given occupation
	 * the taxonomy module expects a valid occupation name as an argument 
	 */
	private static void ASOC3(String occupation) throws Exception
	{

		String url = baseUrl
			+ "concept_fulltext?q="
			+ occupation.replace(" ", "%20")
			+ "&baranch=occupation&format="
			+ format
			+ "&search_lang="
			+ lang
			+ "&output_lang="
			+ lang;
		//System.out.println("<center>\t\t<br>**************  <b>" + occupation + "</b>  **********************" + "<br>.\n\t</td></tr>");
		//System.out.println("to get id, concept, score: " + url);

		String results = sendPostAPI(url);
		System.out.println("to get id, concept, score: " + url + "\n" + results);
		
		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
		JSONArray lang = (JSONArray)jsonObject.get("results");

		Iterator i = lang.iterator();
		//double score =0.0;
		String id = "";
		int l = 0;
		// fetch the correct ID
		while (i.hasNext())
		{
			JSONObject innerObj = (JSONObject)i.next();
			//if (innerObj.get("label").toString().equalsIgnoreCase(occupation) && Double.parseDouble(innerObj.get("score").toString())>=fullmatch) {
			if (Double.parseDouble(innerObj.get("score").toString()) >= fullmatch)
			{
				//System.out.println( "\n<b>" + innerObj.get("id") + ": " + innerObj.get("label") + "</b>");
				OCs.add(
					new Occupation(
						l,
						innerObj.get("id").toString(),
						innerObj.get("label").toString(),
						"",
						Double.parseDouble(innerObj.get("score").toString())));
				getConceptDetails(innerObj.get("id").toString(), l);
				OCC_expand(innerObj.get("label").toString(), l);
				getSkills(innerObj.get("id").toString(), l);
				//getSoftSkills(innerObj.get("id").toString(), l);
				l++;
			}
		}
		//System.out.println("</ul></td></tr></table></html>");
	}

	/*
	 * This method classifies the ASOC 'MOL' occupations against registered ASOC codes in Jannz ontology
	 * the method takes as input:
	 * 		- excel file with mol occupations
	 * 		- wanted ASOC code, possible values are: 'ASOC/NES' , 'ASOC Occupation Code', 'ASOC JobTitle Code'
	 * 		- language, possible values are: 'en' and 'ar'
	 * 		- rows to skip in the excel file (usually two)
	 */
	private static void classify_mol(String asoc_file, String want_code, String lang, String table, double threshold, int skip) throws Exception
	{

		try
		{

			Date sDate = new Date();
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			Connection conn = DriverManager
				.getConnection("jdbc:mysql://localhost/asoc?useUnicode=true&characterEncoding=utf-8", "root", "");
			Statement stmt = conn.createStatement();
			ResultSet rs;
			FileInputStream file = new FileInputStream(new File(asoc_file));

			XSSFWorkbook workbook = new XSSFWorkbook(file);
			XSSFSheet sheet = workbook.getSheetAt(0);

			Iterator<Row> rowIterator = sheet.iterator();
			Row row;
			//row = rowIterator.next(); rowIterator.next(); 
			for (int i = 1; i <= skip; i++)
			{
				rowIterator.next();
			}
			String occupation = "";
			int j = skip;
			while (rowIterator.hasNext())
			{
				row = rowIterator.next();
				if (lang.equalsIgnoreCase("en"))
				{
					occupation = row.getCell(5).getStringCellValue().trim();
				}
				if (lang.equalsIgnoreCase("ar"))
				{
					occupation = row.getCell(4).getStringCellValue().trim();
				}

				rs = stmt.executeQuery("SELECT * FROM asoc.occupation_classif_new where classif_title='Auditing Firm Manager'");
				if (rs.next())
					continue;
				String url = baseUrl
					+ "fulltext_classify?text="
					+ occupation.replace(" ", "%20")
					+ "&format="
					+ format
					+ "&want_code="
					+ want_code.replace(" ", "%20")
					+ "&output_lang="
					+ lang;
				System.out.println(url);
				String results = sendPostAPI(url);

				row.getCell(0).setCellType(1);
				row.getCell(1).setCellType(1);
				//System.out.print("\n" + row.getCell(0).getStringCellValue() + ";" + row.getCell(1).getStringCellValue() + ";" + row.getCell(4).getStringCellValue() + ";" + row.getCell(5).getStringCellValue() + ";");

				JSONParser jsonParser = new JSONParser();
				JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
				JSONObject jsonObject2 = (JSONObject)jsonObject.get("results");
				//if (jsonObject2==null)
				//	continue;
				JSONArray classif = (JSONArray)jsonObject2.get("best_code");

				if (classif == null)
				{
					continue;
				}

				Iterator i = classif.iterator();
				String query = "";
				while (i.hasNext())
				{
					JSONObject innerObj = (JSONObject)i.next();
					if (Double.parseDouble(innerObj.get("accuracy").toString()) >= threshold)
					{
						query = " insert into " + table + " values ("
							+ row.getCell(0).getStringCellValue()
							+ ", "
							+ row.getCell(1).getStringCellValue()
							+ ", "
							+ innerObj.get("code").toString()
							+ ", \""
							+ innerObj.get("label").toString()
							+ "\", "
							+ innerObj.get("accuracy").toString()
							+ ", '"
							+ innerObj.get("classification").toString()
							+ "', '"
							+ lang
							+ "', 'MOL')";
						System.out.println(j + ". " + query);
						if (innerObj.get("code").toString().contains(","))
						{
							break;
						}
						//stmt.executeUpdate(query);
					}
				}
				j++;
			}
			file.close();
			workbook.close();
			stmt.close();
			conn.close();
			System.out.println("Started at: " + sDate);
			System.out.println("Finished at: " + new Date());

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/*
	 * This method classifies the ASOC 'MOL' occupations against registered ASOC codes in Jannz ontology
	 * the method takes as input:
	 * 		- excel file with mol occupations
	 * 		- wanted ASOC code, possible values are: 'ASOC/NES' , 'ASOC Occupation Code', 'ASOC JobTitle Code'
	 * 		- language, possible values are: 'en' and 'ar'
	 * 		- rows to skip in the excel file (usually two)
	 */
	private static void classify_newASOC(String asoc_file, String want_code, String lang, String table, double threshold, int skip) throws Exception
	{

		try
		{

			Date sDate = new Date();
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			Connection conn = DriverManager
				.getConnection("jdbc:mysql://localhost/asoc?useUnicode=true&characterEncoding=utf-8", "root", "");
			Statement stmt = conn.createStatement();
			ResultSet rs;
			FileInputStream file = new FileInputStream(new File(asoc_file));

			XSSFWorkbook workbook = new XSSFWorkbook(file);
			XSSFSheet sheet = workbook.getSheetAt(0);

			Iterator<Row> rowIterator = sheet.iterator();
			Row row;
			//row = rowIterator.next(); rowIterator.next(); 
			for (int i = 1; i <= skip; i++)
			{
				rowIterator.next();
			}
			String occupation = "";
			int j = skip;
			while (rowIterator.hasNext())
			{
			
				row = rowIterator.next();
				if (lang.equalsIgnoreCase("en"))
				{
					occupation = row.getCell(3).getStringCellValue().trim();
				}
				if (lang.equalsIgnoreCase("ar"))
				{
					occupation = row.getCell(4).getStringCellValue().trim();
				}

				rs = stmt.executeQuery("SELECT * FROM asoc.occupation_classif_new where classif_title=\"" + occupation + "\"");
				if (rs.next())
					continue;
				
				String url = baseUrl
					+ "fulltext_classify?text="
					+ occupation.replace(" ", "%20").replace("'", "%27")
					+ "&format="
					+ format
					+ "&want_code="
					+ want_code.replace(" ", "%20")
					+ "&output_lang="
					+ lang;
				System.out.println(url);
				String results = sendPostAPI(url);

				row.getCell(0).setCellType(1);
				row.getCell(1).setCellType(1);
				//System.out.print("\n" + row.getCell(0).getStringCellValue() + ";" + row.getCell(1).getStringCellValue() + ";" + row.getCell(4).getStringCellValue() + ";" + row.getCell(5).getStringCellValue() + ";");

				JSONParser jsonParser = new JSONParser();
				JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
				JSONObject jsonObject2 = (JSONObject)jsonObject.get("results");
				//if (jsonObject2==null)
				//	continue;
				JSONArray classif = (JSONArray)jsonObject2.get("best_code");

				if (classif == null)
				{
					continue;
				}

				Iterator i = classif.iterator();
				String query = "";
				while (i.hasNext())
				{
					JSONObject innerObj = (JSONObject)i.next();
					if (Double.parseDouble(innerObj.get("accuracy").toString()) >= threshold)
					{
						query = " insert into " + table + " values ('"
							+ row.getCell(2).getStringCellValue()
							+ "', "
							+ row.getCell(0).getStringCellValue()
							+ ", "
							+ innerObj.get("code").toString()
							+ ", \""
							+ innerObj.get("label").toString()
							+ "\", "
							+ innerObj.get("accuracy").toString()
							+ ", '"
							+ innerObj.get("classification").toString()
							+ "', '"
							+ lang
							+ "', 'newASOC')";
						System.out.println(j + ". " + query);
						if (innerObj.get("code").toString().contains(","))
						{
							break;
						}
						stmt.executeUpdate(query);
					}
				}
				j++;
			}
			file.close();
			workbook.close();
			stmt.close();
			conn.close();
			System.out.println("Started at: " + sDate);
			System.out.println("Finished at: " + new Date());

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static void classify_molOccupation(String occup, String want_code) throws Exception
	{

		try
		{
			int l = (int)occup.charAt(1);
			if (l>=65 && l<=122)
				lang = "en";
			else
				lang = "ar";

			String url = baseUrl
				+ "fulltext_classify?text="
				+ occup.replace(" ", "%20")
				+ "&format="
				+ format
				+ "&want_code="
				+ want_code.replace(" ", "%20")
				+ "&output_lang="
				+ lang;
			//System.out.println(url);
			String results = sendPostAPI(url);

			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
			JSONObject jsonObject2 = (JSONObject)jsonObject.get("results");

			// STEP 1: Retrieve best matches for the occupation
			JSONArray classif = (JSONArray)jsonObject2.get("best_code");
			if (classif == null)
				return;
			
			String best_codes = ""; // String to hold codes in best_code matches

			Iterator i = classif.iterator();
			System.out.println("Matches for \"" + occup + "\": (" + want_code + ")");
			while (i.hasNext())
			{
				JSONObject innerObj = (JSONObject)i.next();
				System.out.println(
					"\t- "
						+ innerObj.get("code").toString()
						+ ": \""
						+ innerObj.get("label").toString()
						+ "\" \t "
						+ innerObj.get("accuracy").toString()
						+ " match");
				
				best_codes += innerObj.get("code").toString() + innerObj.get("label").toString();
			}

			// STEP 2: Retrieve complete matching set for the occupation
			classif = (JSONArray)jsonObject2.get("top_codes");

			if (classif == null)
				return;
			String top_codes = ""; // String to hold codes in top_codes matches

			i = classif.iterator();
			System.out.println("\t Other matching concepts:");
			while (i.hasNext())
			{
				JSONObject innerObj = (JSONObject)i.next();
				
				top_codes += innerObj.get("cid").toString() + innerObj.get("label").toString();
				
				if (best_codes.contains(innerObj.get("code").toString() + innerObj.get("label").toString()))
					continue;
				
				System.out.println(
					"\t\t- "
						+ innerObj.get("cid").toString()
						+ "/"
						+ innerObj.get("code").toString()
						+ ": \t\""
						+ innerObj.get("label").toString()
						+ "\" \t "
						+ innerObj.get("score").toString()
						+ " match");
			}

			// STEP 3: Retrieve other related concepts for the occupation
			classif = (JSONArray)jsonObject2.get("top_concepts");

			if (classif == null)
				return;

			i = classif.iterator();
			System.out.println("\t Other related concepts:");
			while (i.hasNext())
			{
				JSONObject innerObj = (JSONObject)i.next();
				if (top_codes.contains(innerObj.get("cid").toString() + innerObj.get("label").toString()))
					continue;
				System.out.println(
					"\t\t- "
						+ innerObj.get("cid").toString()
						+ ": \t\""
						+ innerObj.get("label").toString()
						+ "\" \t "
						+ innerObj.get("score").toString()
						+ " match");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static void OCC_expand(String occupation, int l) throws Exception
	{

		String url = baseUrl
			+ "expand_concept?q="
			+ occupation.replace(" ", "%20")
			+ "&branch=occupation&format="
			+ format
			+ "&search_lang="
			+ lang
			+ "&output_lang="
			+ lang;
		System.out.println("to get synonyms, similars, childs, parents: " + url);
		List<String> synonyms = new ArrayList<String>();
		List<String> parents = new ArrayList<String>();
		List<String> childs = new ArrayList<String>();
		List<String> similars = new ArrayList<String>();
		List<String> others = new ArrayList<String>();

		String results = sendPostAPI(url);
		
		System.out.println("to get synonyms, similars, childs, parents: " + url + "\n" + results);

		if (results == null)
		{
			return;
		}

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
		JSONArray lang = (JSONArray)jsonObject.get("results");

		Iterator i = lang.iterator();
		// fetch the correct ID
		while (i.hasNext())
		{
			JSONObject innerObj = (JSONObject)i.next();
			switch (innerObj.get("rel").toString())
			{
			case "synonym":
				synonyms.add(innerObj.get("label").toString());
				break;

			case "child":
				childs.add(innerObj.get("label").toString());
				break;

			case "parent":
				parents.add(innerObj.get("label").toString());
				break;
			case "similar":
				similars.add(innerObj.get("label").toString());
				break;
			default:
				others.add(innerObj.get("label").toString() + " (" + innerObj.get("rel").toString() + ")");
				break;
			}
		}
		OCs.get(l).setSynonyms(synonyms);
		OCs.get(l).setParents(parents);
		OCs.get(l).setChilds(childs);
		OCs.get(l).setSimilars(similars);
		OCs.get(l).setOthers(others);
	}

	private static void getSimilarJobs(String occupation, String lang) throws Exception
	{

		String url = baseUrl
			+ "expand_concept?q="
			+ occupation.replace(" ", "%20")
			+ "&branch=occupation&format="
			+ format
			+ "&search_lang="
			+ lang
			+ "&output_lang="
			+ lang;
		//System.out.println("to get synonyms, similars, childs, parents: " + url);
		List<String> synonyms = new ArrayList<String>();
		List<String> similars = new ArrayList<String>();

		String results = sendPostAPI(url);
		if (results == null)
		{
			return;
		}

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
		JSONArray lang2 = (JSONArray)jsonObject.get("results");

		Iterator i = lang2.iterator();
		// fetch the correct ID
		while (i.hasNext())
		{
			JSONObject innerObj = (JSONObject)i.next();
			switch (innerObj.get("rel").toString())
			{
			case "synonym":
				synonyms.add(innerObj.get("label").toString());
				break;

			case "similar":
				similars.add(innerObj.get("label").toString());
				break;
			default:
				break;
			}
		}
		OCs.get(0).setSynonyms(synonyms);
		OCs.get(0).setSimilars(similars);
	}

	/* Print Occupations in plain text ***********************/
	private static void getSkills(String occupationId, int l) throws Exception
	{

		String url = baseUrl
			+ "concept_relation_by_id/?relation=skills&concept_id="
			+ occupationId
			+ "&include_descendants=true&branch=skill&format="
			+ format
			+ "&lang="
			+ lang;
		System.out.println("to get skills: " + url);
		List<String> skills = new ArrayList<String>();

		String results = sendPostAPI(url);
		System.out.println("to get skills: " + url + "\n" + results);
		
		if (results == null)
		{
			return;
		}
		results = "{\"results\": " + results + "}";

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
		JSONArray langA = (JSONArray)jsonObject.get("results");

		Iterator i = langA.iterator();
		// fetch the correct ID
		while (i.hasNext())
		{
			JSONObject innerObj = (JSONObject)i.next();
			if (innerObj.get("preferred_label_lang").toString().equalsIgnoreCase(lang))
			{
				skills.add(innerObj.get("preferred_label").toString() + " (" + innerObj.get("id").toString() + ")");
			}
		}
		OCs.get(l).setSkills(skills);
	}

	private static void getSoftSkills(String occupationId, int l) throws Exception
	{

		String url = baseUrl
			+ "concept_relation_by_id/?relation=softskills&concept_id="
			+ occupationId
			+ "&include_descendants=true&branch=softskill&format="
			+ format
			+ "&lang="
			+ lang;
		System.out.println("to get softskills: " + url);
		List<String> softSkills = new ArrayList<String>();

		String results = sendPostAPI(url);
		if (results == null)
		{
			return;
		}
		results = "{\"results\": " + results + "}";

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject)jsonParser.parse(results);
		JSONArray langA = (JSONArray)jsonObject.get("results");

		Iterator i = langA.iterator();
		// fetch the correct ID
		while (i.hasNext())
		{
			JSONObject innerObj = (JSONObject)i.next();
			if (innerObj.get("preferred_label_lang").toString().equalsIgnoreCase(lang))
			{
				softSkills.add(innerObj.get("preferred_label").toString() + " (" + innerObj.get("id").toString() + ")");
			}
		}
		OCs.get(l).setSoftSkills(softSkills);
	}

	/* Print Occupations in plain text ***********************/
	public static void printOccupations()
	{
		for (int s = 0; OCs != null && s < OCs.size(); s++)
		{
			System.out.print(OCs.get(s).num + 1 + ". " + OCs.get(s).id + ": " + OCs.get(s).concept);

			if (OCs.get(s).descr != null)
			{
				System.out.print(": ");
			}
			for (int m = 0; OCs.get(s).descr != null && m < OCs.get(s).descr.size(); m++)
			{
				System.out.print(OCs.get(s).descr.get(m));
			}

			System.out.println("\n\tOC: " + OCs.get(s).OC + "\n\tScore: " + OCs.get(s).score + ": " + OCs.get(s).match);

			if (OCs.get(s).labels != null)
			{
				System.out.println("\tLabels: ");
			}
			for (int m = 0; OCs.get(s).labels != null && m < OCs.get(s).labels.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).labels.get(m));
			}

			if (OCs.get(s).classes != null)
			{
				System.out.println("\tClasses: ");
			}
			for (int m = 0; OCs.get(s).classes != null && m < OCs.get(s).classes.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).classes.get(m));
			}

			if (OCs.get(s).synonyms != null)
			{
				System.out.println("\tSynonyms: ");
			}
			for (int m = 0; OCs.get(s).synonyms != null && m < OCs.get(s).synonyms.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).synonyms.get(m));
			}

			if (OCs.get(s).similars != null)
			{
				System.out.println("\tSimilars: ");
			}
			for (int m = 0; OCs.get(s).similars != null && m < OCs.get(s).similars.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).similars.get(m));
			}

			if (OCs.get(s).parents != null)
			{
				System.out.println("\tparents: ");
			}
			for (int m = 0; OCs.get(s).parents != null && m < OCs.get(s).parents.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).parents.get(m));
			}

			if (OCs.get(s).childs != null)
			{
				System.out.println("\tchilds: ");
			}
			for (int m = 0; OCs.get(s).childs != null && m < OCs.get(s).childs.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).childs.get(m));
			}

			if (OCs.get(s).others != null)
			{
				System.out.println("\tothers: ");
			}
			for (int m = 0; OCs.get(s).others != null && m < OCs.get(s).others.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).others.get(m));
			}

			if (OCs.get(s).skills != null)
			{
				System.out.println("\tskills: ");
			}
			for (int m = 0; OCs.get(s).skills != null && m < OCs.get(s).skills.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).skills.get(m));
			}

			if (OCs.get(s).softskills != null)
			{
				System.out.println("\tsoftskills: ");
			}
			for (int m = 0; OCs.get(s).softskills != null && m < OCs.get(s).softskills.size(); m++)
			{
				System.out.println("\t - " + OCs.get(s).softskills.get(m));
			}
		}
	}

	/* Print Occupations in a given format ***********************/
	public static void saveOccupations(String title)
	{
		System.out.println("<tr><td><center>" + title + "</td>");
		String temp = "";
		String buffer = "";

		for (int s = 0; OCs != null && s < OCs.size(); s++)
		{
			temp = "";
			if (OCs.get(s).score >= 1.0)
			{
				temp = "#CCFFCC";
			}
			if (title.equalsIgnoreCase(OCs.get(s).concept) && OCs.get(s).score >= 1.0)
			{
				temp = "#99FF99";
			}

			buffer = "<td bgcolor=#bgcolor#> Id: "
				+ OCs.get(s).id
				+ "<br> Concept: "
				+ OCs.get(s).concept
				+ "<br> OC: "
				+ OCs.get(s).OC
				+ "<br> Score: "
				+ OCs.get(s).score
				+ ": "
				+ OCs.get(s).match
				+ "\n";

			if (OCs.get(s).labels != null)
			{
				buffer += "<br>Labels: <br>";
			}
			for (int m = 0; OCs.get(s).labels != null && m < OCs.get(s).labels.size(); m++)
			{
				buffer += " - " + OCs.get(s).labels.get(m) + "<br>";
			}

			if (OCs.get(s).classes != null)
			{
				buffer += "<br> Classes: <br>";
			}
			for (int m = 0; OCs.get(s).classes != null && m < OCs.get(s).classes.size(); m++)
			{
				buffer += " - " + OCs.get(s).classes.get(m) + "<br>";
				/*		        	if ((OCs.get(s).classes.get(m)).contains(title))
						temp="#DBB8A6";
				*/ }

			if (buffer.contains(": ASOC"))
			{
				temp = "#B2F0FF";
			}

			if (OCs.get(s).match == 100)
			{
				temp = "#DBB8A6";
			}

			System.out.print(buffer.replace("#bgcolor#", temp) + "</td>");

		}
		System.out.print("</tr>");

	}

	public static void cmpLabelsSynonyms()
	{

		for (int s = 0; OCs != null && s < OCs.size(); s++)
		{
			System.out.println(
				OCs.get(s).num
					+ 1
					+ ". "
					+ OCs.get(s).id
					+ ": "
					+ OCs.get(s).concept
					+ "\n\tOC: "
					+ OCs.get(s).OC
					+ "\n\tScore: "
					+ OCs.get(s).score);

			if (OCs.get(s).labels != null && OCs.get(s).synonyms != null)
			{
				System.out.println("I am in ... " + s);
				for (int m = 0; m < OCs.get(s).labels.size(); m++)
				{
					for (int n = 0; n < OCs.get(s).synonyms.size(); n++)
					{
						if (OCs.get(s).labels.get(m).equalsIgnoreCase(OCs.get(s).synonyms.get(n)))
						{
							System.out
								.println(m + ". " + OCs.get(s).labels.get(m) + "<-- -->" + n + ". " + OCs.get(s).synonyms.get(n));
						}
					}
				}

			}
		}

	}

	public static void cmpLabelsSimilars()
	{

		for (int s = 0; OCs != null && s < OCs.size(); s++)
		{
			System.out.println(
				OCs.get(s).num
					+ 1
					+ ". "
					+ OCs.get(s).id
					+ ": "
					+ OCs.get(s).concept
					+ "\n\tOC: "
					+ OCs.get(s).OC
					+ "\n\tScore: "
					+ OCs.get(s).score);

			if (OCs.get(s).labels != null && OCs.get(s).similars != null)
			{
				for (int m = 0; m < OCs.get(s).labels.size(); m++)
				{
					for (int n = 0; n < OCs.get(s).similars.size(); n++)
					{
						if (OCs.get(s).labels.get(m).equalsIgnoreCase(OCs.get(s).similars.get(n)))
						{
							System.out
								.println(m + ". " + OCs.get(s).labels.get(m) + "<-- -->" + n + ". " + OCs.get(s).similars.get(n));
						}
					}
				}

			}
		}

	}

	public static void cmpSkillsSoftSkills()
	{

		for (int s = 0; OCs != null && s < OCs.size(); s++)
		{
			System.out.println(
				OCs.get(s).num
					+ 1
					+ ". "
					+ OCs.get(s).id
					+ ": "
					+ OCs.get(s).concept
					+ "\n\tOC: "
					+ OCs.get(s).OC
					+ "\n\tScore: "
					+ OCs.get(s).score);

			if (OCs.get(s).skills != null && OCs.get(s).softskills != null)
			{
				for (int m = 0; m < OCs.get(s).skills.size(); m++)
				{
					for (int n = 0; n < OCs.get(s).softskills.size(); n++)
					{
						if (OCs.get(s).skills.get(m).equalsIgnoreCase(OCs.get(s).softskills.get(n)))
						{
							System.out
								.println(m + ". " + OCs.get(s).skills.get(m) + "<-- -->" + n + ". " + OCs.get(s).softskills.get(n));
						}
					}
				}

			}
		}

	}

	public static void readProperties()
	{
		Properties prop = new Properties();
		InputStream input = null;

		try
		{

			input = new FileInputStream("Janzz.config");

			// load a properties file
			prop.load(input);
			format = prop.getProperty("format");
			lang = prop.getProperty("lang");
			code = prop.getProperty("code");
			maxConcepts = Integer.parseInt(prop.getProperty("maxConcepts"));
			baseUrl = prop.getProperty("baseUrl");
			token = prop.getProperty("token");
			asoc_file = prop.getProperty("asoc_file");
			fullmatch = Double.parseDouble(prop.getProperty("fullmatch"));
			treshold = Double.parseDouble(prop.getProperty("treshold"));
			stopwords_ar = prop.getProperty("stopwords_ar");
			stopwords_en = prop.getProperty("stopwords_en");

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

	/* Loads data generation properties into the system
	 * allows the connection to the database
	 */
	public static void readDBProperties()
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
	public static void createASOC()
	{

		try
		{
			FileInputStream file = new FileInputStream(new File(asoc_file));

			//Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			//Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			int i = 0;
			while (rowIterator.hasNext())
			{
				//rowIterator.next();
				Row row = rowIterator.next();
				//For each row, iterate through all the columns
				//if ( row.getCell(0).getCellType() == 1)  // String 
				if (lang.equalsIgnoreCase("en"))
				{
					ASOCtitles.put(row.getCell(0).getStringCellValue(), row.getCell(2).getStringCellValue());
				}
				if (lang.equalsIgnoreCase("ar"))
				{
					ASOCtitles.put(row.getCell(0).getStringCellValue(), row.getCell(3).getStringCellValue());
				}

			}
			file.close();
			workbook.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static String stopwords(String title)
	{

		String REGEX = "";
		String INPUT = title;
		String REPLACE = "";
		StringTokenizer st = new StringTokenizer(stopwords_en);
		List<String> words = new ArrayList<String>();
		while (st.hasMoreTokens())
		{
			words.add(st.nextToken().replace("_", " "));
		}
		Collections.sort(words, new comp());
		//Collections.reverse(words);
		for (int i = 0; i < words.size(); i++)
		{
			REGEX = words.get(i);
			//System.out.println(REGEX);
			Pattern p = Pattern.compile("\\b" + REGEX + "\\b", Pattern.CASE_INSENSITIVE);
			// get a matcher object
			Matcher m = p.matcher(INPUT);
			INPUT = m.replaceAll(REPLACE);
			//System.out.println(INPUT);
		}

		//System.out.println(INPUT.trim());
		return INPUT.trim();
	}

	public static void cleanJobTitle() throws IOException
	{
		List<String> words = new ArrayList<String>();
		words.add(" ال");
		Collections.sort(words, new comp());

		FileInputStream file = new FileInputStream(
			new File("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\HRDF-ASOC\\Mapping Template for WCC_ammar.xlsx"));

		XSSFWorkbook workbook = new XSSFWorkbook(file);
		XSSFSheet sheet = workbook.getSheetAt(0);
		Iterator<Row> rowIterator = sheet.iterator();
		rowIterator.next();
		while (rowIterator.hasNext())
		{
			Row row = rowIterator.next();
			if (row.getCell(2) == null)
				continue;
			title = row.getCell(2).getStringCellValue();
			System.out.print(title);
			//title = title.replaceAll(" ", "_");
			title = " " + title;
			for (int i = 0; i < words.size(); i++)
				title = title.replaceAll(words.get(i)," ");
			System.out.println("\t" + title.replaceAll("_", " ").trim());
		}
		System.out.println(new Date());
		file.close();
		workbook.close();
	}
	

	public static String cleanJobTitle_clitics(String title) throws IOException
	{
		List<String> words = new ArrayList<String>();
		words.add("ُ");words.add("َ");words.add("ِ");words.add("ٌ");words.add("ٌّ");words.add("ُّ");words.add("ًّ");words.add("َّ");words.add("ً");
		words.add("ٍ");words.add("ْ");words.add("ِّ");words.add("ٍّ");

		//First step: clean the clitics
		for (int i = 0; i < words.size(); i++)
			title = title.replaceAll(words.get(i),"");
			
		//Second step: clean the 'the ال'
   	 	if (!title.substring(0, 2).equalsIgnoreCase("ال"))
   	 		return title;
   	 	
		StringTokenizer st = new StringTokenizer(title);
		String word;
		int i= 1;
		while (st.hasMoreTokens() && i<=2) {
	    	 word = st.nextToken();
	    	 if (word.substring(0, 2).equalsIgnoreCase("ال"))
		    	 if (word.length()>4)
		    		 title = title.replace(word, word.substring(2, word.length()));
	    	 i++;
	     }
				
		return title.trim();
	}
	

	public static void checkJobTitleASOC3() 
	{
		try
		{
		List<String> words = new ArrayList<String>();
		words.add(" ال");
		Collections.sort(words, new comp());
		Connection conn = null;
		Statement stmt2,stmt = null;
		ResultSet rs, rs2;
		String query="";
		Class.forName(JDBC_DRIVER);

		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		
		FileInputStream file = new FileInputStream(
			new File("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\HRDF-ASOC\\Mapping Template for WCC_ammar.xlsx"));

		XSSFWorkbook workbook = new XSSFWorkbook(file);
		XSSFSheet sheet = workbook.getSheetAt(0);
		Iterator<Row> rowIterator = sheet.iterator();
		rowIterator.next();
		while (rowIterator.hasNext())
		{
			Row row = rowIterator.next();
			if (row.getCell(2) == null)
				continue;
			title = row.getCell(2).getStringCellValue();
			System.out.print(title);
			//title = title.replaceAll(" ", "_");
			query = "select * from asoc.occupation where job_title_ar='" + title + "';";
			//System.out.println(query); //TODO: remove println
			rs = stmt.executeQuery(query);
			if (rs.next()){
				System.out.print("\tInASOC\t" + rs.getString("job_title_ar"));
				//query = "select job_title_ar from asoc.occupation where job_title_ar='" + title + "' and occup_code in (select isco_code from asoc.occupation_classif_new where language = 'ar')";
				query = "select * from asoc.occupation_classif_new where isco_code='" + rs.getString("occup_code") + "' and language = 'ar'";
				//System.out.println(query); //TODO: remove println
				rs2 = stmt2.executeQuery(query);
				if (rs2.next())
					System.out.println("\tmapped");
				else
					System.out.println("\tnot mapped");
					
			}
			else {
					System.out.println("\tnot in ASOC\t" + title + "\tnot mapped\t" );
			}
			rs.close();
				
		}
		System.out.println(new Date());
		file.close();
		workbook.close();
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
	}
	
	public static void checkJobTitleMOL3() 
	{
		try
		{
		List<String> words = new ArrayList<String>();
		words.add(" ال");
		Collections.sort(words, new comp());
		Connection conn = null;
		Statement stmt2,stmt = null;
		ResultSet rs, rs2;
		String query="";
		Class.forName(JDBC_DRIVER);

		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		
		FileInputStream file = new FileInputStream(
			new File("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\HRDF-ASOC\\Mapping Template for WCC_ammar.xlsx"));

		XSSFWorkbook workbook = new XSSFWorkbook(file);
		XSSFSheet sheet = workbook.getSheetAt(0);
		Iterator<Row> rowIterator = sheet.iterator();
		rowIterator.next();
		while (rowIterator.hasNext())
		{
			Row row = rowIterator.next();
			if (row.getCell(2) == null)
				continue;
			title = row.getCell(2).getStringCellValue();
			System.out.print(title);
			//title = title.replaceAll(" ", "_");
			query = "select * from taxonomies.occupations_ar where job_title_ar='" + title + "';";
			//System.out.println(query); //TODO: remove println
			rs = stmt.executeQuery(query);
			if (rs.next()){
				System.out.print("\tInMOL\t" + rs.getString("job_title_ar"));
				//query = "select job_title_ar from asoc.occupation where job_title_ar='" + title + "' and occup_code in (select isco_code from asoc.occupation_classif_new where language = 'ar')";
				query = "select * from asoc.occupation_classif_new where isco_code='" + rs.getString("isco_code") + "' and language = 'ar'";
				//System.out.println(query); //TODO: remove println
				rs2 = stmt2.executeQuery(query);
				if (rs2.next())
					System.out.println("\tmapped");
				else
					System.out.println("\tnot mapped");
					
			}
			else {
					System.out.println("\tnot in MOL\t" + title + "\tnot mapped\t" );
			}
			rs.close();
				
		}
		System.out.println(new Date());
		file.close();
		workbook.close();
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
	}
	

	public static void ExtractJobExamples()
	{ //synonyms and similars from Janzz
		Connection conn = null;
		Statement stmt = null;
		Statement stmtS = null;
		String query = "";
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection("jdbc:mysql://localhost/escoskos", "root", "");
			stmt = conn.createStatement();
			stmtS = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs;
			rs = stmt.executeQuery("SELECT ConceptURI, ConceptPT FROM occupations where ConceptType='OC' limit 4733, 1000");

			int i = 1;
			String ocId, skill;
			while (rs.next())
			{
				ocId = rs.getString(1);
				skill = rs.getString(2);
				System.out.println(i + ". " + ocId + ": " + skill);
				OCs.add(new Occupation(0, ocId, skill, "", 0.0));
				getSimilarJobs(skill, lang);
				for (int m = 0; OCs.get(0).synonyms != null && m < OCs.get(0).synonyms.size(); m++)
				{
					query = "insert into escoskos.occupation_synonyms values ('"
						+ ocId
						+ "', \""
						+ OCs.get(0).synonyms.get(m)
						+ "\")";
					System.out.println("\t - " + query);
					stmtS.executeUpdate(query);
				}

				for (int m = 0; OCs.get(0).similars != null && m < OCs.get(0).similars.size(); m++)
				{
					query = "insert into escoskos.occupation_similars values ('"
						+ ocId
						+ "', \""
						+ OCs.get(0).similars.get(m)
						+ "\")";
					System.out.println("\t - " + query);
					stmtS.executeUpdate(query);
				}

				OCs.clear();
				i++;
				//printOccupations();
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
	}//end function

	public static void ExtractJobExamples_ar()
	{ //synonyms and similars from Janzz
		Connection conn = null;
		Statement stmt = null;
		Statement stmtS = null;
		String query = "";
		Date date = new Date();
		System.out.println("Generating Jobs data:");
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager
				.getConnection("jdbc:mysql://localhost/taxonomies?useUnicode=true&characterEncoding=utf-8", "root", "");
			//conn.setClientInfo("useUnicode", "yes");
			//conn.setClientInfo("characterEncoding", "utf8");
			stmt = conn.createStatement();
			stmtS = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs;
			//rs = stmt.executeQuery("SELECT mol_id, isco_code, occup_title, occup_title_en FROM taxonomies.occupations_ar where occup_title_en='Guard'");
			rs = stmt.executeQuery(
				"SELECT mol_id, isco_code, occup_title, occup_title_en FROM taxonomies.occupations_ar limit 2196, 1000");

			int i = 1;
			int j = 0;
			while (rs.next())
			{
				System.out.print(i + ". " + rs.getString(1) + ": " + rs.getString(3));
				OCs.add(new Occupation(0, rs.getString(3), rs.getString(4), "", 0.0));
				getSimilarJobs(rs.getString(3).trim(), "ar");
				for (int m = 0; OCs.get(0).synonyms != null && m < OCs.get(0).synonyms.size(); m++)
				{
					query = "insert into taxonomies.occupations_ar_synonyms values ('"
						+ rs.getString(1)
						+ "', '"
						+ rs.getString(2)
						+ "', \""
						+ rs.getString(3)
						+ "\", \""
						+ OCs.get(0).synonyms.get(m).replaceAll("\"", "")
						+ "\", 'ar')";
					System.out.print(" .");
					//System.out.println ("\t - " + query);
					stmtS.executeUpdate(query);
					j++;
				}

				for (int m = 0; OCs.get(0).similars != null && m < OCs.get(0).similars.size(); m++)
				{
					query = "insert into taxonomies.occupations_ar_similars values ('"
						+ rs.getString(1)
						+ "', '"
						+ rs.getString(2)
						+ "', \""
						+ rs.getString(3)
						+ "\", \""
						+ OCs.get(0).similars.get(m)
						+ "\", 'ar')";
					System.out.print(" .");
					//System.out.println ("\t - " + query);
					stmtS.executeUpdate(query);
					j++;
				}

				OCs.clear();
				System.out.print("\n" + i + ". " + rs.getString(1) + ": " + rs.getString(4));
				OCs.add(new Occupation(0, rs.getString(3), rs.getString(2), "", 0.0));
				getSimilarJobs(rs.getString(4).trim(), "en");
				for (int m = 0; OCs.get(0).synonyms != null && m < OCs.get(0).synonyms.size(); m++)
				{
					query = "insert into taxonomies.occupations_ar_synonyms values ('"
						+ rs.getString(1)
						+ "', '"
						+ rs.getString(2)
						+ "', \""
						+ rs.getString(4)
						+ "\", \""
						+ OCs.get(0).synonyms.get(m)
						+ "\", 'en')";
					System.out.print(" .");
					//System.out.println ("\t - " + query);
					stmtS.executeUpdate(query);
					j++;
				}

				for (int m = 0; OCs.get(0).similars != null && m < OCs.get(0).similars.size(); m++)
				{
					query = "insert into taxonomies.occupations_ar_similars values ('"
						+ rs.getString(1)
						+ "', '"
						+ rs.getString(2)
						+ "', \""
						+ rs.getString(4)
						+ "\", \""
						+ OCs.get(0).similars.get(m)
						+ "\", 'en')";
					System.out.print(" .");
					//System.out.println ("\t - " + query);
					stmtS.executeUpdate(query);
					j++;
				}

				OCs.clear();
				i++;
				//printOccupations();
			}

			System.out.println("\nGenerating Jobs data:  Start at: \t" + date);
			System.out.println("Generating Jobs data:  Finished at: \t" + new Date());
			System.out.println("\t--> " + j + " Synonyms and similar jobs have been created");
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
	}//end function
	
	
	public static void generateJobEducation()
	{ //synonyms and similars from Janzz
		Connection conn = null;
		Statement stmt = null;
		Statement stmt2 = null;
		Statement stmt3 = null, stmt4 = null;
		String query = "";
		Date date = new Date();
		System.out.println("Generating Job Education data:");
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager
				.getConnection("jdbc:mysql://localhost/taxonomies?useUnicode=true&characterEncoding=utf-8", "root", "");
			//conn.setClientInfo("useUnicode", "yes");
			//conn.setClientInfo("characterEncoding", "utf8");
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmt3 = conn.createStatement();
			stmt4 = conn.createStatement();
			ResultSet rs, rs2, rs3;

			// JDBC2Elise data type mappings

			rs = stmt.executeQuery(
				"SELECT candidate_id, job_title, left(occupation,6) occupation, sector, 'en' language FROM employment.candidate_ambitions");

			int i = 1;
			int j = 0;
			int idx = 0, idx2=0;
			String update="";
			Random rn = new Random();
			while (rs.next())
			{
				//System.out.println(rs.getString(1) + ": " + rs.getString(2));
				update = "insert into employment.candidate_education (candidate_id, education_level, education_area, education_field) values (\"" + rs.getString(1) + "\",\"";
				query = "SELECT oe.job_code, e.edu_code, e.edu_title_" + rs.getString("language") + " FROM asoc.occupation_education oe, asoc.education e where oe.edu_code=e.edu_code"
					+ " and oe.job_code=\"" + rs.getString(3) + "\"";
				//System.out.println(query);
				rs2 = stmt2.executeQuery(query);

				while (rs2.next())
				{
					idx = 0;
					//System.out.println("\t- " + rs2.getString(2) + ": " + rs2.getString(3));
					update += rs2.getString(3) + "\",\"";
					
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
							update += rs3.getString("disc_title_" + rs.getString("language")) + "\",\"" + rs3.getString("field_title_" + rs.getString("language")) + "\")";
					}
					if (idx2<=0)
						update += "\",\"\")";
					
					//System.out.println(update.substring(116, update.length()));
					System.out.println(update);
					stmt4.executeUpdate(update);

				}

			}

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

}