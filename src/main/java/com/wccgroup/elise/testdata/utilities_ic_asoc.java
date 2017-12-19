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

public class utilities_ic_asoc
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static String stopWords = "#في#عن#جي#دي#ما#على#إلى#فوق#قبل#و#من#/#-##أو#مع#لها#أي##ومن#وما#وفق#بهم#بها##بتلك#باستثناء#بعد##SQL#Hvac#"; // prepositions and prefixes 
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		readProperties(); //load dataGerator properties
		String input = "";
		ArrayList<String> options = new ArrayList<>();
		options.add("1");
		options.add("2");
		options.add("6");
		
		while (true)
		{
			System.out.printf("Please select the action to perform from the following:\n");
			System.out.println("\t- 1- Tockenize ASOC Occupations");
			System.out.println("\t- 2- Generate JSON word clusters");
			System.out.println("\t- 3- Quit");
			java.io.BufferedReader r = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
			input = r.readLine();
			if (options.contains(input))
				break;
		}
		switch (input)
		{
		case "1":
			//tokenizeJobTitles("asoc3.wordcluster", "SELECT distinct job_code, job_title_ar FROM asoc3.job;");
			tokenizeJobTitles_csv("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\niri\\IC\\arabic_niri\\occupations_ic.csv");
			break;
		case "2":
			generateTaxonomyData("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\niri\\IC\\arabic\\asoc3\\", "SELECT distinct job_code, job_title_ar FROM asoc3.job");
			generateWordClusters("\\\\savannah\\home\\abenabdelkader\\Documents\\projects\\niri\\IC\\arabic\\asoc3\\", "SELECT distinct concept FROM asoc3.wordcluster;");
			break;
		default:
			System.out.println("Bye"); // quit

		}

	}
	// process Alternative titles and hidden titles
	public static void tokenizeJobTitles(String sqlTable, String query) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		Statement stmt2 = conn.createStatement();
		stmt.executeUpdate("truncate table " + sqlTable ); //esco2017.occupation");
		ResultSet rs = stmt.executeQuery(query ); 
		int Id=0;
		StringBuilder queryString = new StringBuilder();
		queryString.append("insert into " + sqlTable + " values ");
		String gender="U";
		while (rs.next()) {
			StringTokenizer st = new StringTokenizer(rs.getString(2), " ");
			//queryString.append("insert into " + sqlTable + " values ");
			while (st.hasMoreTokens()) {
				gender = "U";
				String temp = st.nextToken().replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", "").replaceAll("/", "").trim();
				if (temp.startsWith("وال"))
				{
					temp = temp.substring("وال".length());
					//System.out.println(Id + "\t- " + temp);
				}
				if (temp.startsWith("ال"))
				{
					temp = temp.substring("ال".length());
				}
				if (temp.startsWith("لل"))
				{
					temp = temp.substring("لل".length());
				}
				if (!stopWords.contains("#" + temp + "#"))
					queryString.append("("+ rs.getString(1) + ",\"" + temp + "\",'" + gender + "'),");
			}
			if (Id%200 == 0) {
				System.out.println(Id + "\t- " + queryString.substring(0, queryString.length()-1));
				stmt2.executeUpdate(queryString.substring(0, queryString.length()-1));
				queryString = new StringBuilder();
				queryString.append("insert into " + sqlTable + " values ");
			}
			Id++;
		}
		stmt2.executeUpdate(queryString.substring(0, queryString.length()-1));
		rs.close();
		stmt.close();
		stmt2.close();
		conn.close();
	}

	// tokenize job titles using csv file as input
	public static void tokenizeJobTitles_csv(String filepath) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
		//stmt.executeUpdate("truncate table asoc3.occupation_ic");
		stmt.executeUpdate("truncate table asoc3.wordcluster");
		StringBuilder queryString = new StringBuilder();
		queryString.append("insert into asoc3.wordcluster values ");
		try(BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			String line = br.readLine();
			line = br.readLine();
			int Id = 0;
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "\t");
				System.out.println(line);
				String code = st.nextToken();
				String title = st.nextToken();
				//stmt.executeUpdate("insert into asoc3.occupation_ic values (\'" + code + "',\"" + title +  "\",\"" + st.nextToken() + "\")");
				StringTokenizer words = new StringTokenizer(title, " ");
				while (words.hasMoreTokens()) {
					String temp = words.nextToken().replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", "").replaceAll("/", "").trim();
					if (temp.startsWith("وال"))
					{
						temp = temp.substring("وال".length());
					}
					if (temp.startsWith("ال"))
					{
						temp = temp.substring("ال".length());
					}
					if (temp.startsWith("لل"))
					{
						temp = temp.substring("لل".length());
					}
					if (temp.startsWith("بال"))
					{
						temp = temp.substring("بال".length());
					}
					if (!stopWords.contains("#" + temp + "#"))
						queryString.append("('"+ code + "','" + temp + "'),");
				}
				//System.out.println(Id + "\t- " + queryString.substring(0, queryString.length()-1));
				//stmt.executeUpdate(queryString.substring(0, queryString.length()-1));
				Id++;
				
				//queryString.append("),");
				if (Id%100 == 0) {
					System.out.println(Id + "\t- " + queryString.substring(0, queryString.length()-1));
					stmt.executeUpdate(queryString.substring(0, queryString.length()-1));
					queryString = new StringBuilder();
					queryString.append("insert into asoc3.wordcluster values ");
				}
				
				line = br.readLine();
			}
		}
		//remove prefix and 'و'
		stmt.executeUpdate("update asoc3.wordcluster set concept=substr(concept, 2, length(concept)-1) where concept in ('وكبار','ومشايخ','وتجارة','وعلوم','وخبراء','ومزارع','ومهندسو','وأخصائيّو','وحركة','وتركيب','ومستشارو','وتطوير','ومحلّلو','وشبكات','وعلم','وتقديم','وغيرهم','ومصمِمو','ومنتجو','ووسائل','ومحطة','ومراقبو','ومساعدو','وأسنان','ووسطاء','ومقدرو','ومقاولو','ومديرو','وكالات','ومخبرو','وقادة','ودعم','ونظم','ومشغّلو','ومحصّلو','وعاملو','وكتبة','وإجراء','ومسك','ونقل','وساعو','وحفظ','ومرشدو','ومضيفو','وأخصّائيو','وبائعو','ومزارعو','ومربو','وصائدو','وناصبو','وصيادو','وجامعو','ومواشي','وقاطعوا','ونقش','وتشطيب','ومركِّبو','وتنظيف','وصانعو','وصنع','وربط','وحدّادو','وإعداد','وجلخ','وشحذ','ومصلّحو','وتصليح','وضبط','وقطع','وتجليخ','وتصليحها','وأجهزة','ومصنّفو','وتصنيف','ومكافحة','ومصانع','وطلاء','وتغطية','وآلات','ومنتجات','ورقية','وتصنيع','وطباعة','وتبديل','وتربية','وجبات','ومواد','وعمّال','وظائف','ومحصلو','وحدة','وساطة','وسلامة','وإعلان','وخدمات','وكالة','ومراقبة','ومشروبات','وراثة','وإنتاجي','وتهوية','وتكييف','وغاز','واستغلال','وجدولة','وتحكم','وتوزيع','وتوليد','وأذن','وحنجرة','وتكميلي','وأعمال','ودراسات','واستحواذ','وقروية','ووسائط','ومساجد','وإذاعي','ومحاجر','وطبية','ورياضيات','ومرئيات','ونسخ','وتدبير','وشجيرات','ومربي','وصب','ومركّب','ومجلّخ','وضابط','ومعادن','وخيزران','وثائق','وتجليد','وخضروات','ومشغّل','وصانع','ومقيّم','وبثق','ونسيج','وخضراوات','وأوراق','وزيوت','وأسماك','ومراجل','ومعدنية','ومركبة','ونش','وكيّ','وحيوانات','وبستنة','وتفريغ','وفيات','وترخيص','ومعرفى','وجمعية','وصناعية','وموازنة','ومحاسبية','وممتلكات','وإعادة','وشؤون','وعمال','وظيفي','وصيانة','وتجارية','ومرافق','وتصدير','وبحوث','وتسويق','ومبيعات','وترويج','ومعالجة','ونشر','وصرف','وإمداد','وجسور','ومنسق','ولاسلكية','وتعلّم','ورش','ووكالة','ورياضة','وإنماء','وحراسة','وسكانية','وراثية','وعقاقير','وآليات','وتصميم','وقاية','وصحة','وقائية','وتقنية','وتخطيط','ومواصفات','وضمان','وتوصيل','وتليفزيون','ورادار','ومشاريع','ومحقق','وقولون','وأعصاب','وولادة','وأنف','ومستقيم','ولادة','وأمراض','وطفولة','وسموم','وجودة','وبائية','وتأهيل','وأدوات','وإدارة','وبيئة','وغابات','ومصرفية','وقبالة','وتمريض','وسائل','وتقويم','وموهبة','وترية','وأجور','ومتوسط','وعقود','وإجراءات','وامتيازات','ومعدات','واتصالات','وتشغيل','ومحفوظات','ومطبوعات','وسلالات','وإمام','ومناهج','ونحّات','ومنتج','وتلفازي','وإذاعية','وتلفزيوني','وبصريات','ومحركات','وحافلات','وتبريد','وتدفئة','ومخططات','وصفيح','ومشرف','وكسارة','ورخام','ومياه','ومجاري','وتجميع','ومراعي','وتشحيم','ومؤتمرات','وعقارات','وتأجير','وإعلانات','وحدود','وتسجيل','وطبي','ومجوهرات','وتحنيطها','وطيور','وتلفاز','وبصرية','ومدقق','ورقي','وعمله','وعملة','ومعلومات','وسندات','واستلام','وتصوير','وشواء','وفاكهة','ومعجنات','ومفارش','وتحف','وأشرطة','وبسط','وجلود','وتقليدية','واستنساخ','وفواكه','ولوازم','وخشب','وأشتال','وأسمدة','ومعدّات','ومزيت','وشاي','وغيرها','وتطعيم','ومتنزهات','ونباتات','وحيواني','وقشريات','ومربِّي','وتلميع','وأرضيات','وشباك','وموكيت','ودهان','وتشييدها','وتثبيت','وشاحنات','وسخانات','وضواغط','وكَيّ','ومقاييس','وفضة','وقاطع','وصاقل','وبورسلين','وتنزيل','وسيراميك','وخزف','وﻃﻠﻲ','وجريده','وسعف','ونوافذ','ومجففّ','وغزل','واستوديو','وتركيبها','وبرق','ومد','وتعليب','ومنظف','ومأكولات','وطاولات','وصلات','وزوايا','وبجامات','ومطاطية','وغربلة','وتخصيب','وصقل','ومواسير','وتخريم','ومزج','وتجفيف','وطحن','وفصل','ومستحضرات','وأسود','وثنيه','وغُزُول','ووَسْم','وعصر','وتكرير','وطحان','وسمك','ولبّ','وتفريز','وتسوية','وتشكيل','وسيارات','وتحريك','وأوناش','وإرساء','وفرز','ودواجن','وبياضات','وصحف');");
		//remove prefix with 'ب'
		stmt.executeUpdate("update asoc3.wordcluster set concept=substr(concept, 2, length(concept)-1) where concept in ('بمراكز','بمياه','بأعماق','بأحكام','بمركز','بشؤون','بمؤسسة','بسوق','بيولوجية','بقطاع','بتنظيم','بمختبر','بشركات','بمحطة','بمكونات','بحل','بثلاث','بمحرك','بولدوزر','بمكواة','بحصان','بأراضي')");
		//remove prefix for 'ل'
		stmt.executeUpdate("update asoc3.wordcluster set concept=substr(concept, 2, length(concept)-1) where concept in ('لأغراض','لشؤون','لسوق','لإدارة','لعمليات','لتقنية','لمحطات','لأعمال','لتقنيات','لأنظمة','لمنتجات','لتكنولوجيا','لخطوط','لمحل','لحجوزات','لواجهات','لسفينة','لساعات','لتحلية','لحفر','لمقاومة','لدائن','لألياف','لمزارع')");
		stmt.close();
		conn.close();
	}


	public static void generateTaxonomyData(String output, String query) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
		    writer = new BufferedWriter(new FileWriter(new File(output+"ArabicTaxonomyData.json")));
			System.out.println( "genetationg " + output+"ArabicTaxonomyData.json");
			writer.write("[{\n\t\t\"version\" : \"" + new Date() + "\",\n\t\t\"id\" : \"activity\",\n\t\t\"name\" : \"Occupations\",\n\t\t\"nodes\" : []\n");
			writer.write("\t}, {\n\t\t\"version\" : \"" + new Date() + "\",\n\t\t\"id\" : \"Occupations\",\n\t\t\"name\" : \"Occupation\",\n\t\t\"nodes\" : [\"");
			
				rs = stmt.executeQuery(query);
				int i = 0;
				for (; rs.next();)
				{
					if (i>0)
						writer.write(", ");
				    writer.write ("{\n\t\t\t\t\"code\" : \"" + rs.getString(1) + "\"," 
						+ "\n\t\t\t\t\"name\" : \"" + rs.getString(2) + "\","  
						+ "\n\t\t\t\t\"relations\" [],"  
						+ "\n\t\t\t\t\"properties\" [],"  
						+ "\n\t\t\t\t\"childNodes\" [],"  
						+ "\n\t\t\t}"
				    );
				    i++;
				}
				writer.write("]\n\t\t\t}\n\t\n]");
				System.out.println("\t" + i + " data objects");
		    writer.close();

	}

	public static void generateWordClusters(String output, String query) throws ClassNotFoundException, SQLException, IOException
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs=null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	    BufferedWriter writer;
	    // A- Relations
		    writer = new BufferedWriter(new FileWriter(new File(output+"ArabicWordClusters.json")));
			System.out.println( "genetationg " + output+"ArabicWordClusters.json");
			writer.write("[");
			
				rs = stmt.executeQuery(query);
				int i = 0;
				for (; rs.next();)
				{
					if (i>0)
						writer.write(", ");
				    writer.write ("\n\t{\"name\" : \"" + rs.getString(1) + "\",\"words\": [" 
						+ "\n\t\t{\"value\" : \"getLemmaFromDic()\",\"wordType\":\"LEMMA\"},"  
						+ "\n\t\t{\"value\" : \"getOtherFormsFromDic()\",\"wordType\":\"OtherForm\"}"  
						+ "\n\t]}"  
				    );
				    i++;
				}
				writer.write("\n]");
				System.out.println("\t" + i + " data objects");
		    writer.close();

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