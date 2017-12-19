/**
 * @author abenabdelkader
 *
 * niriParser.java
 * Dec 17, 2015
 */
package com.wccgroup.elise.testdata_ssoc;

/**
 * @author abenabdelkader
 *
 */
import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

import java.util.*;
import java.util.Date;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.text.SimpleDateFormat;
public class Parser_niriVacancies_HRxml extends DefaultHandler 
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Connection conn = null;
	static Statement stmt = null;
	static String[][] vacancies = {
		{"DocID","0"},
		{"FunctionTitle","1"},
		{"EducationTitle","2"},
		{"EducationLevel","3"},
		{"City","4"},
		{"Postcode","5"},
		{"Country","6"},
		{"EliseDate","7"},
		{"FromDate","8"},
		{"ToDate","9"},
		{"Title","10"},
		{"Organization","11"},
		{"Description","12"},
		};
	static String[][] education = {
		{"Name",""},
		{"StartDate",""},
		{"EndDate",""},
		{"Level",""},
		{"area",""},
		};
	static Map<String, Boolean> elmtStaus = new HashMap<String, Boolean>();
	static int counter;
	static String FullName;
	static String cv_id;
	static Map<String, String> skills = new HashMap<String, String>();
	static Map<String, String> responsibilities = new HashMap<String, String>();
	static Map<String, String> jobCategory = new HashMap<String, String>();

	static StringBuilder vacancy = new StringBuilder();
	static StringBuilder competencies = new StringBuilder();
	static StringBuilder educations = new StringBuilder();
	static String[][] shifttype = new String [1670][6];
	static String[] districts = new String [28];
	static Map<String, Integer> salary_scale = new HashMap<String, Integer>();
	static String outputFile = "";
	static String tmp = "";
	static BufferedWriter writer;
	static int i=1;
	static Random rn = new Random();
	static Random rn2 = new Random();
	static Random rn3 = new Random();
	static Random rn4 = new Random();
	static String code, occupation, score, description;
	static BufferedWriter writer2;
	static public void main(String[] args)  throws Exception {
		String path = "C:\\data\\singapore\\vacancies\\jobs_Ief";
		readProperties();
		//parseVacancies (path);
		enrichVacancies (path);
	}
	
	
	
	static public void parseVacancies (String path) throws Exception
	{
		String filename = null;

		for (int i=0; i<vacancies.length; i++)
			elmtStaus.put(vacancies[i][0], false);
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT case when full_part_time='FULLT' then 'Full Time' else 'Part Time' end, left(job_schedule,5), right(job_schedule,5), job_hours*5, case when Convert(left(job_schedule,2),UNSIGNED INTEGER) <=12 then 'Daily' else 'Nightly' end, job_contract_type   FROM emp_dubai.job");
			int i=0;
			while (rs.next()) {
				shifttype[i][0] = rs.getString(1);
				shifttype[i][1] = rs.getString(2);
				shifttype[i][2] = rs.getString(3);
				shifttype[i][3] = rs.getString(4);
				shifttype[i][4] = rs.getString(5);
				shifttype[i][5] = rs.getString(6);
				i++;
				
			}

			rs = stmt.executeQuery("SELECT cities FROM lssoc.districts");
			i=0;
			while (rs.next()) {
				districts[i] = rs.getString(1);
				i++;
				
			}

			rs = stmt.executeQuery("SELECT distinct left(job_code,4), convert(avg(salary/2.70)*12,UNSIGNED INTEGER) FROM emp_dubai.job group by left(job_code,4)");
			i=0;
			while (rs.next()) {
				salary_scale.put(rs.getString(1), rs.getInt(2));
				i++;
				
			}

			rs = stmt.executeQuery("SELECT distinct code, name FROM lssoc.occupation where length(code)=4");
			i=0;
			while (rs.next()) {
				jobCategory.put(rs.getString(1), rs.getString(2));
				i++;
				
			}

		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setNamespaceAware(true);
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		xmlReader.setContentHandler(new Parser_niriVacancies_HRxml());
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();

	    for (i = 0; i < listOfFiles.length ; i++) {
		      if (listOfFiles[i].isFile()) {
		    	  filename = listOfFiles[i].getName();
		        System.out.println(i + ": " + filename);
		        UserHandler userhandler = new UserHandler();
		        //outputFile = path + "_parsed\\" + filename.substring(0, filename.length()-4);
		        outputFile = path + "_parsed\\" + filename;
		        System.out.println("Parsing File: " + outputFile);
		        if (!filename.contains("#")) {
					writer = new BufferedWriter(new FileWriter(new File(outputFile)));
			        //saxParser.parse(convertToFileURL(path + "\\" + filename), userhandler); 
			        saxParser.parse(convertToFileURL(path + "\\" + filename), userhandler); 
			        writer.close();
		        }
		      }
		    }


		stmt.close();
		conn.close();

	}
	catch (SQLException se)
	{
		//Handle errors for JDBC
		se.printStackTrace();
	}
	}
	public static void enrichVacancies(String path) 
	{
		try
		{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
	        //System.out.print(path + "_parsed");
			File folder = new File(path + "_parsed");
			File[] listOfFiles = folder.listFiles();
			int i;
			String filename = null;
			Date date = new Date();
			writer2 = new BufferedWriter(new FileWriter(new File(path + "\\mapping_table.txt")));
			writer2.write("job title\toccupation code\toccupation title\tscore\n");

		    for (i = 0; i < listOfFiles.length; i++) {
			      if (listOfFiles[i].isFile()) {
			  		date = new Date();
			    	filename = listOfFiles[i].getName();
			    	
			        System.out.print((i) + ": " + filename);

					enrichVacancy(path,filename);
			        System.out.println("\tenriched in: " + + (new Date().getTime() - date.getTime())/1000 + "s");
			      }
			    }
		    writer2.close();
			stmt.close();
			conn.close();
		}
		catch (Exception e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		}
		private static class UserHandler extends DefaultHandler {

		boolean SkillNamePersonal = false;
		boolean EducationTitle = false;
		boolean educationTitle = false;
		boolean educationStart = false;
		boolean educationEnd = false;
		boolean educationOrg = false;
		boolean SkillName = false;
		boolean Description = false;
		//boolean Responsibility = false;

		@Override
		public void startElement(String uri, 
			String localName, String qName, Attributes attributes)
				throws SAXException {

			if (qName.equalsIgnoreCase("SkillNamePersonal") || qName.equalsIgnoreCase("SkillNameIT") 
				|| qName.equalsIgnoreCase("SkillNameLanguage") 
				|| qName.equalsIgnoreCase("SkillNameProfessional") || qName.equalsIgnoreCase("SkillNameProfessionalOther") ) {
				SkillNamePersonal = true;
			}
			if (SkillNamePersonal && qName.equalsIgnoreCase("Name")) {
				SkillName = true;
			}
			if (qName.equalsIgnoreCase("EducationTitle")) {
				EducationTitle = true;
			}
/*			if (qName.equalsIgnoreCase("Responsibility")) {
				Responsibility = true;
			}
*/			if (EducationTitle && qName.equalsIgnoreCase("Name")) {
				educationTitle = true;education[0][1]="";
			}
			if (EducationTitle && qName.equalsIgnoreCase("EducationLevel")) {
				educationStart = true;education[3][1]="";
			}
/*			if (EducationTitle && qName.equalsIgnoreCase("FromDate")) {
				educationEnd = true;education[1][1]="";
			}
			if (EducationTitle && qName.equalsIgnoreCase("ToDate")) {
				educationEnd = true;education[2][1]="";
			}
*/			if (EducationTitle && qName.equalsIgnoreCase("IndSciArea")) {
				educationOrg = true;education[4][1]="";
			}
			if (qName.equalsIgnoreCase("Description")) {
				Description = true;
				description="";
			}
			for (int i=0; i<vacancies.length; i++) {
				if (qName.equalsIgnoreCase(vacancies[i][0])) 
					elmtStaus.put(vacancies[i][0], true);
			}
			tmp="";
		}

		@Override
		public void endElement(String uri, 
			String localName, String qName) throws SAXException {
			try
			{
				Date date = new Date();
				GregorianCalendar cal = new GregorianCalendar();
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				cal.setTime(date);
				int idx1=0, idx2 =0, idx3 =0; 
				idx1 = rn.nextInt(31);
				idx2 = rn.nextInt(180-idx1+1);
				idx3 = rn2.nextInt(shifttype.length);
				cal.add(Calendar.DATE, -idx1);


						
				if (qName.equalsIgnoreCase("root")) {
				competencies.append("\t\t<PositionDetail>\n");

				competencies.append("\t\t\t<PhysicalLocation>\n\t\t\t\t<Name>" + vacancies[11][1] + "</Name>\n"
					+ "\t\t\t\t<Area type=\"x:Country\">Singapore</Area>\n" 
					+ "\t\t\t\t<Area type=\"x:Region\">" + districts[rn4.nextInt(28)] + "</Area>\n" 
					+ "\t\t\t</PhysicalLocation>\n");

				getCode(vacancies[1][1]);
				competencies.append("\t\t\t<PositionTitle>" + vacancies[1][1]  + "</PositionTitle>\n");
				//competencies.append("\t\t\t<PositionClassification name=\"" + occupation + "\">" + code + "\"</PositionClassification>\n");
				competencies.append("\t\t\t<PositionSchedule>" + shifttype[idx3][0]  + "</PositionSchedule>\n");
				competencies.append("\t\t\t<Shift shiftPeriod=\"" + shifttype[idx3][4]  + "\">\n");
				competencies.append("\t\t\t\t<Name>Business Hours</Name>\n");
				competencies.append("\t\t\t\t<Hours>" + shifttype[idx3][3]  + "</Hours>\n");
				competencies.append("\t\t\t\t<StartTime>" + shifttype[idx3][1]  + ":00</StartTime>\n");
				competencies.append("\t\t\t\t<EndTime>" + shifttype[idx3][2]  + ":00</EndTime>\n");
				competencies.append("\t\t\t\t<PayTypeHours>Regular</PayTypeHours>\n\t\t\t</Shift>\n");
				for (Map.Entry<String, String> entry : skills.entrySet()) {
					competencies.append("\t\t\t<Competency name=\"" + entry.getKey() + "\"></Competency>\n");
					//competencies.append("\t\t\t\t</CompetencyId>\n");
					//competencies.append("\t\t\t\t<CompetencyWeight>\n");
					//competencies.append("\t\t\t\t\t</NumericValue minValue=\"0\" maxValue=\"100\">\n");
					//competencies.append("\t\t\t\t</CompetencyWeight>\n");
					}
				//System.out.println ("Code: " + code);
				if (code!=null) { 
					String content= enrichSalaryRange(code);
					//System.out.println ("content: " + content);
				if (content!=null)
						competencies.append(content);					
					}
				competencies.append("\t\t</PositionDetail>\n");
				skills = new HashMap<String, String>();
				
				competencies.append("\t\t<FormattedPositionDescription>\n\t\t\t<Value>");
					competencies.append(description + "\n");
				competencies.append("\t\t\t</Value>\n");
				competencies.append("\t\t</FormattedPositionDescription>\n");

/*				competencies.append("\t\t<FormattedPositionDescription>\n\t\t\t<Value>");
				for (Map.Entry<String, String> entry : responsibilities.entrySet()) {
					competencies.append(entry.getKey() + "\n");
					}
				competencies.append("\t\t\t</Value>\n");
				responsibilities = new HashMap<String, String>();
				competencies.append("\t\t</FormattedPositionDescription>\n");

*/
					vacancy.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n\t<PositionOpening>\n");
					vacancy.append("\t\t<PositionRecordInfo>\n\t\t\t<Id>\n");
					vacancy.append("\t\t\t\t<IdValue>" + vacancies[0][1] + "</IdValue>\n\t\t\t</Id>\n");
					if (!vacancies[0][1].contains(".jobstreet."))
						System.out.println("\t\t" + vacancies[0][1]);
					vacancy.append("\t\t\t<Status validFrom=\"" + format.format(cal.getTime()));
					cal.setTime(date);
					cal.add(Calendar.DATE, idx2);
					vacancy.append("\" validTo=\"" + format.format(cal.getTime()) + "\">Active</Status>\n");
					vacancy.append("\t\t</PositionRecordInfo>\n");
					cal.setTime(date);
					cal.add(Calendar.DATE, -(idx1 + rn.nextInt(9)));

					vacancy.append("\t<PositionProfile xml:lang=\"en\">\n"
						+ "\t\t<PositionDateInfo><StartAsSoonAsPossible>true</StartAsSoonAsPossible>\n"
						+ "\t\t\t<StartDate>" + format.format(cal.getTime()) + "</StartDate>\n" 
						+ "\t\t\t<ExpectedEndDate/>\n\t\t</PositionDateInfo>\n");
				
					vacancy.append("\t\t<Organization>\n\t\t\t<OrganizationName>" + vacancies[11][1] + "</OrganizationName>\n"
						//+ "\t\t\t<ContactInfo><ContactMethod><PostalAddress><CountryCode>SG</CountryCode>" 
						//+ "<PostalCode></PostalCode><Region>" + districts[rn4.nextInt(28)] + "</Region><Municipality></Municipality></PostalAddress></ContactMethod>\n\t\t\t</ContactInfo>\n" 
						+ "\t\t</Organization>\n");

					
					vacancy.append(competencies.toString());
					vacancy.append(educations.toString());
					vacancy.append("\t</PositionProfile>\n\t<UserArea>\n");
					vacancy.append("\t\t\t<EducationLevel>\n\t\t\t\t<Value>" + vacancies[2][1]  + "</Value>\n\t\t</EducationLevel>\n");
					vacancy.append("\t\t\t<EmploymentType>\n\t\t\t\t<Value>" + shifttype[idx3][5]  + "</Value>\n\t\t</EmploymentType>\n");

					vacancy.append("\t\t</UserArea>\n");
			            vacancy.append("\t</PositionOpening>");
					writer.write(vacancy.toString());
					vacancy = new StringBuilder();
					competencies = new StringBuilder();
					educations = new StringBuilder();
			}
			}
			catch (IOException | ClassNotFoundException | SQLException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
			if (qName.equalsIgnoreCase("UserArea")) {
				educations.append("\t\t\t<EducationLevel><value>\n\t\t\t\t" + education[0][1] + "</value></EducationLevel>\n");
				educations.append("\t\t\t<EducationSubject><value>\n\t\t\t\t" + education[0][1] + "</value></EducationSubject>\n");
				EducationTitle = false;
			}
			if (qName.equalsIgnoreCase("Description")) {
				Description = false;
			}
		}



		@Override
		public void characters(char ch[], 
			int start, int length) throws SAXException {
			//String tmp="";
			for (int i=0; i<vacancies.length; i++) {
				if (elmtStaus.get(vacancies[i][0])) {
					vacancies[i][1]= new String(ch, start, length);
					elmtStaus.put(vacancies[i][0],false);
				}
			}
			if (SkillName) {
				tmp = new String(ch, start, length);
				skills.put(tmp.replaceAll("\"", "'"), "skill");
				SkillNamePersonal = false;
				SkillName = false;
			}
			
/*			if (Responsibility) {
				tmp = new String(ch, start, length);
				responsibilities.put(tmp.replaceAll("\"", "'"), "resp");
				Responsibility = false;
			}
*/			
			if (Description) {
				description += new String(ch, start, length).replaceAll("&", "&amp;");
			}
			if (EducationTitle) {
				tmp = new String(ch, start, length).replaceAll("\"", "'");
				if (educationTitle) {
					education[0][1]= tmp;
					educationTitle = false;
				}
				if (educationStart) {
					education[1][1]= tmp;
					educationStart = false;
				}
				if (educationEnd) {
					education[2][1]= tmp;
					educationEnd = false;
				}
				if (educationOrg) {
					education[3][1]= tmp;
					educationOrg = false;
				}
			}
			
		}
	}
	private static void enrichVacancy(String path, String filename) throws IOException, ClassNotFoundException, SQLException
	{
		StringBuilder filecontent =  new StringBuilder();
		String stopTitles="#Diploma##:##M##F##debbie@peopleprofilers.com   Consultant##inspire4@achievegroup.asia   Assistant Director##R##environment Usher##Officer##3##Area Manager##Client Relations Executive##Head##PWM  SSS Senior Security Supervisor##R1112755 License No. 13C6305##R1658429 License No. 13C6305##URGENT##workers##$7.50/HOUR TEMP 1-2MONTH UNIFORM PACKER~WORK WITH FRIENDS ~JURONG PIER##$7/HOURTEMP 1-3MONTHS ONLINE PACKER -GREENWICH DRIVE##(ML Coverage)Sales Rep, $3.3k, (Completion Bonus +Allowance) – Till End January##*URGENT* 3 months Temp Admin - Expo *$8/hr (Internship)##[Full-time] PHARMACY CHECKOUT CASHIER ($1400++//44HRS) || CY##015/15/CFEREC/1407/GEFA     Financial Consultant##06C2855##1 Preferable Analyst##6 months Contract Finance - Expense Management ($5000-$6500)##6 Months Contract Finance, Expense Management (Up to $6500)##AB##boss##Business Development Exec - Cantonese Speaking | Well-known MNC | By 16th Oct##C# developers##City Hall##Claims##COMPUTER PACKERS NEEDED##Dennis Ng Account Manager##Digital Consultant##dynamic negotiator##E##Early Childhood - Infant Educarer (Various location available, Up to $2400/mth)##Evaluation##experienced customer##experienced personnel Understudy##Extendable##external##Fab10 Probe Functional Test Engineer##FAE##few Research Officer##FIRE##FMCG##Food##Mergers##Min##Min Diploma##Minimum Diploma##MNC /##MON-FRI ONLY $7.50/HOUR TEMP 1-2 MONTHS IT PACKER~JURONG##Murex Consultant##Nail Saloon Manager##NAS Preferably Manager##Navision Developer##Network##O##Operators @ AMK - rrtg##Paste Technician##Possess##Price##Product##Profilers##RA##Regional L##SPM##SSD Test Engineer##TEMP ACCOUNTS ASST @ WOODLANDS ($10 per hour/5Days/Partial)##UberEats Delivery Partner##Ubi##VP, Development Lead, Core Systems Technology, Technology and Operations##Watch Service Engineer##Wealth Planning Manager/ Top Local Bank##Executive#";
		String title = null;
		File tmpDir = new File(path + "_parsed\\" + filename);
		if (!tmpDir.exists())
			return;
		
		tmpDir = new File(path + "_enriched\\sng" + filename.substring(3, filename.length()-4));
		if (tmpDir.exists()) {
            System.out.print("\talready enriched");
			return;
		}
		
		FileReader fileReader = new FileReader(path + "_parsed\\" + filename);
		//System.out.println("_enriched\\sng" + filename.substring(3, filename.length()-4));
        BufferedReader bufferedReader = new BufferedReader(fileReader);
		//BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path + "_enriched\\" + filename)));
        String line = null;
        //String code = null;
        String enrich = null;

        while((line = bufferedReader.readLine()) != null) {
            //System.out.println(line);
            //writer.write(line);
            filecontent.append(line);
			if (line.contains("<PositionTitle>")) { 
				int index = line.indexOf("<PositionTitle>");
				while (index >= 0) {
					code = occupation=score=null;
					title = line.substring(index+15, line.indexOf("</PositionTitle>", index));
					if (stopTitles.contains("#" + title + "#")) {
						System.out.print("\tSkiped because of bad title : '" + title + "'");
						return;
					}
					
					if (title.trim().length()>2)
						enrich = getCode(title.trim());
					writer2.write(title+"\t" + code + "\t" + occupation + "\t" + score + "\n");
					//System.out.println("\ttitle: " + title + " --> " + enrich);
					if (enrich!=null) {
						//writer.write(enrich);
						filecontent.append("\t\t\t<PositionClassification name=\"" + occupation + "\" score=\"" + score + "\">" + code + "</PositionClassification>\n");
						//code = enrich.substring(enrich.indexOf("id=")+4, enrich.indexOf("\" score"));
					}
					//System.out.println("\tcode: " + " --> " + code);
					if (code!=null) {
						enrich = getEnrichment(code);
					}
					//enrich = getEnrichment(line.substring(12, line.length()-2));
					//System.out.println("\ttitle: " + title + " --> " + (getCode(title)==null?"":getCode(title)));
/*					if (enrich!=null)
						writer.write(enrich);
*/					index = line.indexOf("<PositionTitle>", index+1);
				}
			}  
			
		if (line.contains("<UserArea>") && code!=null) { 
			if (enrich!=null)
				filecontent.append(enrich);
			}
		}  
        bufferedReader.close();
        
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path + "_enriched\\sng" + filename.substring(3, filename.length()-4))));
        writer.write(filecontent.toString());
        writer.close();
	}


	/* Reads the job titles from a taxonomy and mapps them to ontology functions/function_groups */
	public static String getCode(String title) throws SQLException, ClassNotFoundException
	{
			try
			{
				URL url = new URL("http://10.43.2.183:14080/semanticsearch/v1/occupationtitles/text?text=" + title.replaceAll(" ", "%20").trim() + "&sort=sort");
				//URL url = new URL("http://demos.savannah.wcc.nl:14080/semanticsearch/v1/occupationtitles/text?text=" + title.replaceAll(" ", "%20").trim() + "&sort=sort");
			URLConnection yc = url.openConnection();
			BufferedReader in;
				in = new BufferedReader(new InputStreamReader(
					yc.getInputStream()));
			String line;
			//String name="";
			String enrich = "\n<wcc_occupation ";
			//String enrich = "";
			while ((line = in.readLine()) != null) {
				if (line.contains("\"id\" :")){
					code = line.substring(12, line.length()-2);
					enrich += "id=\"" + code + "\"";
					line = in.readLine();
				if (line.contains("\"name\" :"))
					occupation = line.substring(14, line.length()-2);
				line = in.readLine().trim();
				//System.out.println(line);
				if (line.contains("\"score\" :")) {
					score= line.substring(10, line.length());
					enrich += " score=\"" + score + "\">";
				}

				enrich += occupation;

						return enrich + "</wcc_occupation>";
							
				}
			}
			in.close();
			}
			catch (IOException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return null;

	}


	public static String getEnrichment(String code) throws SQLException, ClassNotFoundException
	{
		String content ="\t\t<enriched_content>\n";
		String query = "SELECT distinct ssoc_code, skill, type,relationshipType FROM lssoc.occupation_skills_esco "
			+ "where ssoc_code='" + code + "'"; //relationshipType='essential' and ;
		//System.out.println(query);
		ResultSet rs=stmt.executeQuery(query);
		while (rs.next()) {
			content += "\t\t<wcc_competency>esco_skills;" + rs.getString(2) + ";occupation-" + rs.getString(4) + "-" + rs.getString(3) + "</wcc_competency>\n";
						
		}

		query = "SELECT skill_level FROM lssoc.occupation_skill_level where code='" + code.substring(0, 4) + "'"; 
		rs=stmt.executeQuery(query);
		if (rs.next()) 
			content += "\t\t<wcc_competency_level>" + rs.getString(1) + "</wcc_competency_level>\n";

		query = "SELECT riasec FROM lssoc.occupation_interest_group where code='" + code.substring(0, 4) + "'"; 
		rs=stmt.executeQuery(query);
		if (rs.next()) 
			content += "\t\t<wcc_interest_raisec>" + (rs.getString(1)==null?"":rs.getString(1)) + "</wcc_interest_raisec>\n";

		query = "SELECT distinct b.ssoc_code, concat(element_id,'.', category) education, CONVERT(avg(data_value),UNSIGNED INTEGER) score "
			+ "FROM onet.education_training_experience a, lssoc.ssoc2015_onet2015 b "
			+ "where a.onetsoc_code=b.onet_code_2015 and scale_id='RL' and data_value>1.5 "
			+ "and b.ssoc_code='" + code.substring(0, 4) + "' "
			+ "group by b.ssoc_code, element_id, category having score >20 order by b.ssoc_code, score desc limit 2";
		//System.out.println(query);
		rs=stmt.executeQuery(query);
		while (rs.next()) {
			content += "\t\t<wcc_education>education_training_experience;" + rs.getString(2) + ";occupation-education</wcc_education>\n";
			//content += "\t\t<wcc_education>education_training_experience;occupation-education;" + rs.getString(2) + ";" + rs.getString(3) + "</wcc_education>\n";
						
		}


		query = "SELECT 'interests;', element_id, CONVERT(avg(data_value/7*100),UNSIGNED INTEGER) score, ';occupational-interests'"
			+ "FROM onet.interests a, lssoc.ssoc2015_onet2015 b where a.onetsoc_code=b.onet_code_2015 and scale_id='OI' "
			+ "and b.ssoc_code='" + code.substring(0, 4) + "' "
			+ "group by ssoc_code,element_id order by ssoc_code asc, score desc, element_id asc limit 3";
		//System.out.println(query);
		rs=stmt.executeQuery(query);
		while (rs.next()) {
			content += "\t\t<wcc_interest>" + rs.getString(1)  + rs.getString(2)  + rs.getString(4) + "</wcc_interest>\n";
			//content += "\t\t<wcc_interest>" + rs.getString(1)  + rs.getString(2) + ";" + rs.getString(3) + "</wcc_interest>\n";
						
		}


		query = "SELECT distinct 'interests;', a.element_id, CONVERT(avg(data_value/5*100),UNSIGNED INTEGER) score, ';occupation-work-context'"
			+ "FROM onet.work_context a, lssoc.ssoc2015_onet2015 b where a.onetsoc_code=b.onet_code_2015 and scale_id='CX' and  data_value>=3 "
			+ "and ssoc_code='" + code.substring(0, 4) + "' "
			+ "group by ssoc_code,element_id order by ssoc_code asc, score desc, element_id asc limit 30";
		//System.out.println(query);
		rs=stmt.executeQuery(query);
		while (rs.next()) {
			content += "\t\t<wcc_work_condition>" + rs.getString(1)  + rs.getString(2) + rs.getString(4) + "</wcc_work_condition>\n";
			//content += "\t\t<wcc_work_conditiont>" + rs.getString(1)  + rs.getString(2) + ";" + rs.getString(3) + "</wcc_work_conditiont>\n";
						
		}
		
		content +="\t\t</enriched_content>\n";

		return content;

	}

	public static String getEnrichment2(String code) throws SQLException, ClassNotFoundException
	{
		String query = "SELECT distinct c.code, c.name FROM lonet.occupation_career_cluster a, lssoc.ssoc2015_onet2015 b, lonet.career_clusters c "
			+ "where a.code=b.onet_code_2015 and a.career_Pathway=c.code "
			+ "and b.ssoc_code='" + code.substring(0, 4) + "' limit 2";
		//System.out.println(query);
		ResultSet rs=stmt.executeQuery(query);
		String content ="\t\t<SectorOfIndustry>\n";
		while (rs.next()) 
			content += "\t\t<Value id=\"" + rs.getString(1) + "\">" + rs.getString(2) + "</Value>\n";

		content +="\t\t</SectorOfIndustry>\n";
		//content += "<Region><Value>" + districts[rn4.nextInt(28)] + "</Value></Region>\n";
		if (salary_scale.get(code.substring(0, 4))!=null)
			content += "<PreferredSalary><Value>" + (salary_scale.get(code.substring(0, 4))+(salary_scale.get(code.substring(0, 4)) * rn.nextInt(15)/100)) + "</Value></PreferredSalary>\n";
		return content;

	}

	public static String enrichSalaryRange(String code) throws SQLException, ClassNotFoundException
	{
		String content = null; 
		//content += "<Region><Value>" + districts[rn4.nextInt(28)] + "</Value></Region>\n";
		if (salary_scale.get(code.substring(0, 4))!=null) {
			content ="\t\t<RemunerationPackage>\n\t\t\t<BasePay currencyCode=\"SGD\" baseInterval=\"Monthly\">";
			content += "\t\t\t\t<BasePayAmountMin>" + (salary_scale.get(code.substring(0, 4))-(salary_scale.get(code.substring(0, 4)) * rn.nextInt(25)/100)) + "</BasePayAmountMin>\n";
			content += "\t\t\t\t<BasePayAmountMax>" + (salary_scale.get(code.substring(0, 4))+(salary_scale.get(code.substring(0, 4)) * rn.nextInt(10)/100)) + "</BasePayAmountMax>\n\t\t\t</BasePay>\n\t\t</RemunerationPackage>\n";
		}
		return content;

	}

	private static void usage() {
		System.err.println("Usage: niriParser <file.xml>");
		System.err.println("       -usage or -help = this message");
		System.exit(1);
	}
	private static String convertToFileURL(String filename) {
		String path = new File(filename).getAbsolutePath();
		if (File.separatorChar != '/') {
			path = path.replace(File.separatorChar, '/');
		}

		if (!path.startsWith("/")) {
			path = "/" + path;
		}
		return "file:" + path;
	}
	/* Loads data generation properties into the system
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
