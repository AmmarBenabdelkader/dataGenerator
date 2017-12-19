/**
 * @author abenabdelkader
 *
 * testing.java
 * Oct 7, 2015
 */
package com.wccgroup.elise.testdata;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
//STEP 1. Import required packages
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

//import com.wccgroup.elise.nes.Offered.Candidate;

public class dataGenerator
{
	// JDBC driver name and database URL
	static String JDBC_DRIVER = "";
	static String DB_URL = "";
	static String DB_URL2 = "";

	//  Database credentials
	static String USER = "";
	static String PASS = "";
	static Map<String, String> ESCOjobs = new HashMap<String, String>();
	static Map<Integer, String> zipcodes = new HashMap<Integer, String>();
	static Map<Integer, String> disabilityList = new HashMap<Integer, String>();

	public static void main(String[] args) throws ClassNotFoundException
	{

		readProperties();
		loadEscoTitles();
		generateJobs();
		generateCandidates(15,24, 650); 
		generateCandidates(25,34, 1600); generateCandidates(35,44, 1750);generateCandidates(45,54, 1900);generateCandidates(55,65, 1200); 
		generateLanguages();
		Map<String, Integer> cities = new HashMap<String, Integer>() {{ put("Amsterdam", 3000);  put("'s-Gravenhage", 2000);  put("Almere", 1000); put("Eindhoven", 500); put("Groningen", 500); put("Arnhem", 400); put("Enschede", 400);  put("Alkmaar", 1000); put("Amstelveen", 500); put("Uithoorn", 500); put("Hilversum", 500); put("IJmuiden", 500);
							put("Leeuwarden", 200); put("Sneek", 100); put("Den Helder", 200); put("Zwolle", 50); put("Bergen op Zoom", 50);
							put("Apeldoorn", 100); put("Leiden", 100); put("Dordrecht", 50); put("Breda", 100); put("'s-Hertogenbosch", 80);}};
		generateZipCodes(cities); //generateZipCodes_old(cities);
		generateEducation();
		generateChildren();
		 
		generateJobEducation();
		GenCandidateSkills();
		}

	public static void generateEducation()
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			Statement stmt3 = conn.createStatement();
			ResultSet rs = null, rs2 = null;
			String query;
			int age = 0, niv = 0, cId = 0;
			int j = 1;
			query = "select candidate_id, birth_date, timestampdiff(year,birth_date, curdate()) age FROM test.candidate where candidate_id not in (select distinct candidate_id from test.candidate_education)";
			//System.out.println("\t- " + query); 
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			while (rs.next())
			{
				age = rs.getInt("age");
				cId = rs.getInt("candidate_id");
				if (age > 30)
				{
					niv = rn.nextInt(10) + 1;
				}
				else if (age > 27)
				{
					niv = rn.nextInt(9) + 1;
				}
				else if (age > 25)
				{
					niv = rn.nextInt(8) + 1;
				}
				else if (age > 22)
				{
					niv = rn.nextInt(3) + 1;
				}
				System.out.println("\tAge " + age + " --> Niv.: " + niv);
				if (niv == 0)
				{
					continue;
				}
				for (int i = niv; i <= 10; i++)
				{
					query = "SELECT * from escoskos.education where education_niveau=" + i + " order by niveau_code";
					//System.out.println("\t- " + query); 
					rs2 = stmt2.executeQuery(query);
					if (rs2.next())
					{
						query = "insert into test.candidate_education (candidate_id, education_name, education_level) values ("
							+ cId
							+ ", '"
							+ rs2.getString("education_name")
							+ "', '"
							+ rs2.getString("niveau_code")
							+ "')";
						System.out.println("\t\t- " + query);
						stmt3.executeUpdate(query);

					}
					if (i > 3 && i < 9)
					{
						i = 9;
					}

				}
				rs2.close();
			}
			rs.close();

			//STEP 6: Clean-up environment
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
	}//end 

	public static void generateJobEducation() throws ClassNotFoundException
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null, rs2 = null;
			String query;
			int jobId = 0;
			int j = 1;

			Map<Integer, String> educations = new HashMap<Integer, String>();
			String[] education;
			query = "SELECT education_name, education_desc,education_niveau, niveau_code from escoskos.education";
			System.out.print("Loading education ...");
			rs = stmt.executeQuery(query);
			for (j = 1; rs.next(); j++)
			{
				educations.put(j, rs.getString(1) + "##" + rs.getString(2) + "##" + rs.getString(3) + "##" + rs.getString(4));

			}
			System.out.println("\tTotal of " + j);

			query = "select job_id, job_title, job_occupation isco_code FROM test.job where job_id not in (select distinct job_id from test.job_education)";
			//System.out.println("\t- " + query); 
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			while (rs.next())
			{
				jobId = rs.getInt("job_id");
				education = educations.get(rn.nextInt(j - 1) + 1).split("##");
				query = "insert into test.job_education (job_id, education_name, education_level) values ("
					+ jobId
					+ ", \""
					+ education[0]
					+ "\", '"
					+ education[3]
					+ "')";
				System.out.println("\t\t- " + query);
				stmt2.executeUpdate(query);

			}
			rs.close();

			//STEP 6: Clean-up environment
			stmt.close();
			stmt2.close();
			conn.close();

		}
		catch (SQLException se)
		{
			//Handle errors for JDBC
			se.printStackTrace();
		}
	}//end try

	public static void generateChildren()
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmt2 = conn.createStatement();
			ResultSet rs = null;
			String query;
			int age = 0, nbre = 0, cId = 0, Byear = 0;
			int j = 1;
			query = "SELECT candidate_id, birth_date, timestampdiff(year,birth_date, curdate()) age, year(birth_date) Byear FROM test.candidate where marital_status!='Single' and candidate_id not in (select distinct candidate_id from test.candidate_children)";
			//System.out.println("\t- " + query); 
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			while (rs.next())
			{
				age = rs.getInt("age");
				cId = rs.getInt("candidate_id");
				Byear = rs.getInt("Byear");
				if (age > 40)
				{
					nbre = rn.nextInt(4);
				}
				else if (age > 35)
				{
					nbre = rn.nextInt(3);
				}
				else if (age > 29)
				{
					nbre = rn.nextInt(2);
				}
				else if (age > 22)
				{
					nbre = rn.nextInt(1);
				}
				System.out.println("\tAge " + age + " --> Childeren: " + nbre);
				if (nbre == 0)
				{
					continue;
				}
				for (int i = 1; i <= nbre; i++)
				{
					//System.out.println("\t- " + query); 
					Byear = Byear + 20 + rn.nextInt(4) + 1;
					query = "insert into test.candidate_children (candidate_id, child_dob, child_gender) values ("
						+ cId
						+ ", '"
						+ Byear
						+ "-"
						+ (rn.nextInt(12) + 1)
						+ "-"
						+ (rn.nextInt(28) + 1)
						+ "', '"
						+ (rn.nextInt(5) >= 2 ? "Female" : "Male")
						+ "')";
					System.out.println("\t\t- " + query);
					stmt2.executeUpdate(query);

				}
			}
			rs.close();

			//STEP 6: Clean-up environment
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
	}//end 

	public static void generateZipCodes(Map<String, Integer> cities)
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;
			Iterator<Map.Entry<String, Integer>> it = cities.entrySet().iterator();
			String query;
			int nbre = 0, i = 1, j = 1;

			System.out.print("Generating Addresses ...\t");
			while (it.hasNext())
			{
				Map.Entry<String, Integer> entry = it.next();
				nbre = entry.getValue();
				System.out.print(entry.getKey() + "(" + nbre + "), ");
				query = "select * from escoskos.dutchpostalcodelookup where city = \"" + entry.getKey() + "\"";
				rs = stmt.executeQuery(query);
				for (i = 1; rs.next() && i <= nbre; j++, i++)
				{
					//System.out.println("\t" + j + ". " + zipCode + " " + city + ", " + country); 
					zipcodes.put(
						j,
						rs.getString(1)
							+ "##"
							+ rs.getString(3)
							+ "##"
							+ rs.getString(5)
							+ "##"
							+ rs.getString(6)
							+ "##"
							+ rs.getString(8)
							+ "##"
							+ rs.getString(9));

				}

			}
			System.out.println("\n\tTotal of \t" + j);

			query = "SELECT candidate_id from test.candidate where candidate_id not in (select distinct candidate_id from test.candidate_locations)";
			//query = "SELECT candidate_id from test.candidate where zipcode is null and geo_latitude is null"; 
			//System.out.println(query); 
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			for (; rs.next();)
			{
				String[] addresses = zipcodes.get(rn.nextInt(j - 1) + 1).split("##");
				query = "insert into test.candidate_locations values ('"
					+ rs.getInt(1)
					+ "','"
					+ addresses[0]
					+ "', \""
					+ addresses[1]
					+ "\", \""
					+ addresses[3]
					+ "\", '"
					+ addresses[2]
					+ "', '"
					+ addresses[4]
					+ "', '"
					+ addresses[5]
					+ "', 'Home',"
					+ (rn.nextInt(10) * 5 + 25)
					+ ",'Netherlands')";
				System.out.print(".");
				stmtS.executeUpdate(query);
			}
			rs.close();

			//STEP 6: Clean-up environment
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
	}//end 

	public static void generateZipCodes_old(Map<String, Integer> cities)
	{

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;
			Iterator<Map.Entry<String, Integer>> it = cities.entrySet().iterator();
			String city = "", country, zipCode, query;
			int nbre = 0, from_ = 0, to_ = 0;
			int j = 1;

			while (it.hasNext())
			{
				Map.Entry<String, Integer> entry = it.next();
				nbre = entry.getValue();
				query = "select from_, to_, city, country from escoskos.zip_code where city like '" + entry.getKey() + "%'";
				//System.out.println("\t- " + query); 
				rs = stmt.executeQuery(query);
				if (rs.next())
				{
					from_ = rs.getInt(1);
					to_ = rs.getInt(2);
					country = rs.getString(4);
					city = rs.getString(3);
					//System.out.println(from_ + "-" + to_ + ": " + entry.getKey() + "-" + city); 
					if (city.indexOf(" ") > 0)
					{
						city = city.substring(0, city.indexOf(" "));
					}
					if (city.indexOf("-") > 0)
					{
						city = city.substring(0, city.indexOf("-"));
					}
					if (city.indexOf(",") > 0)
					{
						city = city.substring(0, city.indexOf(","));
					}

					Random rn = new Random();
					for (int i = 0; i < nbre; i++)
					{
						if (to_ == from_)
						{
							zipCode = String.valueOf(from_) + " " + (char)(rn.nextInt(26) + 65) + (char)(rn.nextInt(26) + 65);
						}
						else
						{
							zipCode = String.valueOf(rn.nextInt(to_ - from_) + from_)
								+ (char)(rn.nextInt(26) + 65)
								+ (char)(rn.nextInt(26) + 65);
						}

						//System.out.println("\t" + j + ". " + zipCode + " " + city + ", " + country); 
						zipcodes.put(j, "" + zipCode + " " + city);
						j++;
					}

				}

			}

			/*		// add geo_coordinates to candidate address
					Iterator<Map.Entry<Integer, String>> it2 = zipcodes.entrySet().iterator();
					for (int s=1; it2.hasNext(); s++) {
						Map.Entry<Integer, String> entry = it2.next();
			query = "select * from escoskos.dutchpostalcodelookup where code='" + entry.getValue() + "'";
			System.out.println(s); 
				    	rs = stmt.executeQuery(query);
				    	if (rs.next())
				    		zipcodes.put(s,entry.getValue() + " " + rs.getString(8) + " " + rs.getString(9) );
					}
			
			*/
			query = "SELECT candidate_id from test.candidate where zipcode is null";
			//query = "SELECT candidate_id from test.candidate where zipcode is null and geo_latitude is null"; 
			//System.out.println(query); 
			rs = stmt.executeQuery(query);
			Random rn = new Random();

			for (; rs.next();)
			{
				String[] addresses = zipcodes.get(rn.nextInt(j - 1) + 1).split(" ");
				System.out.println(addresses[0]);
				query = "update test.candidate set zipcode ='"
					+ addresses[0]
					+ "', city='"
					+ addresses[1]
					+ "' where candidate_id="
					+ rs.getInt(1);
				stmtS.executeUpdate(query);
			}
			rs.close();

			//STEP 6: Clean-up environment
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
	}//end 
	/*
	 *  The generateCandidates method generates candidates with random values for occupation, driving license, gender, marital status, and disabilities
	 *  It also generates working experience for the candidate based on his/her age and based on current occupation
	 */

	public static void generateCandidates(int from, int to, int nbre)
	{
		int jump = 3;
		String Bdate = ""; // date of birth
		//Date date = new Date();
		int year = 2015; // current year
		int age = 20; // working age
		int salary = 30000; // yearly salary
		int start = 1; // start counter for candidate Id
		int hours = 0; //availability hours
		String jobFP = ""; // job full/part time
		String query = "", jobType, jobTitle, ocId;
		Map<Integer, String> nationalities = new HashMap<Integer, String>()
		{
			/**
			* 
			*/
			private static final long serialVersionUID = 1L;

			{
				put(1, "Belgian");
				put(2, "Spanish");
				put(3, "Italian");
				put(4, "French");
				put(5, "German");
				put(6, "British");
				put(7, "Bulgarian");
				put(8, "Hungarian");
				put(9, "Croatian");
			}
		};

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;
			rs = stmt.executeQuery("select max(candidate_id) from test.candidate");
			if (rs.next())
			{
				start = rs.getInt(1) + 1;
			}

			for (int i = start; i <= nbre + start; i++)
			{
				// generating date of birth and working experience (only dates) yyyy-mm-dd
				Random rn = new Random();
				int d = rn.nextInt(28);
				d += 1; // generates a random day between 1 and 28
				int m = rn.nextInt(12);
				m += 1; // generates a random month between 1 and 12
				int y = rn.nextInt(to - from);
				y += year - to; // generates a random day between 'from' and 'to' 
				age = rn.nextInt(10) + 15; // generates a random working age between '15' and '30' 
				Bdate = y + "-" + m + "-" + d;

				// generating gender type
				int g = rn.nextInt(3) + 1; // generates a random day between 1 and 3
				String gender = "", occupation = "";
				switch (g)
				{
				case 1:
				case 2:
					gender = "Male";
					break;
				case 3:
					gender = "Female";
					break;
				default:
					gender = "Male";
					break;
				}

				// generating driving licenses
				g = rn.nextInt(9) + 1; // generates a random day between 1 and 10
				String lic = "Yes";
				switch (g)
				{
				case 1:
				case 7:
				case 2:
				case 9:
					lic = "No";
					break;
				default:
					lic = "Yes";
					break;
				}
				// generating marital status
				g = rn.nextInt(12) + 1; // generates a random day between 1 and 10
				String MS = "Single";
				switch (g)
				{
				case 1:
				case 2:
				case 3:
					MS = "Single";
					break;
				case 4:
				case 5:
				case 6:
					MS = "Married";
					break;
				case 7:
				case 8:
					MS = "Divorced";
					break;
				case 9:
				case 10:
					MS = "Living Together";
					break;
				case 11:
					MS = "Separated";
					break;
				case 12:
					MS = "Interlocutory";
					break;
				default:
					MS = "Married";
					break;
				}

				g = rn.nextInt(110); // generates a random day between 1 and 10
				String disability = "";
				switch (g)
				{
				case 10:
					disability = "Attention-Deficit";
					break;
				case 21:
					disability = "Hyperactivity Disorders";
					break;
				case 32:
					disability = "Blindness";
					break;
				case 45:
					disability = "Low Vision";
					break;
				case 57:
					disability = "Brain Injuries";
					break;
				case 69:
					disability = "Deaf";
					break;
				case 70:
					disability = "Hard-of-Hearing";
					break;
				case 93:
					disability = "Learning Disabilities";
					break;
				case 82:
					disability = "Medical Disabilities";
					break;
				case 99:
					disability = "Speech Disabilities";
					break;
				case 105:
					disability = "Language Disabilities";
					break;
				default:
					disability = "";
					break;
				}
				if (disability.length() > 0)
				{
					disabilityList.put(i, disability);
				}

				occupation = ESCOjobs.get(String.valueOf(rn.nextInt(4760)));
				//System.out.println("\n\t\n" + occupation.charAt(occupation.indexOf("##")+2));
				switch (occupation.charAt(occupation.indexOf("##") + 2))
				{
				case '0':
					salary = (rn.nextInt(60) + 30) * 1000;
					break; // Armed forces occupations
				case '1':
					salary = (rn.nextInt(80) + 40) * 1000;
					break; // Managers
				case '2':
					salary = (rn.nextInt(60) + 40) * 1000;
					break; // Professionals
				case '3':
					salary = (rn.nextInt(30) + 40) * 1000;
					break; // Technicians and associate professionals
				case '4':
					salary = (rn.nextInt(25) + 20) * 1000;
					break; // Clerical support workers
				case '5':
					salary = (rn.nextInt(30) + 30) * 1000;
					break; // Service and sales workers 
				case '6':
					salary = (rn.nextInt(30) + 30) * 1000;
					break; // Skilled agricultural, forestry and fishery workers
				case '7':
					salary = (rn.nextInt(20) + 30) * 1000;
					break; // Craft and related trades workers 
				case '8':
					salary = (rn.nextInt(25) + 30) * 1000;
					break; // Plant and machine operators and assemblers
				case '9':
					salary = (rn.nextInt(20) + 20) * 1000;
					break; // Elementary occupations
				default:
					salary = 0;
					break; // a
				}

				int validity = rn.nextInt(100);
				String days = "'2099-12-31'";
				if (validity < 10)
				{
					days = "date_sub(curdate(), interval " + rn.nextInt(365 * 2) + " day)";
				}
				if (validity < 30)
				{
					days = "date_add(curdate(), interval " + rn.nextInt(365 * 3) + " day)";
				}

				//TODO other nationalities, use hash map with random
				query = "insert into test.candidate (candidate_id,birth_date, gender, driving_licence, marital_status, candidate_Status, nationality, work_permit_validity) values ("
					+ i
					+ ",'"
					+ Bdate
					+ "', '"
					+ gender
					+ "', '"
					+ lic
					+ "', '"
					+ MS
					+ "', '"
					+ (rn.nextInt(100) > 12 ? "Active" : "Inactive")
					+ "', '"
					+ (rn.nextInt(100) < 80 ? "Dutch" : nationalities.get(rn.nextInt(9) + 1))
					+ "', "
					+ days
					+ ")";
				System.out.println(query);
				stmt.executeUpdate(query);
				// assign unlimited work-permit_validity to Dutch citizens
				stmt.executeUpdate("update test.candidate set work_permit_validity='2099-12-31'  where nationality='Dutch'");

				g = rn.nextInt(17);
				jobFP = "Full Time";
				switch (g)
				{
				case 0:
					hours = 4;
					jobFP = "Part Time";
					break;
				case 1:
					hours = 40;
					jobFP = "Full Time";
					break;
				case 2:
					hours = 8;
					jobFP = "Part Time";
					break;
				case 3:
					hours = 12;
					jobFP = "Part Time";
					break;
				case 4:
					hours = 36;
					jobFP = "Full Time";
					break;
				case 5:
					hours = 16;
					jobFP = "Part Time";
					break;
				case 6:
					hours = 32;
					jobFP = "Part Time";
					break;
				case 7:
					hours = 20;
					jobFP = "Part Time";
					break;
				case 8:
					hours = 28;
					jobFP = "Part Time";
					break;
				case 9:
					hours = 24;
					jobFP = "Part Time";
					break;
				case 10:
				case 11:
				case 12:
				case 13:
					hours = 40;
					jobFP = "Full Time";
					break;
				case 14:
				case 15:
				case 16:
				case 17:
					hours = 36;
					jobFP = "Full Time";
					break;
				default:
					jobFP = "Full Time";
					break;
				}

				query = "insert into test.candidate_ambitions (candidate_id,job_title, availability_date, salary, salary_period, desired_contract_type, desired_job_type, desired_full_part_time, availability_hours, hours_period) values ("
					+ i
					+ ",\""
					+ occupation.substring(0, occupation.indexOf("##"))
					+ "\", date_add(curdate(), interval "
					+ rn.nextInt(6 * 30)
					+ " day), "
					+ salary
					+ ", 'Year', '"
					+ (rn.nextInt(10) > 4 ? "Permanent" : "Temporary")
					+ "', '"
					+ (rn.nextInt(10) > 3 ? "Regular-job" : "Internship")
					+ "', '"
					+ jobFP
					+ "', "
					+ hours
					+ ", 'Week')";
				System.out.println(query);
				stmt.executeUpdate(query);

				// retrieve list of similar occupations, to create work experience
				query = "SELECT occupation, similar FROM occupation_similars where ConceptPT=\"" + occupation + "\"";
				//System.out.println(query); 
				rs = stmt.executeQuery(query);
				Map<Integer, String> similars = new HashMap<Integer, String>();
				similars.put(0, occupation);
				ocId = "";
				System.out.println("\tSimilar occupations for work experience: ");
				for (int j = 1; rs.next(); j++)
				{
					similars.put(j, rs.getString(2));
					ocId = rs.getString(1);
					System.out.println("\t - " + rs.getString(2));
				}
				rs.close();

				// ********** end list of similar occupations

				int exp = year - y - age; ///3;
				//System.out.println("\t\t -exp1 " + exp); 
				//exp = exp<2?1:exp;
				//System.out.println("\t\t -y= " + exp); 
				Map<String, String> skills = new HashMap<String, String>();
				for (int j = y + age; j < year; j = j + jump)
				{

					g = rn.nextInt(17);
					jobFP = "Full Time";
					switch (g)
					{
					case 0:
						hours = 4;
						jobFP = "Part Time";
						break;
					case 1:
						hours = 40;
						jobFP = "Full Time";
						break;
					case 2:
						hours = 8;
						jobFP = "Part Time";
						break;
					case 3:
						hours = 12;
						jobFP = "Part Time";
						break;
					case 4:
						hours = 36;
						jobFP = "Full Time";
						break;
					case 5:
						hours = 16;
						jobFP = "Part Time";
						break;
					case 6:
						hours = 32;
						jobFP = "Part Time";
						break;
					case 7:
						hours = 20;
						jobFP = "Part Time";
						break;
					case 8:
						hours = 28;
						jobFP = "Part Time";
						break;
					case 9:
						hours = 24;
						jobFP = "Part Time";
						break;
					case 10:
					case 11:
					case 12:
					case 13:
						hours = 40;
						jobFP = "Full Time";
						break;
					case 14:
					case 15:
					case 16:
					case 17:
						hours = 36;
						jobFP = "Full Time";
						break;
					default:
						jobFP = "Full Time";
						break;
					}

					jobTitle = similars.get(rn.nextInt(similars.size()));

					if (exp < 2)
					{
						jump = exp;
					}
					else
					{
						jump = rn.nextInt(exp / 2) + 1; //jump = ((j+jump)>y) ?year-j:jump;
					}

					query = "insert into test.work_experience (candidate_id, job_title_we, isco_code, full_part_time, start_we, end_we, employer_id) values ("
						+ i
						+ ", \""
						+ jobTitle.substring(0, jobTitle.indexOf("##"))
						+ "\", '"
						+ jobTitle.substring(jobTitle.indexOf("##") + 2)
						+ "', '"
						+ jobFP
						+ "', '"
						+ j
						+ "-"
						+ (rn.nextInt(12) + 1)
						+ "-"
						+ (rn.nextInt(28) + 1)
						+ "', '"
						+ (j + jump > year ? year : j + jump)
						+ "-"
						+ (rn.nextInt(12) + 1)
						+ "-"
						+ (rn.nextInt(28) + 1)
						+ "', 1)";

					System.out.println("\t- " + query);
					stmt.executeUpdate(query);

					// retrieve list of skills for jobs, to create candidate skills
					query = "SELECT ConceptURI, ConceptPT FROM skills where ConceptURI in (select hasRelatedConcept from occupation_skill where isRelatedConcept='"
						+ ocId
						+ "')";
					//System.out.println(query); 
					rs = stmt.executeQuery(query);
					String skillId = "";
					for (; rs.next();)
					{
						skillId = rs.getString(1);
						skillId = skillId.substring(skillId.lastIndexOf("/") + 1, skillId.length());
						skills.put(skillId, rs.getString(2));
						//System.out.println("\t\t skill --> " + rs.getString(1) + ": " + rs.getString(2)); 
						//query = "insert into test.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');"; 
						//System.out.println("\t" + query); 
						//stmt.executeUpdate(query);
					}
					rs.close();

					// ********** end list of similar occupations

				}
				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();
				//System.out.println("\tCombined skills for all for work experience: "); 

				while (it.hasNext())
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into test.candidate_skills (candidate_id, skill_id, skill_name) values ("
						+ i
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue()); 
					stmt.executeUpdate(query);

				}
			}

			Iterator<Map.Entry<Integer, String>> it = disabilityList.entrySet().iterator();
			int l = 1;
			while (it.hasNext())
			{
				Map.Entry<Integer, String> entry = it.next();
				query = "insert into test.candidate_disabilities (candidate_id, disability_id, disability_name) values ("
					+ entry.getKey()
					+ ", "
					+ l++
					+ ", \""
					+ entry.getValue()
					+ "\");";
				//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue()); 
				stmt.executeUpdate(query);

			}

			//STEP 6: Clean-up environment
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
	}//end 

	public static void GenCandidateSkills()
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmtC = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmtC = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs = null, rsC;
			int i = 0;
			int c = 0;

			String query = "SELECT candidate_id, job_title from test.candidate_ambitions where candidate_id not in (select distinct candidate_id from test.candidate_skills)";
			//System.out.println(query); 
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();

				System.out.print("\nSimilar occupations for " + rsC.getString(2) + ": \n\t-");
				// retrieve list of skills for jobs, to create candidate skills
				query = "SELECT ConceptURI, ConceptPT FROM skills where ConceptURI in (select hasRelatedConcept from occupation_skill where isRelatedConcept=(SELECT ConceptURI FROM escoskos.occupations where conceptPT = \""
					+ rsC.getString(2)
					+ "\"))";
				//System.out.println(query); 
				rs = stmt2.executeQuery(query);
				String skillId = "";
				for (; rs.next();)
				{
					skillId = rs.getString(1);
					skillId = skillId.substring(skillId.lastIndexOf("/") + 1, skillId.length());
					skills.put(skillId, rs.getString(2));
					System.out.print(rs.getString(2) + ", ");
					i++;
					//query = "insert into test.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');"; 
					//System.out.println("\t" + query); 
					//stmt.executeUpdate(query);
				}
				rs.close();
				if (skills.size() >= 25)
				{
					i = 12;
				}
				if (skills.size() < 25 && skills.size() >= 20)
				{
					i = 10;
				}
				if (skills.size() <= 15 && skills.size() > 8)
				{
					i = 8;
				}
				if (skills.size() <= 8 && skills.size() > 4)
				{
					i = 5;
				}
				if (skills.size() <= 4)
				{
					i = skills.size();
				}

				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();

				c = 0;

				while (it.hasNext() && c < i)
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into test.candidate_skills (candidate_id, skill_id, skill_name) values ("
						+ rsC.getString(1)
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue()); 
					System.out.print(entry.getValue() + ", ");
					stmt.executeUpdate(query);
					c++;

				}
				i = 0;
			}

			//STEP 6: Clean-up environment
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
	}//end function

	public static void GenCandidateSkills2()
	{
		Connection conn = null;
		Statement stmt = null, stmt2 = null, stmtC = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmt2 = conn.createStatement();
			stmtC = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs = null, rs2, rsC;
			int i = 0;
			int c = 0;

			String query = "SELECT candidate_id, job_title from test.candidate_ambitions where candidate_id not in (select distinct candidate_id from test.candidate_skills)";
			//System.out.println(query); 
			rsC = stmtC.executeQuery(query);
			Map<String, String> skills = new HashMap<String, String>();
			while (rsC.next())
			{
				skills.clear();
				// retrieve list of similar occupations, to create work experience
				query = "SELECT occupation, similar FROM occupation_similars where ConceptPT=\"" + rsC.getString(2) + "\"";
				//System.out.println(query); 
				rs = stmt.executeQuery(query);
				//Map<Integer, String> similars = new HashMap<Integer, String>();
				//similars.put(0, rsC.getString(2));
				String ocId = "";
				//System.out.print("\n" + rsC.getString(1) + ": " + rsC.getString(2) +"\n\t -"); 
				System.out.println("\nSimilar occupations for " + rsC.getString(2) + ": ");
				for (int j = 1; rs.next(); j++)
				{
					//similars.put(j, rs.getString(2));
					ocId = rs.getString(1);
					System.out.print("\n\t - " + rs.getString(2) + "\n\t\tSkills: ");

					// retrieve list of skills for jobs, to create candidate skills
					query = "SELECT ConceptURI, ConceptPT FROM skills where ConceptURI in (select hasRelatedConcept from occupation_skill where isRelatedConcept='"
						+ ocId
						+ "')";
					//System.out.println(query); 
					rs2 = stmt2.executeQuery(query);
					String skillId = "";
					for (; rs2.next();)
					{
						skillId = rs2.getString(1);
						skillId = skillId.substring(skillId.lastIndexOf("/") + 1, skillId.length());
						skills.put(skillId, rs2.getString(2));
						System.out.print(rs2.getString(2) + ", ");
						//query = "insert into test.candidate_skills (job_id, skill_id) values (" + i + ", '" +  skillId + "');"; 
						//System.out.println("\t" + query); 
						//stmt.executeUpdate(query);
					}
					rs2.close();
				}
				rs.close();
				if (skills.size() >= 25)
				{
					i = 12;
				}
				if (skills.size() < 25 && skills.size() >= 20)
				{
					i = 10;
				}
				if (skills.size() <= 15 && skills.size() > 8)
				{
					i = 8;
				}
				if (skills.size() <= 8 && skills.size() > 4)
				{
					i = 5;
				}
				if (skills.size() <= 4)
				{
					i = skills.size();
				}

				Iterator<Map.Entry<String, String>> it = skills.entrySet().iterator();

				while (it.hasNext() && c < i)
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into test.candidate_skills (candidate_id, skill_id, skill_name) values ("
						+ rsC.getString(1)
						+ ", '"
						+ entry.getKey()
						+ "', \""
						+ entry.getValue()
						+ "\");";
					//System.out.println("\t - " + entry.getKey() + ": " + entry.getValue()); 
					System.out.print(entry.getValue() + ", ");
					//stmt.executeUpdate(query);
					c++;

				}
				i = 0;
			}

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
	}//end function

	public static void generateLanguages()
	{
		Map<String, String> languages = new HashMap<String, String>();
		String query = "";

		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			Statement stmtS = conn.createStatement();
			ResultSet rs = null;

			// Generate Candidate Languages
			rs = stmt.executeQuery(
				"SELECT Candidate_id FROM test.Candidate where candidate_id not in (select distinct candidate_id from test.candidate_languages)");

			String cId, lang, level = "";
			while (rs.next())
			{
				cId = rs.getString(1);
				lang = "en";
				for (int l = 0; l < 4; l++)
				{
					Random rn = new Random();
					int r = rn.nextInt(19); // generates a random number between 1 and 19
					switch (r)
					{
					case 0:
						lang = "de";
						level = "Intermediate";
						break;
					case 1:
						lang = "nl";
						level = "Advanced";
						break;
					case 2:
						lang = "nl";
						level = "Beginner";
						break;
					case 3:
						lang = "nl";
						level = "Beginner";
						break;
					case 4:
						lang = "en";
						level = "Advanced";
						break;
					case 5:
						lang = "nl";
						level = "Advanced";
						break;
					case 6:
						lang = "en";
						level = "Beginner";
						break;
					case 7:
						lang = "it";
						level = "Beginner";
						break;
					case 8:
						lang = "it";
						level = "Intermediate";
						break;
					case 9:
						lang = "es";
						level = "Intermediate";
						break;
					case 10:
						lang = "es";
						level = "Beginner";
						break;
					case 11:
						lang = "fr";
						level = "Beginner";
						break;
					case 12:
						lang = "fr";
						level = "Intermediate";
						break;
					case 13:
						lang = "de";
						level = "Beginner";
						break;
					case 14:
						lang = "nl";
						level = "Intermediate";
						break;
					case 15:
						lang = "en";
						level = "Intermediate";
						break;
					case 16:
						lang = "it";
						level = "Advanced";
						break;
					case 17:
						lang = "es";
						level = "Advanced";
						break;
					case 18:
						lang = "fr";
						level = "Advanced";
						break;
					case 19:
						lang = "de";
						level = "Advanced";
						break;
					default:
						lang = "en";
						level = "Intermediate";
						break;
					}
					languages.put(lang, level);
				}
				Iterator<Map.Entry<String, String>> it = languages.entrySet().iterator();
				while (it.hasNext())
				{
					Map.Entry<String, String> entry = it.next();
					query = "insert into test.Candidate_Languages (candidate_id, language_id, level) values ("
						+ cId
						+ ", '"
						+ entry.getKey()
						+ "', '"
						+ entry.getValue()
						+ "')";
					System.out.println(query);
					stmtS.executeUpdate(query);
				}
				languages.clear();

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
	}//end 

	public static void loadEscoTitles()
	{
		Connection conn = null;
		Statement stmt = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs, rsS;
			rs = stmt.executeQuery("SELECT ConceptPT, isco_code FROM occupations where ConceptType='OC'");

			int i = 0;
			while (rs.next())
			{
				ESCOjobs.put(String.valueOf(i++), rs.getString(1) + "##" + rs.getString(2));
			}

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
	}//end function

	public static void generateJobs()
	{
		Connection conn = null;
		Statement stmt = null;
		Statement stmtS = null;
		Statement stmtQ = null;
		try
		{
			//STEP 2: Register JDBC driver
			Class.forName(JDBC_DRIVER);

			//STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			stmtS = conn.createStatement();
			stmtQ = conn.createStatement();

			// JDBC2Elise data type mappings

			ResultSet rs, rsS;
			int i = 1, salary = 0;

			rs = stmt.executeQuery("select max(job_id) from test.job");
			if (rs.next())
			{
				i = rs.getInt(1) + 1;
			}

			rs = stmt.executeQuery("SELECT ConceptURI, ConceptPT, isco_code FROM occupations where ConceptType='OC'");

			String ocId, skill, order, part_full_time, jobSchedule = "", jobId, skillId;
			while (rs.next())
			{
				ocId = rs.getString(1);
				skill = rs.getString(2);
				//System.out.println(i++ + ". " + ocId + ": " + skillId); 

				Random rn = new Random();
				int r = rn.nextInt(5);
				r += 1; // generates a random number between 1 and 4
				//System.out.print("The Random Number is " + r);
				part_full_time = "Full time";
				switch (r)
				{
				case 1:
					order = "";
					break;
				case 2:
					order = " order by ConceptPT";
					break;
				case 3:
					order = " order by ConceptPT desc";
					part_full_time = "Part time";
					break;
				case 4:
					order = " order by length(ConceptPT)";
					break;
				case 5:
					order = " order by length(ConceptPT) desc";
					break;
				default:
					order = "";
					break;
				}

				if (part_full_time.equalsIgnoreCase("part time"))
				{
					Random rn2 = new Random();
					int r2 = rn2.nextInt(7);
					r2 += 1; // generates a random number between 1 and 7
					switch (r2)
					{
					case 1:
						jobSchedule = "08:00 - 12:00";
						break;
					case 2:
						jobSchedule = "12:00 - 17:00";
						break;
					case 3:
						jobSchedule = "09:00 - 12:00";
						break;
					case 4:
						jobSchedule = "13:00 - 17:00";
						break;
					case 5:
						jobSchedule = "12:00 - 16:00";
						break;
					case 6:
						jobSchedule = "17:00 - 22:00";
						break;
					case 7:
						jobSchedule = "18:00 - 24:00";
						break;
					default:
						jobSchedule = "";
						break;
					}

				}

				if (part_full_time.equalsIgnoreCase("full time"))
				{
					Random rn2 = new Random();
					int r2 = rn2.nextInt(7);
					r2 += 1; // generates a random number between 1 and 7
					switch (r2)
					{
					case 1:
						jobSchedule = "09:00 - 17:00";
						break;
					case 2:
						jobSchedule = "09:00 - 17:00";
						break;
					case 3:
						jobSchedule = "08:00 - 16:00";
						break;
					case 4:
						jobSchedule = "08:00 - 16:00";
						break;
					case 5:
						jobSchedule = "16:00 - 24:00";
						break;
					case 6:
						jobSchedule = "18:00 - 02:00";
						break;
					case 7:
						jobSchedule = "20:00 - 04:00";
						break;
					default:
						jobSchedule = "";
						break;
					}

				}

				jobId = ocId.substring(ocId.lastIndexOf("/") + 1, ocId.length());
				switch (rs.getString(3).charAt(0))
				{
				case '0':
					salary = (rn.nextInt(60) + 30) * 1000;
					break; // Armed forces occupations
				case '1':
					salary = (rn.nextInt(80) + 40) * 1000;
					break; // Managers
				case '2':
					salary = (rn.nextInt(60) + 40) * 1000;
					break; // Professionals
				case '3':
					salary = (rn.nextInt(30) + 40) * 1000;
					break; // Technicians and associate professionals
				case '4':
					salary = (rn.nextInt(25) + 20) * 1000;
					break; // Clerical support workers
				case '5':
					salary = (rn.nextInt(30) + 30) * 1000;
					break; // Service and sales workers 
				case '6':
					salary = (rn.nextInt(30) + 30) * 1000;
					break; // Skilled agricultural, forestry and fishery workers
				case '7':
					salary = (rn.nextInt(20) + 30) * 1000;
					break; // Craft and related trades workers 
				case '8':
					salary = (rn.nextInt(25) + 30) * 1000;
					break; // Plant and machine operators and assemblers
				case '9':
					salary = (rn.nextInt(20) + 20) * 1000;
					break; // Elementary occupations
				default:
					salary = 0;
					break; // a
				}

				//System.out.println(i++ + ". " + skillId + " - " + jobType + " (" + jobSchedule + ")"); 
				String query = "insert into test.job (job_id, job_title, part_full_time, job_schedule, job_occupation, salary, salary_period, start_date, telework, job_status, job_contract_type, travel_to_work) values ("
					+ i
					+ ", \""
					+ skill
					+ "\", '"
					+ part_full_time
					+ "', '"
					+ jobSchedule
					+ "', '"
					+ jobId
					+ "', '"
					+ salary
					+ "', 'Year', date_add(curdate(), interval "
					+ rn.nextInt(6 * 30)
					+ " day), '"
					+ (rn.nextInt(100) > 20 ? "No" : "Yes")
					+ "', '"
					+ (rn.nextInt(100) > 20 ? "Active" : "Inactive")
					+ "', '"
					+ (rn.nextInt(100) > 20 ? "Contract" : "Permanent")
					+ "', '"
					+ (rn.nextInt(100) > 12 ? "Yes" : "No")
					+ "');";
				System.out.println(query);
				stmtS.executeUpdate(query);

				query = "SELECT ConceptURI, ConceptPT FROM skills where ConceptURI in (select hasRelatedConcept from occupation_skill where isRelatedConcept='"
					+ ocId
					+ "')"
					+ order;
				//System.out.println(query); 
				rsS = stmtS.executeQuery(query);
				int j = 0;
				while (rsS.next() && j++ < r + 2)
				{
					skillId = rsS.getString(1);
					skillId = skillId.substring(skillId.lastIndexOf("/") + 1, skillId.length());
					//System.out.println("\t" + rsS.getString(2)); 
					query = "insert into test.job_skills (job_id, skill_id) values (" + i + ", '" + skillId + "');";
					System.out.println("\t" + query);
					stmtQ.executeUpdate(query);
				}
				i++;
				rsS.close();
			}

			//STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			stmtS.close();
			stmtQ.close();
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
			DB_URL2 = prop.getProperty("url2");
			USER = prop.getProperty("user");
			PASS = prop.getProperty("pass");

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

}