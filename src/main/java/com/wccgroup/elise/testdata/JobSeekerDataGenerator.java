package com.wccgroup.elise.testdata;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import com.wccgroup.elise.AlternateFunction;
import com.wccgroup.elise.DateValue;
import com.wccgroup.elise.EliseDate;
import com.wccgroup.elise.EliseObjectModel;
import com.wccgroup.elise.IDataObjectIterator;
import com.wccgroup.elise.IElise;
import com.wccgroup.elise.IEliseSession;
import com.wccgroup.elise.IPropertyCollection;
import com.wccgroup.elise.IStructuredDataObject;
import com.wccgroup.elise.Key;
import com.wccgroup.elise.KeyType;
import com.wccgroup.elise.dictionary.IListValueSet;
import com.wccgroup.elise.dictionary.IListValueSetList;
import com.wccgroup.elise.options.InsertOptions;

public class JobSeekerDataGenerator
{

	private ArrayList<String> _educations;
	private ArrayList<String> _educationMajorAreas;
	private ArrayList<String> _educationMajorFields;
	private ArrayList<String> _educationMajorSpecificFields;
	private ArrayList<String> _typesOfUniversities;
	private ArrayList<String> _totalYearsWorkExperiences;
	private ArrayList<String> _industries;
	private ArrayList<String> _sectors;
	private ArrayList<String> _occupationMajorGroups;
	private ArrayList<String> _occupationSubMajorGroups;
	private ArrayList<String> _occupationMinorGroups;
	private ArrayList<String> _occupationUnitGroups;
	private ArrayList<String> _occupations;
	private ArrayList<String> _languageSkills;
	private ArrayList<String> _languageSkillLevels;
	private ArrayList<String> _publicationStatuses;
	private ArrayList<String> _jobTypes;
	private ArrayList<String> _countries;
	private ArrayList<String> _provinces;
	private ArrayList<String> _cities;
	private ArrayList<String> _teleworkings;
	private ArrayList<String> _partTimeOrFullTime;
	private ArrayList<String> _shiftTypes;
	private ArrayList<String> _willingToTravels;
	private ArrayList<String> _jobsList;
	private String staticKeywords = getKeywords();

	public static void main(String[] args)
	{
		JobSeekerDataGenerator js = new JobSeekerDataGenerator();
		js.run(args);
	}

	private void run(String[] args)
	{
		Locale.setDefault(Locale.US);

		// setting up the connection to Elise HRDF DEV server
		Key UserKey = Key.parse("1:f:\"SuperUser\"");
		String Password = "Elise";
		String EliseAddress = "localhost:2800";
		IEliseSession session = EliseObjectModel.createSession(EliseAddress, UserKey, Password);
		IElise elise = session.getElise();

		IDataObjectIterator iterator = elise.getDataObjectIterator();

		while (iterator.moveNext())
		{
			if (iterator.getCurrent().getKey().getKeyType().equals(KeyType.SEQUENCE))
			{
				elise.delete(iterator.getCurrent().getKey());
			}
		}

		// we have around 5,2 million job seekers in production.. Considering 60 match engines in production,
		// while we have 4 ME deployed in HRDFDEV. This program generates 430K job seekers data objects
		for (int i = 0; i < 430000; i++)
		{
			// Create a new empty DataObject of the type "jobseeker".
			IStructuredDataObject dataObject = elise.createDataObject("jobseeker");

			// Add the values of a few properties of the new DataObject.
			IPropertyCollection offeredProperties = dataObject.getOfferedProduct().getProperties();
			IPropertyCollection demandedProperties = dataObject.getDemandedProduct().getProperties();
			_educations = getEducationList(elise);
			// leave only the leaf child
			_educationMajorAreas = getCodesFromList(elise, "ct_maji");
			_educationMajorFields = getCodesFromList(elise, "ct_majii");
			_educationMajorSpecificFields = getCodesFromList(elise, "ct_majiii");
			_typesOfUniversities = getCodesFromList(elise, "ct_unytyp");
			_totalYearsWorkExperiences = getCodesFromList(elise, "ct_twrkex");
			_industries = getCodesFromList(elise, "ct_ind");
			_sectors = getCodesFromList(elise, "ct_sec");
			_occupationMajorGroups = getCodesFromList(elise, "jobtitlemajor");
			_occupationSubMajorGroups = getCodesFromList(elise, "jobtitlesubmajor");
			_occupationMinorGroups = getCodesFromList(elise, "jobtitleminor");
			_occupationUnitGroups = getCodesFromList(elise, "jobtitleunit");
			_occupations = getCodesFromList(elise, "ct_occu");
			_languageSkills = getCodesFromList(elise, "ct_lang");
			_languageSkillLevels = getCodesFromList(elise, "ct_slv");
			_publicationStatuses = getCodesFromList(elise, "ct_pls");
			_occupationMajorGroups = getCodesFromList(elise, "jobtitlemajor");
			_occupationSubMajorGroups = getCodesFromList(elise, "jobtitlesubmajor");
			_occupationMinorGroups = getCodesFromList(elise, "jobtitleminor");
			_occupationUnitGroups = getCodesFromList(elise, "jobtitleunit");
			_occupations = getCodesFromList(elise, "ct_occu");
			_jobTypes = getCodesFromList(elise, "ct_jobtyp");
			_industries = getCodesFromList(elise, "ct_ind");
			_sectors = getCodesFromList(elise, "ct_sec");
			_countries = getCodesFromList(elise, "ct_cty");
			_teleworkings = getCodesFromList(elise, "ct_twg");
			_partTimeOrFullTime = getCodesFromList(elise, "ct_wtm");
			_shiftTypes = getCodesFromList(elise, "ct_shf");
			_willingToTravels = getCodesFromList(elise, "ct_mbl");
			_jobsList = getJobList();

			// Generating bulk static data
			switch (args[0])
			{
			case "0":
				getJsOfferedPropertiesStaticData(offeredProperties);
				getJsDemandedPropertiesStaticData(demandedProperties);
				break;

			case "1":
				getJsOfferedPropertiesDistinctData(offeredProperties, elise);
				getJsDemandedPropertiesDistinctData(demandedProperties);
				break;

			case "2":
				getJsOfferedPropertiesProd(offeredProperties, elise, i);
				getJsDemandedPropertiesProd(demandedProperties, i);
				break;

			default:
				throw new RuntimeException("Invalid commandline argument");
			}

			dataObject.setKey(new Key(1, KeyType.INTEGER, i));

			InsertOptions options = new InsertOptions();
			options.setAlternateFunction(AlternateFunction.UPDATE);

			// Insert the DataObject.
			elise.insert(dataObject, options);
		}
	}

	private IPropertyCollection getJsOfferedPropertiesStaticData(IPropertyCollection properties)
	{

		properties.add("firstName", "James Brown");
		properties.add("lastName", "42");
		properties.add("email", "pippo@gmail.com");
		properties.add("gender", "1");
		properties.add("dateOfBirth", DateValue.create(new EliseDate(new Date())));
		properties.add("age", "30");
		properties.add("disabilities", "NO");
		properties.add("maritalStatus", "0");
		properties.add("children", "NO");
		properties.add("streetNameAndNumber", "Zonnebaan Riyadh");
		properties.add("postOfficeBox", "300");
		properties.add("zipCode", "3000");
		properties.add("city", "Riyadh");
		properties.add("province", "Dammam");
		properties.add("country", "SA");
		properties.add("longitude", "30");
		properties.add("latitude", "30");
		properties.add("distance", "30");
		properties.add("educationLevel", "DDG");
		//properties.add("educationLevel",getRandomValueFromList(educationList));
		//properties.add("educationMajorArea","4");
		//properties.add("educationMajorField","6");
		properties.add("educationMajorSpecificField", "4");
		properties.add("typeOfUniversity", "GOV");
		properties.add("hasDriversLicense", "YES");
		properties.add("totalYearsWorkExperience", "20");
		properties.add("totalYearsWorkExperienceList", "6");
		properties.add("industry", "HCARE");
		properties.add("sector", "CONSTR");
		//properties.add("Occupation", "1112001");

		// properties.add("OccupationMajorGroup","7");
		// properties.add("OccupationSubMajorGroup","21");
		// properties.add("OccupationMinorGroup","133");
		// properties.add("OccupationUnitGroup","1113");
		for (int i = 0; i < 5; i++)
		{
			properties.add("skill");
			properties.add("languageSkillName", "eu");
			properties.add("languageSkillLevel", "BEG");
		}

		properties.add("profileId", "21212");
		properties.add("registrationDate", DateValue.create(new EliseDate(new Date())));
		properties.add("daysSinceRegistration", "30");
		properties.add("availableFrom", DateValue.create(new EliseDate(new Date())));
		properties.add("authorizationStatus", "AUTHFA");
		properties.add("authorizationEndDate", DateValue.create(new EliseDate(new Date())));
		properties.add("publicationStatus", "PUB");

		/* properties.add("desiredOccupationMajorGroup","4");
		   properties.add("desiredOccupationSubMajorGroup","22");
		   properties.add("desiredOccupationMinorGroup","111");
		   properties.add("desiredOccupationUnitGroup","1120"); */

		for (int i = 0; i < 5; i++)
		{
			properties.add("job");
			properties.add("desiredJobProfileTitle", "Scuba diver");
			//properties.add("desiredOccupation", "911180");
			properties.add("desiredJobType", "INTERN");
			properties.add("desiredSalary", "90000");
			properties.add("desiredIndustry", "SECURY");
			properties.add("desiredSector", "FORGSCH");
		}

		properties.add("desiredCountry", "SA");

		for (int i = 0; i < 3; i++)
		{
			properties.add("desiredLocation");
			properties.add("desiredProvince", "Dammam");
			properties.add("desiredCity", "Riyadh");
		}

		properties.add("teleworking", "NOTELEW");
		properties.add("desiredPartTimeOrFullTime", "PART");
		properties.add("desiredShiftType", "3");
		properties.add("desiredWillingToTravel", "2");
		properties.add("keywords", staticKeywords);
		return properties;
	}

	public IPropertyCollection getJsDemandedPropertiesStaticData(IPropertyCollection properties1)
	{

		properties1.add("companyName", "ABC Ltd");
		properties1.add("industry", "FINSRV");
		properties1.add("distance", "10");
		properties1.add("teleworking", "NOTELEW");
		//properties1.add("OccupationUnitGroup","1120");
		//properties1.add("Occupation", "911180");
		properties1.add("salary", "90000");
		properties1.add("suitableForDisabilities", "NO");
		properties1.add("desiredGender", "M");
		properties1.add("partTimeOrFullTime", "PART");
		properties1.add("shiftType", "3");
		properties1.add("travelRequirement", "2");
		properties1.add("creationDate", DateValue.create(new EliseDate(new Date())));
		properties1.add("daysSinceCreation", "30");
		properties1.add("startDate", DateValue.create(new EliseDate(new Date())));
		properties1.add("daysSinceStartDate", "20");
		properties1.add("source", "3");
		properties1.add("postedBy", "2");
		properties1.add("languageSkillName", "eu");
		properties1.add("languageSkillLevel", "BEG");
		// properties1.add("requiredEducationLevel","DDG");
		// properties1.add("requiredEducationField","6");
		properties1.add("requiredEducationSpecificField", "4");
		properties1.add("requiredTypeOfUniversity", "GOV");
		return properties1;

	}

	private IPropertyCollection getJsOfferedPropertiesDistinctData(IPropertyCollection properties, IElise elise)
	{
		NameGenerator nameGen = new NameGenerator();
		properties.add("firstName", nameGen.getName());
		properties.add("lastName", nameGen.getName());
		properties.add("email", nameGen.getName() + "@gmail.com");
		properties.add("gender", getRandomGender());
		properties.add("dateOfBirth", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		// ** omit age in elise   properties.add("age",String.valueOf(randInt(18, 60)));
		properties.add("disabilities", getRandomYesNo());
		properties.add("maritalStatus", getRandomMaritalStatus());
		properties.add("children", getRandomYesNo());
		properties.add("streetNameAndNumber", nameGen.getName() + "straat " + String.valueOf(randInt(1, 900)));
		//properties.add("postOfficeBox",String.valueOf(randInt(1000, 9000)));
		properties.add("zipCode", String.valueOf(randInt(1000, 9000)));
		properties.add("city", getRandomCity());
		properties.add("province", getRandomProvince());
		properties.add("country", getRandomValueFromList(_countries));
		HashMap<String, String> coordinates = (HashMap<String, String>)getRandomCoordinates();
		properties.add("longitude", coordinates.get("longitude"));
		properties.add("latitude", coordinates.get("latitude").toString());
		// omit distance as age field - properties.add("distance",String.valueOf(randInt(1, 60)));
		/*properties.add("educationLevel",getRandomValueFromList(educationList));
		  properties.add("educationMajorArea",getRandomValueFromList(educationMajorAreaList));
		  properties.add("educationMajorField",getRandomValueFromList(educationMajorFieldList)); */
		properties.add("educationMajorSpecificField", getRandomValueFromList(_educationMajorSpecificFields));
		properties.add("typeOfUniversity", getRandomValueFromList(_typesOfUniversities));
		properties.add("hasDriversLicense", getRandomYesNo());
		properties.add("totalYearsWorkExperience", String.valueOf(randInt(1, 30)));
		properties.add("totalYearsWorkExperienceList", getRandomValueFromList(_totalYearsWorkExperiences));
		properties.add("industry", getRandomValueFromList(_industries));
		properties.add("sector", getRandomValueFromList(_sectors));
		/* properties.add("OccupationMajorGroup",getRandomValueFromList(occupationMajorGroupList));
		   properties.add("OccupationSubMajorGroup",getRandomValueFromList(occupationSubMajorGroupList));
		   properties.add("OccupationMinorGroup",getRandomValueFromList(occupationMinorGroupList));
		   properties.add("OccupationUnitGroup",getRandomValueFromList(occupationUnitGroupList)); */
		properties.add("Occupation", getRandomValueFromList(_occupations));
		for (int i = 0; i < 5; i++)
		{
			properties.add("skill");
			properties.add("languageSkillName", getRandomValueFromList(_languageSkills));
			properties.add("languageSkillLevel", getRandomValueFromList(_languageSkillLevels));
		}
		properties.add("profileId", String.valueOf(randInt(1, 10000)));
		properties.add("registrationDate", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		// properties.add("daysSinceRegistration",String.valueOf(randInt(1, 30)));
		properties.add("availableFrom", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		properties.add("authorizationStatus", "AUTHFA");
		properties.add("authorizationEndDate", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		properties.add("publicationStatus", getRandomValueFromList(_publicationStatuses));

		/*properties.add("desiredOccupationMajorGroup",getRandomValueFromList(occupationMajorGroupList));
		properties.add("desiredOccupationSubMajorGroup",getRandomValueFromList(occupationSubMajorGroupList));
		properties.add("desiredOccupationMinorGroup",getRandomValueFromList(occupationMinorGroupList));
		properties.add("desiredOccupationUnitGroup",getRandomValueFromList(occupationUnitGroupList));*/
		for (int i = 0; i < 5; i++)
		{
			properties.add("job");
			properties.add("desiredJobProfileTitle", getRandomValueFromList(_jobsList));
			properties.add("desiredOccupation", getRandomValueFromList(_occupations));
			properties.add("desiredJobType", getRandomValueFromList(_jobTypes));
			properties.add("desiredSalary", String.valueOf(randInt(20000, 100000)));
			properties.add("desiredIndustry", getRandomValueFromList(_industries));
			properties.add("desiredSector", getRandomValueFromList(_sectors));
		}
		properties.add("desiredCountry", getRandomValueFromList(_countries));
		for (int i = 0; i < 3; i++)
		{
			properties.add("desiredLocation");
			properties.add("desiredProvince", getRandomProvince());
			properties.add("desiredCity", getRandomCity());
		}

		properties.add("teleworking", getRandomValueFromList(_teleworkings));
		properties.add("desiredPartTimeOrFullTime", getRandomValueFromList(_partTimeOrFullTime));
		properties.add("desiredShiftType", getRandomValueFromList(_shiftTypes));
		properties.add("desiredWillingToTravel", getRandomValueFromList(_willingToTravels));
		String keywords = getKeywords();
		properties.add("keywords", keywords);
		return properties;
	}

	public IPropertyCollection getJsDemandedPropertiesDistinctData(IPropertyCollection properties1)
	{

		NameGenerator nameGen = new NameGenerator();
		properties1.add("companyName", nameGen.getName() + " Ltd");
		properties1.add("industry", getRandomValueFromList(_industries));
		properties1.add("distance", String.valueOf(randInt(1, 60)));
		properties1.add("teleworking", getRandomValueFromList(_teleworkings));
		// properties1.add("OccupationUnitGroup",getRandomValueFromList(occupationUnitGroupList));
		properties1.add("Occupation", getRandomValueFromList(_occupations));
		properties1.add("salary", String.valueOf(randInt(20000, 100000)));
		properties1.add("suitableForDisabilities", getRandomYesNo());
		//properties1.add("desiredGender", getRandomGender());
		properties1.add("partTimeOrFullTime", getRandomValueFromList(_partTimeOrFullTime));
		properties1.add("shiftType", getRandomValueFromList(_shiftTypes));
		properties1.add("creationDate", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		properties1.add("daysSinceCreation", String.valueOf(randInt(1, 30)));
		properties1.add("startDate", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		properties1.add("daysSinceStartDate", String.valueOf(randInt(1, 30)));
		properties1.add("languageSkillName", getRandomValueFromList(_languageSkills));
		properties1.add("languageSkillLevel", getRandomValueFromList(_languageSkillLevels));
		// properties1.add("requiredEducationLevel",getRandomValueFromList(educationList));
		// properties1.add("requiredEducationField",getRandomValueFromList(educationMajorFieldList));
		properties1.add("requiredEducationSpecificField", getRandomValueFromList(_educationMajorSpecificFields));
		properties1.add("requiredTypeOfUniversity", getRandomValueFromList(_typesOfUniversities));
		return properties1;

	}

	private IPropertyCollection getJsDemandedPropertiesProd(IPropertyCollection properties, int i)
	{
		return null;

	}

	private IPropertyCollection getJsOfferedPropertiesProd(IPropertyCollection properties, IElise elise, int i)
	{
		NameGenerator nameGen = new NameGenerator();
		float rand;

		properties.add("firstName", nameGen.getName());
		properties.add("lastName", nameGen.getName());
		properties.add("email", nameGen.getName() + "@gmail.com");
		properties.add("gender", getRandomGender());
		properties.add("dateOfBirth", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		properties.add("age", String.valueOf(randInt(18, 60)));
		//properties.add("disabilities",getRandomYesNo());
		properties.add("maritalStatus", getRandomMaritalStatus());
		//properties.add("children",getRandomYesNo());
		properties.add("streetNameAndNumber", nameGen.getName() + "straat " + String.valueOf(randInt(1, 900)));
		//properties.add("postOfficeBox",String.valueOf(randInt(1000, 9000)));
		//properties.add("zipCode",String.valueOf(randInt(1000, 9000)));
		rand = getRandomFloat();

		if (rand < 0.16)
		{
			properties.add("city", getRandomCity());
			properties.add("province", getRandomProvince());
			properties.add("country", "SA");
		}

		//HashMap<String, String> coordinates = (HashMap<String, String>) getRandomCoordinates();
		//properties.add("longitude",coordinates.get("longitude"));
		//properties.add("latitude",coordinates.get("latitude").toString());
		//properties.add("distance",String.valueOf(randInt(1, 60)));
		rand = getRandomFloat();

		if (rand < 0.70)
		{
			/* properties.add("educationLevel",getRandomValueFromList(educationList));
			 properties.add("educationMajorArea",getRandomValueFromList(educationMajorAreaList));
			 properties.add("educationMajorField",getRandomValueFromList(educationMajorFieldList));*/
			properties.add("educationMajorSpecificField", getRandomValueFromList(_educationMajorSpecificFields));
		}

		rand = getRandomFloat();
		if (rand < 0.15)
		{
			properties.add("typeOfUniversity", getRandomValueFromList(_typesOfUniversities));
		}
		//properties.add("hasDriversLicense",getRandomYesNo());
		//properties.add("totalYearsWorkExperience",String.valueOf(randInt(1, 30)));
		//properties.add("totalYearsWorkExperienceList",getRandomValueFromList(totalYearsWorkExperienceList));
		//properties.add("industry",getRandomValueFromList(industryList));
		//properties.add("sector",getRandomValueFromList(sectorList));
		rand = getRandomFloat();
		if (rand < 0.15)
		{
			/* properties.add("OccupationMajorGroup",getRandomValueFromList(occupationMajorGroupList));
			 properties.add("OccupationSubMajorGroup",getRandomValueFromList(occupationSubMajorGroupList));
			 properties.add("OccupationMinorGroup",getRandomValueFromList(occupationMinorGroupList));
			 properties.add("OccupationUnitGroup",getRandomValueFromList(occupationUnitGroupList));*/
			properties.add("Occupation", getRandomValueFromList(_occupations));
		}
		//properties.add("languageSkillName",getRandomValueFromList(languageSkillList));
		//properties.add("languageSkillLevel",getRandomValueFromList(languageSkillLevelList));
		properties.add("profileId", String.valueOf(randInt(1, 10000)));

		rand = getRandomFloat();
		if (rand < 0.70)
		{
			properties.add("registrationDate", DateValue.create(new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		}
		//properties.add("daysSinceRegistration",String.valueOf(randInt(1, 30)));

		/* properties.add("availableFrom", DateValue.create( new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		properties.add("authorizationStatus","AUTHFA");
		properties.add("authorizationEndDate",DateValue.create( new EliseDate(new Date(getRandomTimeBetweenTwoDates()))));
		properties.add("publicationStatus",getRandomValueFromList(publicationStatusList));
		*/
		rand = getRandomFloat();
		if (rand < 0.02)
		{
			properties.add("desiredJobProfileTitle", getRandomValueFromList(_jobsList));
			/*properties.add("desiredOccupationMajorGroup",getRandomValueFromList(occupationMajorGroupList));
			properties.add("desiredOccupationSubMajorGroup",getRandomValueFromList(occupationSubMajorGroupList));
			properties.add("desiredOccupationMinorGroup",getRandomValueFromList(occupationMinorGroupList));
			properties.add("desiredOccupationUnitGroup",getRandomValueFromList(occupationUnitGroupList));*/
			properties.add("desiredOccupation", getRandomValueFromList(_occupations));
			properties.add("desiredJobType", getRandomValueFromList(_jobTypes));
		}
		/*properties.add("desiredSalary",String.valueOf(randInt(20000, 100000)));
		properties.add("desiredIndustry",getRandomValueFromList(industryList));
		properties.add("desiredSector",getRandomValueFromList(sectorList));
		properties.add("desiredCountry",getRandomValueFromList(countryList));
		properties.add("desiredProvince",getRandomProvince());
		properties.add("desiredCity",getRandomCity());
		properties.add("teleworking",getRandomValueFromList(teleworkingList));*/
		properties.add("desiredPartTimeOrFullTime", getRandomValueFromList(_partTimeOrFullTime));
		properties.add("desiredShiftType", getRandomValueFromList(_shiftTypes));
		properties.add("desiredWillingToTravel", getRandomValueFromList(_willingToTravels));

		properties.add("keywords", getKeywords());

		return properties;
	}

	private static long getRandomTimeBetweenTwoDates()
	{
		long beginTime = Timestamp.valueOf("1960-01-01 00:00:00").getTime();
		long endTime = Timestamp.valueOf("2000-12-31 00:58:00").getTime();

		long diff = endTime - beginTime + 1;
		return beginTime + (long)(Math.random() * diff);
	}

	public static int randInt(int min, int max)
	{

		// NOTE: Usually this should be a field rather than a method
		// variable so that it is not re-seeded every call.
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt(max - min + 1) + min;

		return randomNum;
	}

	private static String getRandomMaritalStatus()
	{
		ArrayList<String> mslist = new ArrayList<String>();
		mslist.add("0");
		mslist.add("1");
		mslist.add("2");
		mslist.add("3");
		Collections.shuffle(mslist);
		return mslist.get(0);

	}

	private static String getRandomYesNo()
	{

		ArrayList<String> ynlist = new ArrayList<String>();
		ynlist.add("YES");
		ynlist.add("NO");
		Collections.shuffle(ynlist);
		return ynlist.get(0);

	}

	private static String getRandomGender()
	{

		ArrayList<String> glist = new ArrayList<String>();
		glist.add("1");
		glist.add("2");
		Collections.shuffle(glist);
		return glist.get(0);

	}

	private static String getRandomCity()
	{
		ArrayList<String> clist = new ArrayList<String>();
		clist.add("Riyadh");
		clist.add("Jeddah");
		clist.add("Medina");
		clist.add("Al-Ahsa");
		clist.add("Ta'if");
		clist.add("Dammam");
		clist.add("Khamis Mushait");
		clist.add("Buraidah");
		clist.add("Khobar");

		Collections.shuffle(clist);
		return clist.get(0);
	}

	private static String getRandomProvince()
	{
		ArrayList<String> clist = new ArrayList<String>();
		clist.add("Riyadh");
		clist.add("Makkah");
		clist.add("Al Madinah");
		clist.add("Eastern");
		clist.add("Makkah");
		clist.add("Al-Qassim");

		Collections.shuffle(clist);
		return clist.get(0);
	}

	private static Map<String, String> getRandomCoordinates()
	{
		HashMap<String, String> map = new HashMap<String, String>();
		double minLat = -90.00;
		double maxLat = 90.00;
		double latitude = minLat + Math.random() * (maxLat - minLat + 1);
		double minLon = 0.00;
		double maxLon = 180.00;
		double longitude = minLon + Math.random() * (maxLon - minLon + 1);
		DecimalFormat df = new DecimalFormat("#.#####");
		String latitudeStr = df.format(latitude);
		String longitudeStr = df.format(longitude);
		map.put("longitude", longitudeStr);
		map.put("latitude", latitudeStr);

		return map;

	}

	private static ArrayList<String> getEducationList(IElise elise)
	{
		IListValueSetList sets = elise.getLatestDictionary().getListValueSetList();

		IListValueSet list = sets.get("CT_EDC");

		ArrayList<String> elist = new ArrayList<String>();

		for (int i = 0; i < list.getCount(); i++)
		{
			elist.add(list.get(i).getCode());
		}
		return elist;
	}

	private static ArrayList<String> getCodesFromList(IElise elise, String listname)
	{
		IListValueSet list = elise.getLatestDictionary().getListValueSetList().get(listname);
		ArrayList<String> elist = new ArrayList<String>();

		for (int i = 0; i < list.getCount(); i++)
		{
			elist.add(list.get(i).getCode());
		}
		return elist;
	}

	private static String getRandomValueFromList(ArrayList<String> list)
	{
		Collections.shuffle(list);
		return list.get(0);
	}

	private static ArrayList<String> getJobList()
	{

		Scanner s = null;
		try
		{
			s = new Scanner(new File("c:\\elise\\jobs.txt")).useDelimiter("\n");
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<String> list = new ArrayList<String>();
		while (s.hasNext())
		{
			list.add(s.next());
		}
		s.close();
		return list;
	}

	private static float getRandomFloat()
	{

		float minX = 0.00f;
		float maxX = 1.00f;

		Random rand = new Random();

		float finalX = rand.nextFloat() * (maxX - minX) + minX;
		return finalX;
	}

	private static String getKeywords()
	{
		NameGenerator nameGen = new NameGenerator();
		String keywords = "";
		for (int i = 0; i < 30; i++)
		{
			keywords += nameGen.getName() + " ";
		}
		return keywords;
	}

}
