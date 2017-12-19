/**
 * @author abenabdelkader
 *
 * URLReader.java
 * Aug 14, 2017
 */
package com.wccgroup.elise.testdata_ssoc;

/**
 * @author abenabdelkader
 *
 */
import java.net.*;
import java.util.StringTokenizer;
import java.io.*;

public class URLReader {
    public static void main(String[] args) throws Exception {
    	
    	String baseURI="http://beam.biz/";

    	processAllVacancies();
    	if (1==1)
    		return;
    	
        URL oracle = new URL("file://savannah/home/abenabdelkader/Documents/projects/testdata/ssoc/beam_biz_vacancies.html");
        //URL oracle = new URL("http://beam.biz/jobs/keywords/Singapore");
        BufferedReader in = new BufferedReader(
        new InputStreamReader(oracle.openStream()));

        String inputLine;
        int i = 1;
        String jobtitle,joblink;
        int j=1;
        while ((inputLine = in.readLine()) != null) {
        	if (i==35) {
        		System.out.println("i=" + i + "\n" + inputLine);
        		int idx1 = inputLine.indexOf("\"><a href=\"job/");
        	     while (idx1>0) { 
        	    	 joblink = inputLine.substring(idx1+11, inputLine.indexOf("\" style=\"",idx1));
        	    	 int idx2=inputLine.indexOf("t:normal;\">",idx1);
        	    	 jobtitle = inputLine.substring(idx2+11, inputLine.indexOf("</a></p>",idx2));
        	         System.out.println(j + "\t" + jobtitle + "\t" + baseURI+joblink);  
             		idx1 = inputLine.indexOf("\"><a href=\"job/", idx2);
             		
                    URL vacancy = new URL(baseURI + joblink);
                    BufferedReader vacancy_in = new BufferedReader(
                    new InputStreamReader(vacancy.openStream()));
                    String inputLine2;
                    int i2=1;
                    while ((inputLine2 = vacancy_in.readLine()) != null) {
                    	if (i2==35) {
                    		//System.out.println("i=" + i + "\n" + inputLine2);
                    		int idx1a = inputLine2.indexOf(">Job Description<");
                    		idx1a = inputLine2.indexOf("padding-bottom:20px;\">", idx1a);
                    	     if (idx1a>0) { 
                    	    	 joblink = inputLine2.substring(idx1a+22, inputLine2.length());
                    	    	 while (vacancy_in.readLine().equalsIgnoreCase("<br />"))
                    	    		 joblink+=vacancy_in.readLine();
                    	    	 //joblink = inputLine2.substring(idx1a+11, inputLine2.indexOf("</p></div><div",idx1a));
                    	    	 int idx2a=inputLine2.indexOf("</p></div><div",idx1a);
                    	    	 //jobtitle = inputLine2.substring(idx2a+10, inputLine2.indexOf("</a></p>",idx2a));
                    	         System.out.println("\t" + joblink + "\n" + vacancy_in.readLine());  
                         		idx1a = inputLine2.indexOf(">Job Description<", idx2a);
                         		
                    	     }
                    	}
                    	i2++;
                    }
             		j++;
       	     }  

        		}
        	i++;
        }
        
        in.close();
    }
    public static void processAllVacancies() throws IOException {
    	URL oracle = new URL("file://savannah/home/abenabdelkader/Documents/projects/testdata/ssoc/beam_biz_vacancies2.html");
    	//URL oracle = new URL("http://beam.biz/jobs/keywords/Singapore");
    	BufferedReader in = new BufferedReader(
    		new InputStreamReader(oracle.openStream()));

    	String inputLine;
    	int i = 1;
    	int j=1;
    	while ((inputLine = in.readLine()) != null) {
    		System.out.println(j + "\t" + inputLine);  
    		in.readLine();
    		in.readLine();
    		in.readLine();

    		j++;
    	}

    	in.close();
    }
}
