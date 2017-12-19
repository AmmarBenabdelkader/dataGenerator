/**
 * @author abenabdelkader
 *
 * testing.java
 * Oct 7, 2015
 */
package com.wccgroup.elise.testdata;

public interface Offered
{

	public class Candidate implements Offered
	{
		String Name = "";
	}

	public class Skills extends Candidate
	{
	}

	public class Languages extends Candidate
	{
	}

	Candidate can = new Candidate();
}