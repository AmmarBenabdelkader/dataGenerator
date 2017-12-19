/**
 * @author abenabdelkader
 *
 * testing.java
 * Oct 7, 2015
 */
package com.wccgroup.elise.testdata;

import java.util.Comparator;

public class comp implements Comparator<String>
{
	@Override
	public int compare(String o1, String o2)
	{
		if (o1.length() > o2.length())
		{
			return -1;
		}
		else if (o1.length() < o2.length())
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}
}
