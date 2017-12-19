/**
 * @author abenabdelkader
 *
 * testing.java
 * Oct 7, 2015
 */
package com.wccgroup.elise.testdata;

import java.util.List;

public class Occupation
{

	public int num;
	public String id;
	public String concept;
	public String OC;
	public double score;
	public double match;
	public List<String> descr;
	public List<String> labels;
	public List<String> classes;
	public List<String> synonyms;
	public List<String> parents;
	public List<String> childs;
	public List<String> similars;
	public List<String> others;
	public List<String> skills;
	public List<String> softskills;

	public Occupation(int num, String id, String concept, String OC, Double score)
	{
		this.num = num;
		this.id = id;
		this.concept = concept;
		this.score = score;
	}

	public void setDescr(List<String> descr)
	{
		this.descr = descr;
	}

	public void setLabels(List<String> labels)
	{
		this.labels = labels;
	}

	public void setOC(String oc)
	{
		OC = oc;
	}

	public void setMatch(double match)
	{
		this.match = match;
	}

	public void setClasses(List<String> classes)
	{
		this.classes = classes;
	}

	public void setSynonyms(List<String> synonyms)
	{
		this.synonyms = synonyms;
	}

	public void setParents(List<String> parents)
	{
		this.parents = parents;
	}

	public void setChilds(List<String> childs)
	{
		this.childs = childs;
	}

	public void setSimilars(List<String> similars)
	{
		this.similars = similars;
	}

	public void setOthers(List<String> others)
	{
		this.others = others;
	}

	public void setSkills(List<String> skills)
	{
		this.skills = skills;
	}

	public void setSoftSkills(List<String> softskills)
	{
		this.softskills = softskills;
	}

}
