/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-05  Javlin Consulting <info@javlinconsulting.cz>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util.string;

import java.util.HashMap;
import java.util.Map;

/**
 * In this class are stored rules for StringAproxComparator for different locale
 * Rules for given locale are stored in array of Strings: 
 * 	each String in the array have to be like "c1=c2=..=cn", where c1,c2,...,cn are chars which will be considered as equivalent (no white space is allowed)
 * After definition new rules don't forget to put it in the HashMap rules
 * 
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created August 17, 2006 
 */
public class StringAproxComparatorLocaleRules {

	private static Map rules=new HashMap();

	public static final String[] CZ_RULES={
		"a=á=A=�?",
		"c=č=C=Č",
		"d=ď=D=Ď",
		"e=é=ě=E=É=Ě",
		"i=í=I=Í",
		"n=�?=N=Ň",
		"o=ó=O=Ó",
		"r=ř=R=�?",
		"s=š=S=Š",
		"t=ť=T=Ť",
		"u=ů=ú=U=Ů=Ú",
		"y=ý=Y=Ý",
		"z=ž=Z=Ž"
		};
	
	public static final String[] PL_RULES={
		"a=ą=A=Ą",
		"c=ć=C=Ć",
		"e=ę=E=�?",
		"l=ł=L=�?",
		"n=ń=N=�?",
		"o=ó=O=Ó",
		"s=ś=S=Ś",
		"z=ż=ź=Z=Ż=Ź"
	};

	/**
	 * Static initalization for rules HashMap
	 */
	static {
		rules.put("CZ",CZ_RULES);
		rules.put("CS",CZ_RULES);
		rules.put("PL",PL_RULES);
	}
	
	
	/**
	 * Method for getting rules for given locale
	 * @param locale
	 * @return
	 * @throws NoSuchFieldException
	 */
	public static String getRules(String locale) throws NoSuchFieldException{
		String[] rule=(String[])rules.get(locale.substring(0,2).toUpperCase());
		if (rule!=null){
			StringBuffer result=new StringBuffer(6);
			for (int i=0;i<rule.length;i++){
				result.append("& ");
				result.append(rule[i]);
			}
			return result.toString();
		}else {
			throw new NoSuchFieldException("No field for this locale or wrong locale format");
		}
		
	}

	/**
	 * This method gets out avaible locale
	 */
	public static String[] getAvaibleLocales(){
		return (String[]) rules.keySet().toArray(new String[0]);
	}
}
