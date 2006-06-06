package org.jetel.util;

import java.util.HashMap;
import java.util.Map;

/**
 * In this class are stored rules for StringAproxComparator for difrent locale
 * Rules for given locale are stored in array of Strings: 
 * 	each String in the array have to be like "c1=c2=..=cn", where c1,c2,...,cn are chars which will be considered as equivalent (no white space is allowed)
 * After definition new rules don't forget to put it in the HashMap rules
 * @author avackova
 *
 */
public class StringAproxComparatorLocaleRules {

	private static Map rules=new HashMap();

	public static final String[] CZ_RULES={
		"a=á=A=Á",
		"c=č=C=Č",
		"d=ď=D=Ď",
		"e=é=ě=E=É=Ě",
		"i=í=I=Í",
		"n=ň=N=Ň",
		"o=ó=O=Ó",
		"r=ř=R=Ř",
		"s=š=S=Š",
		"t=ť=T=Ť",
		"u=ů=ú=U=Ů=Ú",
		"y=ý=Y=Ý",
		"z=ž=Z=Ž"
		};
	
	public static final String[] PL_RULES={
		"a=ą=A=Ą",
		"c=ć=C=Ć",
		"e=ę=E=Ę",
		"l=ł=L=Ł",
		"n=ń=N=Ń",
		"o=ó=O=Ó",
		"s=ś=S=Ś",
		"z=ż=ź=Z=Ż=Ź"
	};

	/**
	 * Static initalization for rules HashMap
	 */
	static {
		rules.put("CZ",CZ_RULES);
		rules.put("PL",PL_RULES);
	}
	
	
	/**
	 * Method for getting rules for given locale
	 * @param locale
	 * @return
	 * @throws NoSuchFieldException
	 */
	public static String getRules(String locale) throws NoSuchFieldException{
		String[] r=(String[])rules.get(locale.substring(0,2).toUpperCase());
		if (r!=null){
			StringBuffer result=new StringBuffer(6);
			for (int i=0;i<r.length;i++){
				result.append("& ");
				result.append(r[i]);
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
