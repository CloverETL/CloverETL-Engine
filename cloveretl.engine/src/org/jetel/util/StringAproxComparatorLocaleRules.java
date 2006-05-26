package org.jetel.util;

import java.util.HashMap;
import java.util.Map;

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
 	
	
	public static String[] getRules(String locale) throws NoSuchFieldException{
		rules.put("CZ.cz",CZ_RULES);
		rules.put("PL.pl",PL_RULES);
		String[] r=(String[])rules.get(locale);
		if (r!=null){
			return r;
		}else {
			throw new NoSuchFieldException("No field for this locale or wrong locale format");
		}
		
	}

}
