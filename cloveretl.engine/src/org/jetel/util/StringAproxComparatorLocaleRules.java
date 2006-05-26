package org.jetel.util;

public class StringAproxComparatorLocaleRules {

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
	
	public static String[] getRules(String locale){
		//TODO
		return CZ_RULES;
	}

}
