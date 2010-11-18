/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.util.string;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

//	ar, , Arabic
//	ar, AE, Arabic (United Arab Emirates)
//	ar, BH, Arabic (Bahrain)
//	ar, DZ, Arabic (Algeria)
//	ar, EG, Arabic (Egypt)
//	ar, IQ, Arabic (Iraq)
//	ar, JO, Arabic (Jordan)
//	ar, KW, Arabic (Kuwait)
//	ar, LB, Arabic (Lebanon)
//	ar, LY, Arabic (Libya)
//	ar, MA, Arabic (Morocco)
//	ar, OM, Arabic (Oman)
//	ar, QA, Arabic (Qatar)
//	ar, SA, Arabic (Saudi Arabia)
//	ar, SD, Arabic (Sudan)
//	ar, SY, Arabic (Syria)
//	ar, TN, Arabic (Tunisia)
//	ar, YE, Arabic (Yemen)
//	be, , Belarusian
//	be, BY, Belarusian (Belarus)
//	bg, , Bulgarian
//	bg, BG, Bulgarian (Bulgaria)
//	ca, , Catalan
//	ca, ES, Catalan (Spain)
//	cs, , čeština
//	cs, CZ, čeština (Česká republika)
//	da, , Danish
//	da, DK, Danish (Denmark)
//	de, , German
//	de, AT, German (Austria)
//	de, CH, German (Switzerland)
//	de, DE, German (Germany)
//	de, LU, German (Luxembourg)
//	el, , Greek
//	el, CY, Greek (Cyprus)
//	el, GR, Greek (Greece)
//	en, , English
//	en, AU, English (Australia)
//	en, CA, English (Canada)
//	en, GB, English (United Kingdom)
//	en, IE, English (Ireland)
//	en, IN, English (India)
//	en, MT, English (Malta)
//	en, NZ, English (New Zealand)
//	en, PH, English (Philippines)
//	en, SG, English (Singapore)
//	en, US, English (United States)
//	en, ZA, English (South Africa)
//	es, , Spanish
//	es, AR, Spanish (Argentina)
//	es, BO, Spanish (Bolivia)
//	es, CL, Spanish (Chile)
//	es, CO, Spanish (Colombia)
//	es, CR, Spanish (Costa Rica)
//	es, DO, Spanish (Dominican Republic)
//	es, EC, Spanish (Ecuador)
//	es, ES, Spanish (Spain)
//	es, GT, Spanish (Guatemala)
//	es, HN, Spanish (Honduras)
//	es, MX, Spanish (Mexico)
//	es, NI, Spanish (Nicaragua)
//	es, PA, Spanish (Panama)
//	es, PE, Spanish (Peru)
//	es, PR, Spanish (Puerto Rico)
//	es, PY, Spanish (Paraguay)
//	es, SV, Spanish (El Salvador)
//	es, US, Spanish (United States)
//	es, UY, Spanish (Uruguay)
//	es, VE, Spanish (Venezuela)
//	et, , Estonian
//	et, EE, Estonian (Estonia)
//	fi, , Finnish
//	fi, FI, Finnish (Finland)
//	fr, , French
//	fr, BE, French (Belgium)
//	fr, CA, French (Canada)
//	fr, CH, French (Switzerland)
//	fr, FR, French (France)
//	fr, LU, French (Luxembourg)
//	ga, , Irish
//	ga, IE, Irish (Ireland)
//	hi, IN, Hindi (India)
//	hr, , Croatian
//	hr, HR, Croatian (Croatia)
//	hu, , Hungarian
//	hu, HU, Hungarian (Hungary)
//	in, , Indonesian
//	in, ID, Indonesian (Indonesia)
//	is, , Icelandic
//	is, IS, Icelandic (Iceland)
//	it, , Italian
//	it, CH, Italian (Switzerland)
//	it, IT, Italian (Italy)
//	iw, , Hebrew
//	iw, IL, Hebrew (Israel)
//	ja, , Japanese
//	ja, JP, Japanese (Japan)
//	ja, JP, Japanese (Japan,JP)
//	ko, , Korean
//	ko, KR, Korean (South Korea)
//	lt, , Lithuanian
//	lt, LT, Lithuanian (Lithuania)
//	lv, , Latvian
//	lv, LV, Latvian (Latvia)
//	mk, , Macedonian
//	mk, MK, Macedonian (Macedonia)
//	ms, , Malay
//	ms, MY, Malay (Malaysia)
//	mt, , Maltese
//	mt, MT, Maltese (Malta)
//	nl, , Dutch
//	nl, BE, Dutch (Belgium)
//	nl, NL, Dutch (Netherlands)
//	no, , Norwegian
//	no, NO, Norwegian (Norway)
//	no, NO, Norwegian (Norway,Nynorsk)
//	pl, , Polish
//	pl, PL, Polish (Poland)
//	pt, , Portuguese
//	pt, BR, Portuguese (Brazil)
//	pt, PT, Portuguese (Portugal)
//	ro, , Romanian
//	ro, RO, Romanian (Romania)
//	ru, , Russian
//	ru, RU, Russian (Russia)
//	sk, , Slovak
//	sk, SK, Slovak (Slovakia)
//	sl, , Slovenian
//	sl, SI, Slovenian (Slovenia)
//	sq, , Albanian
//	sq, AL, Albanian (Albania)
//	sr, , Serbian
//	sr, BA, Serbian (Bosnia and Herzegovina)
//	sr, CS, Serbian (Serbia and Montenegro)
//	sr, ME, Serbian (Montenegro)
//	sr, RS, Serbian (Serbia)
//	sv, , Swedish
//	sv, SE, Swedish (Sweden)
//	th, , Thai
//	th, TH, Thai (Thailand)
//	th, TH, Thai (Thailand,TH)
//	tr, , Turkish
//	tr, TR, Turkish (Turkey)
//	uk, , Ukrainian
//	uk, UA, Ukrainian (Ukraine)
//	vi, , Vietnamese
//	vi, VN, Vietnamese (Vietnam)
//	zh, , Chinese
//	zh, CN, Chinese (China)
//	zh, HK, Chinese (Hong Kong)
//	zh, SG, Chinese (Singapore)
//	zh, TW, Chinese (Taiwan)

	private static Map<String, String[]> rules=new HashMap<String, String[]>();

	private static final String[] CZ_RULES={
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
	
	private static final String[] PL_RULES={
		"a=ą=A=Ą",
		"c=ć=C=Ć",
		"e=ę=E=Ę",
		"l=ł=L=Ł",
		"n=ń=N=Ń",
		"o=ó=O=Ó",
		"s=ś=S=Ś",
		"z=ż=ź=Z=Ż=Ź"
	};

	private static final String[] ES_RULES={
		"a=á=A=Á",
		"e=é=E=É",
		"i=í=I=Í",
		"o=ó=O=Ó",
		"u=ú=ü=U=Ú=Ü",
		"n=ñ=N=Ñ"
	};

	private static final String[] FR_RULES={
		"a=à=â=A=À=Â",
		"e=è=é=ê=ë=E=È=É=Ê=Ë",
		"i=ï=I=Ï",
		"o=ô=O=Ô",
		"u=ù=û=ü=U=Ù=Û=Ü",
		"c=ç=C=Ç"
	};
	
	private static final String[] SK_RULES={
		"a=á=ä=A=Á=Ä",
		"c=č=C=Č",
		"d=ď=D=Ď",
		"e=é=E=É",
		"i=í=I=Í",
		"l=ĺ=ľ=L=Ĺ=Ľ",
		"n=ň=N=Ň",
		"o=ó=ô=O=Ó=Ô",
		"r=ŕ=R=Ŕ",
		"s=š=S=Š",
		"t=ť=T=Ť",
		"u=ú=U=Ú",
		"y=ý=Y=Ý",
		"z=ž=Z=Ž"
	};

	private static final String[] HU_RULES={
		"a=á=A=Á",
		"e=é=E=É",
		"i=í=I=Í",
		"o=ó=ö=ő=O=Ó=Ö=Ő",
		"u=ú=ü=ű=U=Ú=Ü=Ű"
	};

	private static final String[] IS_RULES={//Icelandic
		"a=á=æ=A=Á=Æ",
		"e=é=E=É",
		"i=í=I=Í",
		"o=ó=ö=O=Ó=Ö",
		"u=ú=U=Ú",
		"y=ý=Y=Ý",
		"d=ð=D=Ð"
	};	

	private static final String[] DA_NO_RULES={//Danish and Norwegian
		"a=æ=å=A=Æ=Å",
		"o=ø=O=Ø"
	};

	private static final String[] SV_RULES={//Swedish
		"a=ä=å=A=Ä=Å",
		"o=ö=O=Ö"
	};

	private static final String[] FI_RULES={//Finnish
		"a=ä=A=Ä",
		"o=ö=O=Ö"
	};
	
	private static final String[] ET_RULES={//Estonian
		"a=ä=A=Ä",
		"o=ö=õ=O=Ö=Õ",
		"u=ü=U=Ü",
		"s=š=S=Š",
		"z=ž=Z=Ž"
	};
	
	private static final String[] LV_RULES={//Latvian
		"a=ā=A=Ā",
		"e=ē=E=Ē",
		"i=ī=I=Ī",
		"u=ū=U=Ū",
		"c=č=C=Č",
		"g=ģ=G=Ģ",
		"k=ķ=K=Ķ",
		"l=ļ=L=Ļ",
		"n=ņ=N=Ņ",
		"s=š=S=Š",
		"z=ž=Z=Ž"
	};
	
	private static final String[] LT_RULES={//Lithuanian
		"a=ą=A=Ą",
		"e=ę=ė=E=Ę=Ė",
		"i=į=I=Į",
		"u=ų=ū=U=Ų=Ū",
		"c=č=C=Č",
		"s=š=S=Š",
		"z=ž=Z=Ž"
	};

	private static final String[] DE_RULES={//German
		"a=ä=A=Ä",
		"o=ö=O=Ö",
		"u=ü=U=Ü"
	};

	private static final String[] IT_RULES={//Italian
		"a=à=A=À",
		"e=é=è=E=É=È",
		"i=ì=I=Ì",
		"o=ò=O=Ò",
		"u=ù=U=Ù"
	};
	
	private static final String[] SL_RULES={//Slovenian
		"c=č=C=Č",
		"s=š=S=Š",
		"z=ž=Z=Ž"
	};
	
	private static final String[] HR_RULES={//Croatian
		"c=ć=č=C=Ć=Č",
		"d=đ=D=Đ",
		"s=š=S=Š",
		"z=ž=Z=Ž"
	};
	
	private static final String[] RO_RULES={//Romanian
		"a=ă=â=A=Ă=Â",
		"i=î=I=Î",
		"s=ş=S=Ş",
		"t=ţ=T=Ţ"
	};

	private static final String[] PT_RULES={//Portuguese
		"a=ã=á=à=â=A=Ã=Á=À=Â",
		"e=é=ê=E=É=Ê",
		"i=í=I=Í",
		"o=õ=ó=ô=O=Õ=Ó=Ô",
		"u=ú=U=Ú",
		"c=ç=C=Ç"
	};
	
	private static final String[] CA_RULES={//Catalan
		"a=à=A=À",
		"e=è=é=E=È=É",
		"i=í=ï=I=Í=Ï",
		"o=ò=ó=O=Ò=Ó",
		"u=ú=ü=U=Ú=Ü",
		"c=ç=C=Ç"
	};

	private static final String[] SQ_RULES={//Albanian
		"e=ë=E=Ë",
		"c=ç=C=Ç"
	};
	
	private static final String[] TR_RULES={//Turkish
		"ı=i=İ=I",
		"o=ö=O=Ö",
		"u=ü=U=Ü",
		"c=ç=C=Ç",
		"g=ğ=G=Ğ",
		"s=ş=S=Ş"
	};

	private static final String[] RU_RULES={//Russian
		"Е=е=Ё=ё=É=é",
		"И=и=Й=й=И҆=и́",
		"O=o=Ó=ó",
		"А=а=Á=á",
		"Ы=ы=Ы҆=ы҆",
		"Я=я=Я҆=я́",
		"Ю=ю=Ю҆=ю҆",
		"У=у=У҆=у́",
		"Э=э=Э҆=э́"
	};
	
	/**
	 * Static initalization for rules HashMap
	 */
	static {
		rules.put("CZ",CZ_RULES);
		rules.put("CS",CZ_RULES);
		
		rules.put("AR",ES_RULES);
		rules.put("BO",ES_RULES);
		rules.put("CL",ES_RULES);
		rules.put("CO",ES_RULES);
		rules.put("CR",ES_RULES);
		rules.put("DO",ES_RULES);
		rules.put("SV",ES_RULES);
		rules.put("GT",ES_RULES);
		rules.put("HN",ES_RULES);
		rules.put("MX",ES_RULES);
		rules.put("NI",ES_RULES);
		rules.put("PA",ES_RULES);
		rules.put("PY",ES_RULES);
		rules.put("PE",ES_RULES);
		rules.put("PR",ES_RULES);
		rules.put("ES",ES_RULES);
		rules.put("UY",ES_RULES);
		rules.put("VE",ES_RULES);
		
		rules.put("BE",FR_RULES);
		rules.put("CA",FR_RULES);
		rules.put("FR",FR_RULES);
		rules.put("LU",FR_RULES);
		rules.put("CH",FR_RULES);
		
		rules.put("PL",PL_RULES);
		
		rules.put("RU",RU_RULES);
		
		rules.put("HU",HU_RULES);

		rules.put("SK",SK_RULES);
		
		rules.put("IS",IS_RULES);
		
		rules.put("DK",DA_NO_RULES);
		rules.put("NO",DA_NO_RULES);

		rules.put("SE",SV_RULES);
		
		rules.put("FI",FI_RULES);
		
		rules.put("EE",ET_RULES);

		rules.put("LV",LV_RULES);

		rules.put("LT",LT_RULES);

		rules.put("AT",DE_RULES);
		rules.put("CH",DE_RULES);
		rules.put("DE",DE_RULES);
		rules.put("LU",DE_RULES);

		rules.put("CH",IT_RULES);
		rules.put("IT",IT_RULES);

		rules.put("SI",SL_RULES);

		rules.put("HR",HR_RULES);

		rules.put("RO",RO_RULES);

		rules.put("BR",PT_RULES);
		rules.put("PT",PT_RULES);

		rules.put("ES_ca",CA_RULES); // the same code as Spain

		rules.put("AL",SQ_RULES);

		rules.put("TR",TR_RULES);

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
		final Set<String> rulesSet = rules.keySet();
		return (String[]) rulesSet.toArray(new String[rulesSet.size()]);
	}
}
