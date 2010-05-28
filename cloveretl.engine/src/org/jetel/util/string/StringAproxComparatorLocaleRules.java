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
//	cs, , ??e??tina
//	cs, CZ, ??e??tina (??esk?? republika)
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
		"a=??=A=??",
		"c=??=C=??",
		"d=??=D=??",
		"e=??=??=E=??=??",
		"i=??=I=??",
		"n=??=N=??",
		"o=??=O=??",
		"r=??=R=??",
		"s=??=S=??",
		"t=??=T=??",
		"u=??=??=U=??=??",
		"y=??=Y=??",
		"z=??=Z=??"
		};
	
	private static final String[] PL_RULES={
		"a=??=A=??",
		"c=??=C=??",
		"e=??=E=??",
		"l=??=L=??",
		"n=??=N=??",
		"o=??=O=??",
		"s=??=S=??",
		"z=??=??=Z=??=??"
	};

	private static final String[] ES_RULES={
		"a=??=A=??",
		"e=??=E=??",
		"i=??=I=??",
		"o=??=O=??",
		"u=??=??=U=??=??",
		"n=??=N=??"
	};

	private static final String[] FR_RULES={
		"a=??=??=A=??=??",
		"e=??=??=??=??=E=??=??=??=??",
		"i=??=I=??",
		"o=??=O=??",
		"u=??=??=??=U=??=??=??",
		"c=??=C=??"
	};
	
	private static final String[] SK_RULES={
		"a=??=??=A=??=??",
		"c=??=C=??",
		"d=??=D=??",
		"e=??=E=??",
		"i=??=I=??",
		"l=??=??=L=??=??",
		"n=??=N=??",
		"o=??=??=O=??=??",
		"r=??=R=??",
		"s=??=S=??",
		"t=??=T=??",
		"u=??=U=??",
		"y=??=Y=??",
		"z=??=Z=??"
	};

	private static final String[] HU_RULES={
		"a=??=A=??",
		"e=??=E=??",
		"i=??=I=??",
		"o=??=??=??=O=??=??=??",
		"u=??=??=??=U=??=??=??"
	};

	private static final String[] IS_RULES={//Icelandic
		"a=??=??=A=??=??",
		"e=??=E=??",
		"i=??=I=??",
		"o=??=??=O=??=??",
		"u=??=U=??",
		"y=??=Y=??",
		"d=??=D=??"
	};	

	private static final String[] DA_NO_RULES={//Danish and Norwegian
		"a=??=??=A=??=??",
		"o=??=O=??"
	};

	private static final String[] SV_RULES={//Swedish
		"a=??=??=A=??=??",
		"o=??=O=??"
	};

	private static final String[] FI_RULES={//Finnish
		"a=??=A=??",
		"o=??=O=??"
	};
	
	private static final String[] ET_RULES={//Estonian
		"a=??=A=??",
		"o=??=??=O=??=??",
		"u=??=U=??",
		"s=??=S=??",
		"z=??=Z=??"
	};
	
	private static final String[] LV_RULES={//Latvian
		"a=??=A=??",
		"e=??=E=??",
		"i=??=I=??",
		"u=??=U=??",
		"c=??=C=??",
		"g=??=G=??",
		"k=??=K=??",
		"l=??=L=??",
		"n=??=N=??",
		"s=??=S=??",
		"z=??=Z=??"
	};
	
	private static final String[] LT_RULES={//Lithuanian
		"a=??=A=??",
		"e=??=??=E=??=??",
		"i=??=I=??",
		"u=??=??=U=??=??",
		"c=??=C=??",
		"s=??=S=??",
		"z=??=Z=??"
	};

	private static final String[] DE_RULES={//German
		"a=??=A=??",
		"o=??=O=??",
		"u=??=U=??"
	};

	private static final String[] IT_RULES={//Italian
		"a=??=A=??",
		"e=??=??=E=??=??",
		"i=??=I=??",
		"o=??=O=??",
		"u=??=U=??"
	};
	
	private static final String[] SL_RULES={//Slovenian
		"c=??=C=??",
		"s=??=S=??",
		"z=??=Z=??"
	};
	
	private static final String[] HR_RULES={//Croatian
		"c=??=??=C=??=??",
		"d=??=D=??",
		"s=??=S=??",
		"z=??=Z=??"
	};
	
	private static final String[] RO_RULES={//Romanian
		"a=??=??=A=??=??",
		"i=??=I=??",
		"s=??=S=??",
		"t=??=T=??"
	};

	private static final String[] PT_RULES={//Portuguese
		"a=??=??=??=??=A=??=??=??=??",
		"e=??=??=E=??=??",
		"i=??=I=??",
		"o=??=??=??=O=??=??=??",
		"u=??=U=??",
		"c=??=C=??"
	};
	
	private static final String[] CA_RULES={//Catalan
		"a=??=A=??",
		"e=??=??=E=??=??",
		"i=??=??=I=??=??",
		"o=??=??=O=??=??",
		"u=??=??=U=??=??",
		"c=??=C=??"
	};

	private static final String[] SQ_RULES={//Albanian
		"e=??=E=??",
		"c=??=C=??"
	};
	
	private static final String[] TR_RULES={//Turkish
		"??=i=??=I",
		"o=??=O=??",
		"u=??=U=??",
		"c=??=C=??",
		"g=??=G=??",
		"s=??=S=??"
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
