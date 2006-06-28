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
package org.jetel.util;

import java.text.CollationKey;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.HashMap;
import java.util.Locale;

import org.jetel.exception.JetelException;

/**
 * Class for aproximative string comparison
 * 
 * @author avackova
 *
 */

public class StringAproxComparator{

	//strentgh fields
	public final static int  IDENTICAL=4;
	public final static int TERTIARY=3; //upper case = lower case
	public final static int SECONDARY=2;//diacrtic letters = letters witout diacrityk (locale dependent)
			// now done only for CZ and PL
	public final static int PRIMARY=1;// mistakes acceptable (Collator.PRIMARY,new Locale("en","UK"))
	
	private int strentgh;
	private boolean IDEN; 	//if comparator on level IDENTICAL works
	private boolean TER; 	//if comparator on level TERTIARY works
	private boolean SEC; 	//if comparator on level SECONDARY works
	private boolean PRIM;	//if comparator on level PRIMARY works

	private int maxLettersToChange=3; 
	private int delMultiplier=1;
	private int changeMultiplier=1;
	
	private int substCost;// cost of char substituting of one letter
	private int changeCost;// = substitution cost on strongest level * changeMultiplier- depends on strenth and values of IDEN,TER,SEC fields
	private int delCost; // = substitution cost on strongest level*delMultiplier
	private int maxDiffrence;// = substitution cost on strongest level * maxLettersToChange

	//arrays for method distance
	char[] schars;
	char[] tchars;
	int[] slast = null;
	int[] tlast = null;
	int[] tblast = null;
	int[] now = null;

	Collator en_col = Collator.getInstance(Locale.US);
	Collator col	=Collator.getInstance();
	private String locale=null;
	
	//Comparators factory
	private static HashMap comparators = new HashMap();
	
	public static StringAproxComparator createComparator(String locale,boolean[] strenght)
			throws JetelException{
		if (!checkStrentgh(strenght[0],strenght[1],strenght[2],strenght[3])){
			throw new JetelException("Not allowed strenght combination");
		}
		ComparatorParameters cp;
		if (locale!=null){
			cp = new ComparatorParameters(strenght,locale);
			if (!comparators.containsKey(cp)){
				comparators.put(cp,new StringAproxComparator(locale,strenght));
			}
		}else{
			cp = new ComparatorParameters(strenght,"");
			if (!comparators.containsKey(cp)){
				comparators.put(cp,new StringAproxComparator(strenght));
			}
		}
		return (StringAproxComparator)comparators.get(cp);
	}

	public static StringAproxComparator createComparator(String locale,
			boolean s1,boolean s2,boolean s3,boolean s4) throws JetelException{
		boolean[] strenght={s1,s2,s3,s4};
		return createComparator(locale,strenght);
	}

	
	public static StringAproxComparator createComparator(boolean[] strenght)
			throws JetelException{
		if (!checkStrentgh(strenght[0],strenght[1],strenght[2],strenght[3])){
			throw new JetelException("Not allowed strenght combination");
		}
		ComparatorParameters cp = new ComparatorParameters(strenght,"");
		if (!comparators.containsKey(cp)){
			comparators.put(cp,new StringAproxComparator(strenght));
		}
		return (StringAproxComparator)comparators.get(cp);
	}
	
	public static StringAproxComparator createComparator(boolean s1,boolean s2,boolean s3,boolean s4) 
			throws JetelException{
		boolean[] strenght={s1,s2,s3,s4};
		return createComparator(strenght);
	}

	/**
	 * Checks if for given parameters there are possible settings: 
	 *  stronger field  can not be true for whole comparator weaker eg. when comparator has strentgh TERTIARY field SEC has to be false:
	 *  allowed configuration:
	 *  identical:	 T F F F  T F F  T F  T
	 *  tertiary:	 T T F F  T T F  T T  F
	 *  secundary: 	 T T T F  T T T  F F  F
	 *  primary:	 T T T T  F F F  F F  F
	 * 
	 * @param strentgh - strentgh of comparator
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secondary - indicates if SEC level works
	 * @param primary - indicates if PRIM level works
	 * @return
	 */
	public static boolean checkStrentgh(boolean identical,boolean tertiary,
				boolean secondary,boolean primary){
		if (identical && !tertiary && secondary && primary) return false;
		if (identical && tertiary && !secondary && primary) return false;
		if (!identical && tertiary && !secondary && primary) return false;
		if (identical && !tertiary && ! secondary && primary) return false;
		if (identical && ! tertiary && secondary && !primary) return false;
		if (!identical && !tertiary && !secondary && !primary) return false;
		return true;
	}
	
	/**
	 * @param strentgh = comparison level
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secundary - indicates if SEC level works
	 */
	private StringAproxComparator(boolean identical,boolean tertiary,
				boolean secundary,boolean primary) {

		col.setStrength(Collator.PRIMARY);
		en_col.setStrength(Collator.PRIMARY);
		setStrentgh(identical, tertiary, secundary, primary);
	}

	private StringAproxComparator(boolean[] strenght)  {
		this(strenght[0],strenght[1],strenght[2],strenght[3]);
	}
	
	private StringAproxComparator(String locale,boolean[] strenght)  {
		this(locale,strenght[0],strenght[1],strenght[2],strenght[3]);
	}
 
	/**
	 * @param locale = parameter for getting rules from StringAproxComparatorLocaleRules
	 * @param strentgh = comparison level
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secundary - indicates if SEC level works
	 */
	private StringAproxComparator(String locale,boolean identical,boolean tertiary,
				boolean secundary,boolean primary){
		this( identical, tertiary, secundary, primary);
		setLocale(locale);
	}

	public StringAproxComparator() throws JetelException{
		this(true,false,false,false);
	}
	
	/**
	 * Sets the locale string and appropriate collator
	 * 
	 * @param locale - string which represents given locale
	 */
	private void setLocale(String locale) {
		try {
			col=new RuleBasedCollator(
					((RuleBasedCollator)Collator.getInstance()).getRules()
					+StringAproxComparatorLocaleRules.getRules(locale));
			this.locale=locale.substring(0,2).toUpperCase();
		}catch(ParseException ex) {
			ex.printStackTrace();
		}
		 catch(NoSuchFieldException ex) {
			 Locale l=new Locale(locale.substring(0,2));
			 col=Collator.getInstance(l);
			 this.locale=l.getCountry();
		 }
		 catch(StringIndexOutOfBoundsException ex){
			 col=Collator.getInstance();
			 this.locale=null;
		 }
	}
	
	public String getLocale(){
		return locale;
	}
	
	/**
	 * This method checks if two chars are equal on given comparison strenght
	 * 
	 * @param c1 - char to compare
	 * @param c2 - char to compare
	 * @param strenth - comparator level
	 * @return (true,false) if (equal, unequal)
	 */
	public boolean charEquals(char c1, char c2, int strenth ) {
        switch (strenth) {
        case IDENTICAL:
            return c1 == c2;
        case TERTIARY:
             return (Character.toLowerCase(c1)==Character.toLowerCase(c2));
        case SECONDARY:
         	return (col.compare(String.valueOf(c1), String.valueOf(c2)) == 0);
        case PRIMARY:
         	return (en_col.compare(String.valueOf(c1), String.valueOf(c2)) == 0 || 
            		charEquals(c1, c2, StringAproxComparator.SECONDARY));
        }
        return false;
    }
	
	/**
	 * Computes substitution cost for one car to another
	 * 
	 * @param c1 char before substitution
	 * @param c2 char after substitution
	 * @param strentgh level of comparison
	 * @return (0,1,2,3,4) depending on which level c1 equals c2
	 */
	private int computeSubstCost(char c1,char c2,int strentgh){
		boolean check=true;
		for (int i=strentgh;i<=StringAproxComparator.IDENTICAL;i++){
			switch (i) {
			case StringAproxComparator.PRIMARY:check=PRIM;
				break;
			case StringAproxComparator.SECONDARY:check=SEC;
				break;
			case StringAproxComparator.TERTIARY:check=TER;
				break;
			case StringAproxComparator.IDENTICAL:check=IDEN;
				break;
			}
			if (check && !(charEquals(c1,c2,i))) {
				return StringAproxComparator.IDENTICAL+1-i;
			}
		}
		return 0;
	}

	
	/**
	 * @param i1
	 * @param i2
	 * @param i3
	 * @return minimum of three int 
	 */
	private int min(int i1,int i2,int i3){
		if (i1<=i2) {
			if (i1<=i3)
				return i1;
			else 
				return i3;
		}else {
			if (i2<=i3)
				return i2;
			else 
				return i3;
		}
	}
	
	/**
	 * This method calculates distance between Strings s and t. If 
	 * the distance is greater then maxDiffrence it returns maxDiffrence+1
	 * Distance is calculated as sum of costs of minimum changes in 
	 * string s to get string t. Cost of one change depends on strenght
	 * of comparator and for substitution varies from 1 to 4.
	 * 
	 * @param s
	 * @param t
	 * @return distance between Strings s and t
	 */
	public int distance(String s,String t){
		if (s.length()==0 || t.length()==0)
			return Math.max(s.length(),t.length())*delCost;

		int slength=s.length()+1;
		int tlength=t.length()+1;
		
		//initializing helpfull arrays
		 if (slast==null){
			slast = new int[slength];
			schars = new char[slength-1];
			tlast = new int[tlength];
			tblast = new int[tlength];
			now = new int[tlength];
			tchars = new char[tlength-1];
		 }
		if (slast.length<slength){
			slast = new int[slength];
			schars = new char[slength-1];
		}
		if (tlast.length<tlength){
			tlast = new int[tlength];
			tblast = new int[tlength];
			now = new int[tlength];
			tchars = new char[tlength-1];
		}
		 
		for (int i=0;i<slength;i++)
			slast[i]=i*delCost;
		for (int i=0;i<tlength;i++)
			tlast[i]=i*delCost;

		s.getChars(0,s.length(),schars,0);
		t.getChars(0,t.length(),tchars,0);
				
		int cost=0;
		
		for (int i=1;i<slength;i++) {
			now[0]=slast[i];
			tlast[0]=slast[i-1];
			cost=now[0];
			for (int j=1;j<tlength;j++){
				//getting min from: deleting one letter from s, deleting one letter from t, substituting letter in s by letter from t
				int m=min(now[j-1]+delCost,tlast[j]+delCost,tlast[j-1]+computeSubstCost(schars[i-1],tchars[j-1],strentgh));
				//if t and s have at least 2 letters each maybe we can exchange last two letters
				if (i>1 && j>1)
					if (schars[i-2]==tchars[j-1]&&schars[i-1]==tchars[j-2])
						m=Math.min(m,tblast[j-2]+changeCost);
				now[j]=m;
				if (m<cost) cost=m;
			}
			if (cost>maxDiffrence) return Math.min(cost,maxDiffrence+Math.max(delCost,changeCost));
			//rewrite arrays for last and beforelast results
			for (int j=0;j<tlength;j++){
				tblast[j]=tlast[j];
				tlast[j]=now[j];
			}
		}
		
		return Math.min(tlast[tlength-1],maxDiffrence+Math.max(delCost,changeCost));
		
	}

	public boolean[] getStrentgh() {
		return new boolean[] {IDEN,TER,SEC,PRIM};
	}

	private void setStrentgh(boolean identical,boolean tertiary,boolean secondary,
			boolean primary){
		if (primary) {
			this.setStrentgh(StringAproxComparator.PRIMARY, identical, tertiary, secondary, primary);
		}
		else if (secondary) {
			this.setStrentgh(StringAproxComparator.SECONDARY,identical,tertiary,secondary,primary);
		}
		else if (tertiary) {
			this.setStrentgh(StringAproxComparator.TERTIARY,identical,tertiary,secondary,primary);
		}
		else {
			this.setStrentgh(StringAproxComparator.IDENTICAL,identical,tertiary,secondary,primary);
		}
	}
	
	private void setStrentgh(int strenth,boolean identical,boolean tertiary,
			boolean secondary,boolean primary){
		IDEN=identical;
		TER=tertiary;
		SEC=secondary;
		PRIM=primary;
		this.strentgh = strenth;
		substCost=StringAproxComparator.IDENTICAL+1-strentgh;
		changeCost=changeMultiplier*substCost;
		delCost=delMultiplier*substCost;
		maxDiffrence=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getChangeMultiplier() {
		return changeMultiplier;
	}

	public void setChangeMultiplier(int change_multiplier) {
		changeMultiplier = change_multiplier;
		changeCost=changeMultiplier*substCost;
		this.maxDiffrence=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getDelMultiplier() {
		return delMultiplier;
	}

	public void setDelMultiplier(int del_multiplier) {
		delMultiplier = del_multiplier;
		delCost=delMultiplier*substCost;
		this.maxDiffrence=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getMaxLettersToChange() {
		return maxLettersToChange;
	}

	public void setMaxLettersToChange(int max_diffrence) {
		maxLettersToChange = max_diffrence;
		this.maxDiffrence=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getMaxCostForOneLetter() {
		return Math.max(delCost,changeCost);
	}

	public CollationKey getCollationKey(String source){
		return col.getCollationKey(source);
	}

	private static class ComparatorParameters{
		boolean[] strenght=new boolean[StringAproxComparator.IDENTICAL];
		String locale;
		
		ComparatorParameters(boolean[] strenght,String locale){
			this.strenght=strenght;
			this.locale=locale;
		}
		
		public boolean equals(Object obj) {
			if (!(obj instanceof ComparatorParameters)) {
				return false;
			}
			boolean[] objStrenght = ((ComparatorParameters)obj).getStrenght();
			boolean[] thisStrenght = getStrenght();
			boolean eq=true;
			for (int i=0;i<objStrenght.length;i++){
				eq = eq && (objStrenght[i]==thisStrenght[i]);
			}
			return eq && ((ComparatorParameters)obj).getLocale().equals(locale);
		}

		public int hashCode() {
			int hash=0;
			if (strenght[StringAproxComparator.IDENTICAL-1]) {
				hash+=1;
			}
			if (strenght[StringAproxComparator.TERTIARY-1]){
				hash+=2;
			}
			if (strenght[StringAproxComparator.SECONDARY-1]){
				hash+=4;
			}
			if (strenght[StringAproxComparator.PRIMARY-1]){
				hash+=8;
			}
			return 37*hash+locale.hashCode();
		}

		public boolean[] getStrenght() {
			return strenght;
		}

		public String getLocale() {
			return locale;
		}
	}

}
