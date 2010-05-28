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

import java.text.CollationKey;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.Locale;

import org.jetel.exception.JetelException;

/**
 * Class for approximative string comparison
 * 
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created August 17, 2006 
 */

public class StringAproxComparator{

	//strength fields
	public final static int  IDENTICAL=4;
	/**
	 * Upper case = lower case
	 */
	public final static int TERTIARY=3;
	/**
	 * diacritic letters = letters without diacritic (locale dependent)
	 * now done only for CZ and PL
	 */
	public final static int SECONDARY=2;
	/**
	 * mistakes acceptable (Collator.PRIMARY,new Locale("en","UK"))
	 */
	public final static int PRIMARY=1;
	
	private int strength;
	private boolean IDEN; 	//if comparator on level IDENTICAL works
	private boolean TER; 	//if comparator on level TERTIARY works
	private boolean SEC; 	//if comparator on level SECONDARY works
	private boolean PRIM;	//if comparator on level PRIMARY works

	private int maxLettersToChange=3; 
	private int delMultiplier=1;
	private int changeMultiplier=1;
	
	private int substCost;// cost of char substituting of one letter
	private int changeCost;// = substitution cost on strongest level * changeMultiplier- depends on strength and values of IDEN,TER,SEC fields
	private int delCost; // = substitution cost on strongest level*delMultiplier
	private int maxDifference;// = substitution cost on strongest level * maxLettersToChange

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
	
	public static StringAproxComparator createComparator(String locale,boolean[] strength)
			throws JetelException{
		if (!checkStrength(strength[0],strength[1],strength[2],strength[3])){
			throw new JetelException("Not allowed strength combination");
		}
		if (locale!=null){
			return new StringAproxComparator(locale,strength);
		}else{
			return new StringAproxComparator(strength);
		}
	}

	public static StringAproxComparator createComparator(String locale,
			boolean identical,boolean tertiary,boolean secondary,boolean primary) 
			throws JetelException{
		boolean[] strength={identical,tertiary,secondary,primary};
		return createComparator(locale,strength);
	}

	
	public static StringAproxComparator createComparator(boolean[] strength)
			throws JetelException{
		if (!checkStrength(strength[0],strength[1],strength[2],strength[3])){
			throw new JetelException("Not allowed strength combination");
		}
		return new StringAproxComparator(strength);
	}
	
	public static StringAproxComparator createComparator(boolean identical,
			boolean tertiary,boolean secondary,boolean primary) 
			throws JetelException{
		boolean[] strength={identical,tertiary,secondary,primary};
		return createComparator(strength);
	}

	/**
	 * Checks if for given parameters there are possible settings: 
	 *  stronger field  can not be true for whole comparator weaker eg. when comparator has strength TERTIARY field SEC has to be false:
	 *  allowed configuration:
	 *  identical:	 T F F F  T F F  T F  T
	 *  tertiary:	 T T F F  T T F  T T  F
	 *  secondary: 	 T T T F  T T T  F F  F
	 *  primary:	 T T T T  F F F  F F  F
	 * 
	 * @param strength - strength of comparator
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secondary - indicates if SEC level works
	 * @param primary - indicates if PRIM level works
	 * @return
	 */
	public static boolean checkStrength(boolean identical,boolean tertiary,
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
	 * @param strength = comparison level
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secondary - indicates if SEC level works
	 */
	private StringAproxComparator(boolean identical,boolean tertiary,
				boolean secondary,boolean primary) {

		col.setStrength(Collator.PRIMARY);
		en_col.setStrength(Collator.PRIMARY);
		setStrength(identical, tertiary, secondary, primary);
	}

	private StringAproxComparator(boolean[] strength)  {
		this(strength[0],strength[1],strength[2],strength[3]);
	}
	
	private StringAproxComparator(String locale,boolean[] strength)  {
		this(locale,strength[0],strength[1],strength[2],strength[3]);
	}
 
	/**
	 * @param locale = parameter for getting rules from StringAproxComparatorLocaleRules
	 * @param strength = comparison level
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secondary - indicates if SEC level works
	 */
	private StringAproxComparator(String locale,boolean identical,boolean tertiary,
				boolean secondary,boolean primary){
		this( identical, tertiary, secondary, primary);
		setLocale(locale);
		if (secondary) col.setStrength(Collator.SECONDARY);
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
	 * This method checks if two chars are equal on given comparison strength
	 * 
	 * @param c1 - char to compare
	 * @param c2 - char to compare
	 * @param strength - comparator level
	 * @return (true,false) if (equal, unequal)
	 */
	public boolean charEquals(char c1, char c2, int strength ) {
        switch (strength) {
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
	 * @param strength level of comparison
	 * @return (0,1,2,3,4) depending on which level c1 equals c2
	 */
	private int computeSubstCost(char c1,char c2,int strength){
		boolean check=true;
		for (int i=strength;i<=StringAproxComparator.IDENTICAL;i++){
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
	 * the distance is greater then maxDifference it returns maxDifference+1
	 * Distance is calculated as sum of costs of minimum changes in 
	 * string s to get string t. Cost of one change depends on strength
	 * of comparator and for substitution varies from 1 to 4.
	 * 
	 * @param s
	 * @param t
	 * @return distance between Strings s and t
	 */
	public int distance(String s,String t){
		if (StringUtils.isEmpty(s)|| StringUtils.isEmpty(t))
			return Math.min(Math.max(s.length(),t.length())*delCost, maxDifference*delCost);

		int slength=s.length()+1;
		int tlength=t.length()+1;
		
		//initializing helpful arrays
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
				int m=min(now[j-1]+delCost,tlast[j]+delCost,tlast[j-1]+computeSubstCost(schars[i-1],tchars[j-1],strength));
				//if t and s have at least 2 letters each maybe we can exchange last two letters
				if (i>1 && j>1)
					if (schars[i-2]==tchars[j-1]&&schars[i-1]==tchars[j-2])
						m=Math.min(m,tblast[j-2]+changeCost);
				now[j]=m;
				if (m<cost) cost=m;
			}
			if (cost>maxDifference) return Math.min(cost,maxDifference+Math.max(delCost,changeCost));
			//rewrite arrays for last and before last results
			for (int j=0;j<tlength;j++){
				tblast[j]=tlast[j];
				tlast[j]=now[j];
			}
		}
		
		return Math.min(tlast[tlength-1],maxDifference*Math.max(delCost,changeCost));
		
	}

	public boolean[] getStrength() {
		return new boolean[] {IDEN,TER,SEC,PRIM};
	}

	private void setStrength(boolean identical,boolean tertiary,boolean secondary,
			boolean primary){
		if (primary) {
			this.setStrength(StringAproxComparator.PRIMARY, identical, tertiary, secondary, primary);
		}
		else if (secondary) {
			this.setStrength(StringAproxComparator.SECONDARY,identical,tertiary,secondary,primary);
		}
		else if (tertiary) {
			this.setStrength(StringAproxComparator.TERTIARY,identical,tertiary,secondary,primary);
		}
		else {
			this.setStrength(StringAproxComparator.IDENTICAL,identical,tertiary,secondary,primary);
		}
	}
	
	private void setStrength(int strength,boolean identical,boolean tertiary,
			boolean secondary,boolean primary){
		IDEN=identical;
		TER=tertiary;
		SEC=secondary;
		PRIM=primary;
		this.strength = strength;
		substCost=StringAproxComparator.IDENTICAL+1-strength;
		changeCost=changeMultiplier*substCost;
		delCost=delMultiplier*substCost;
		maxDifference=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getChangeMultiplier() {
		return changeMultiplier;
	}

	public void setChangeMultiplier(int change_multiplier) {
		changeMultiplier = change_multiplier;
		changeCost=changeMultiplier*substCost;
		this.maxDifference=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getDelMultiplier() {
		return delMultiplier;
	}

	public void setDelMultiplier(int del_multiplier) {
		delMultiplier = del_multiplier;
		delCost=delMultiplier*substCost;
		this.maxDifference=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getMaxLettersToChange() {
		return maxLettersToChange;
	}

	public void setMaxLettersToChange(int max_difference) {
		maxLettersToChange = max_difference;
		this.maxDifference=maxLettersToChange*Math.max(delCost,changeCost);
	}

	public int getMaxCostForOneLetter() {
		return Math.max(delCost,changeCost);
	}

	public CollationKey getCollationKey(String source){
		return col.getCollationKey(source);
	}

}
