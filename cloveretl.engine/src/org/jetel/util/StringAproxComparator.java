package org.jetel.util;

import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
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
	
	private int substCost=1;// cost of char substituting on weakest level - depends on strenth and values of IDEN,TER,SEC fields
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

	Collator col = Collator.getInstance();

	/**
	 * Checks if for given parameters there are possible settings: 
	 *  stronger field  can not be true for whole comparator weaker eg. when comparator has strentgh TERTIARY field SEC has to be false:
	 *  allowed configuration:
	 *  identical:	 T F F F  T F F  T F  T
	 *  tertiary:	 T T F F  T T F  T T  F
	 *  secundary: 	 T T T F  T T T  F F  F
	 *  primary:		 T T T T  F F F  F F  F
	 * 
	 * @param strentgh - strentgh of comparator
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secundary - indicates if SEC level works
	 * @param primary - indicates if PRIM level works
	 * @return
	 */
	private boolean checkStrentgh(boolean identical,boolean tertiary,
				boolean secundary,boolean primary){
		if (primary){
			if (secundary){
				if (tertiary && ! identical) return false;
			}
			else { 
				if (!identical) return false;
			}
		}else {
			if (!secundary && !tertiary && !identical) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * @param strentgh = comparison level
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secundary - indicates if SEC level works
	 */
	public StringAproxComparator(boolean identical,boolean tertiary,
				boolean secundary,boolean primary) throws JetelException{

		setStrentgh(identical, tertiary, secundary, primary);
	}

	/**
	 * @param locale = parameter for getting rules from StringAproxComparatorLocaleRules
	 * @param strentgh = comparison level
	 * @param identical - indicates if IDEN level works
	 * @param tertiary - indicates if TER level works
	 * @param secundary - indicates if SEC level works
	 */
	public StringAproxComparator(String locale,boolean identical,boolean tertiary,
				boolean secundary,boolean primary) throws JetelException{
		this( identical, tertiary, secundary, primary);
		setLocale(locale);
	}

	public StringAproxComparator() throws JetelException{
		this(true,false,false,false);
	}
	
	public void setLocale(String locale) {
		try {
			col=new RuleBasedCollator(
					((RuleBasedCollator)Collator.getInstance()).getRules()
					+StringAproxComparatorLocaleRules.getRules(locale));
		}catch(ParseException ex) {}
		 catch(NoSuchFieldException ex){
			 col=Collator.getInstance();
		 }
	}
	
	public boolean charEquals(char c1, char c2, int strenth) {
        switch (strenth) {
        case IDENTICAL:
            return c1 == c2;
        case TERTIARY:
             return (Character.toLowerCase(c1)==Character.toLowerCase(c2));
        case SECONDARY:
        	return (col.compare(String.valueOf(c1), String.valueOf(c2)) == 0);
        case PRIMARY:
            return (col.compare(String.valueOf(c1), String.valueOf(c2)) == 0 || 
            		charEquals(c1, c2, StringAproxComparator.SECONDARY));
        }
        return false;
    }
	
	private int countSubstCost(char c1,char c2,int strentgh){
		int i=StringAproxComparator.IDENTICAL;
		int cost=0;
		int weight=0;
		while (strentgh<=i){
			switch (i) {
				case StringAproxComparator.IDENTICAL:weight = IDEN ? 1: 0;
					break;
				case StringAproxComparator.TERTIARY:	weight = TER ? 1:0;
					break;
				case StringAproxComparator.SECONDARY:weight = SEC ? 1:0;
					break;
				case StringAproxComparator.PRIMARY:  weight = PRIM ? 1:0;
					break;
			}
			cost+= charEquals(c1,c2,i) ? 0 : weight * substCost;
			i--;
		}
		return cost;
	}

	
	private int min(int i1,int i2,int i3){
		int m=i1;
		if (i2<m) m=i2;
		if (i3<m) m=i3;
		return m;
	}
	
	/**
	 * This method calculates distance between Strings s and t. If the distance is greater then maxDiffrence it returns maxDiffrence+1
	 * @param s
	 * @param t
	 * @return distance between Strings s and t
	 */
	public int distance(String s,String t){
		if (s.length()==0 || t.length()==0)
			return Math.min(s.length(),t.length())*delCost;

		int slength=s.length()+1;
		int tlength=t.length()+1;
		
		
		 if (slast==null){
			slast = new int[slength];
			tlast = new int[tlength];
			tblast = new int[tlength];
			now = new int[tlength];
		 }
		if (slast.length<slength)
			slast = new int[slength];
		if (tlast.length<tlength){
			tlast = new int[tlength];
			tblast = new int[tlength];
			now = new int[tlength];
		}
		 
		for (int i=0;i<slength;i++)
			slast[i]=i*delCost;
		for (int i=0;i<tlength;i++)
			tlast[i]=i*delCost;

		schars=s.toCharArray();
		tchars=t.toCharArray();
		
		int cost=0;
		
		for (int i=1;i<slength;i++) {
			now[0]=slast[i];
			tlast[0]=slast[i-1];
			cost=now[0];
			for (int j=1;j<tlength;j++){
				int m=min(now[j-1]+delCost,tlast[j]+delCost,tlast[j-1]+countSubstCost(schars[i-1],tchars[j-1],strentgh));
				if (i>1 && j>1)
					if (schars[i-2]==tchars[j-1]&&schars[i-1]==tchars[j-2])
						m=Math.min(m,tblast[j-2]+changeCost);
				now[j]=m;
				if (m<cost) cost=m;
			}
			if (cost>maxDiffrence) return Math.min(cost,maxDiffrence+Math.max(delCost,changeCost));
			for (int j=0;j<tlength;j++){
				tblast[j]=tlast[j];
				tlast[j]=now[j];
			}
		}
		
		return Math.min(tlast[tlength-1],maxDiffrence+Math.max(delCost,changeCost));
		
	}

	public int getStrentgh() {
		return strentgh;
	}

	public void setStrentgh(boolean identical,boolean tertiary,boolean secundary,
			boolean primary) throws JetelException{
		if (primary) {
			this.setStrentgh(StringAproxComparator.PRIMARY, identical, tertiary, secundary, primary);
		}
		else if (secundary) {
			this.setStrentgh(StringAproxComparator.SECONDARY,identical,tertiary,secundary,primary);
		}
		else if (tertiary) {
			this.setStrentgh(StringAproxComparator.TERTIARY,identical,tertiary,secundary,primary);
		}
		else {
			this.setStrentgh(StringAproxComparator.IDENTICAL,identical,tertiary,secundary,primary);
		}
	}
	
	public void setStrentgh(int strenth,boolean identical,boolean tertiary,
			boolean secundary,boolean primary) throws JetelException{
		if (!checkStrentgh(identical,tertiary,secundary,primary)) {
			throw new JetelException("not acceptable arguments");
		}
		IDEN=identical;
		TER=tertiary;
		SEC=secundary;
		PRIM=primary;
		this.strentgh = strenth;
		int cost=0;
		if (IDEN) cost++;
		if (TER) cost++;
		if (SEC) cost++;
		if (PRIM) cost++;
		changeCost=changeMultiplier*cost;
		delCost=delMultiplier*cost;
		maxDiffrence=maxLettersToChange*Math.max(delCost,changeCost);
		if (strenth==StringAproxComparator.PRIMARY&&col==null){
			col=Collator.getInstance(new Locale("US"));
			col.setStrength(Collator.PRIMARY);
		}
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

}
