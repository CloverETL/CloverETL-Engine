package org.jetel.util;

import java.text.Collator;
import java.util.Locale;

import org.jetel.exception.JetelException;

/**
 * Class for aproximative string comparison
 * For comparison level IDENTICAL 
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
	private boolean IDEN; //if comparator on level IDENTICAL works
	private boolean TER; //if comparator on level TERTIARY works
	private boolean SEC; //if comparator on level SECONDARY works

	private int MAX_DIFFRENCE=3; 
	private int DEL_MULTIPLIER=2;
	private int CHANGE_MULTIPLIER=1;
	
	private String[] rules={};
	private int substCost=1;// cost of char substituting on weakest level - depends on strenth and values of IDEN,TER,SEC fields
	private int changeCost;// = substitution cost on strongest level * CHANGE_MULTIPLIER- depends on strenth and values of IDEN,TER,SEC fields
	private int delCost; // = substitution cost on strongest level*DEL_MULTIPLIER
	private int maxDiffrence;// = substitution cost on strongest level * MAX_DIFFRENCE

	//arrays for method distance
	char[] schars;
	char[] tchars;
	int[] slast = null;
	int[] tlast = null;
	int[] tblast = null;
	int[] now = null;

	Collator col;

	/**
	 * Checks if for given parameters there are possible settings: 
	 *  it can not be stronger field be true for whole comparator weaker eg. when comparator has strentgh TERTIARY field SEC has to be false
	 * 
	 * @param strentgh - strentgh of comparator
	 * @param i - indicates if IDEN level works
	 * @param t - indicates if TER level works
	 * @param s - indicates if SEC level works
	 * @return
	 */
	private boolean checkStrentgh(int strentgh,boolean i,boolean t,boolean s){
		if (strentgh>IDENTICAL) return false;
		if (strentgh<PRIMARY) return false;
		switch (strentgh) {
			case IDENTICAL:if (!i || t || s) return false; 
					break;
			case TERTIARY: if (!t || s) return false;
					break;
			case SECONDARY:if (!s ) return false;
						   if (!t) if (i) return false;
					break;
			case PRIMARY:  if (!s) if (t || i) return false;
			               if (!t) if (i) return false;
					break;
		}
		return true;
	}
	
	/**
	 * @param strentgh = comparison level
	 * @param i - indicates if IDEN level works
	 * @param t - indicates if TER level works
	 * @param s - indicates if SEC level works
	 */
	public StringAproxComparator(int strenth,boolean i,boolean t,boolean s) throws JetelException{
		if (checkStrentgh(strenth, i, t, s))
			this.setStrentgh(strenth, i, t, s);
		else throw new JetelException("not acceptable arguments");
	}

	/**
	 * @param locale = parameter for getting rules from StringAproxComparatorLocaleRules
	 * @param strentgh = comparison level
	 * @param i - indicates if IDEN level works
	 * @param t - indicates if TER level works
	 * @param s - indicates if SEC level works
	 */
	public StringAproxComparator(String locale,int strenth,boolean i,boolean t,boolean s) throws JetelException{
		this(locale);
		if (checkStrentgh(strenth, i, t, s))
			this.setStrentgh(strenth, i, t, s);
		else throw new JetelException("not acceptable arguments");
	}

	public StringAproxComparator() {
		this.setStrentgh(IDENTICAL,true,false,false);
	}
	
	public StringAproxComparator(String locale) {
		this();
		try {
			rules=StringAproxComparatorLocaleRules.getRules(locale);
		}catch(NoSuchFieldException e){
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * Method for geting rules for one character - locale dependent  
	 *  
	 * @param c char for witch the rules are to be
	 * @param rules set of rule for given locale
	 * @return
	 */
	private int getRules(char c,String[] rules){
		int i;
		for (i=0;i<rules.length;i++){
			String rule=rules[i];
			if (!(rule.indexOf(c)==-1)) break;
		}
		return i;
	}
	
	public boolean charEquals(char c1, char c2, int strenth) {
        int c;
        switch (strenth) {
        case IDENTICAL:
            return c1 == c2;
        case TERTIARY:
             return (Character.toLowerCase(c1)==Character.toLowerCase(c2));
        case SECONDARY:
            c = getRules(c1, rules);
            if (c < rules.length)
                return (rules[c].indexOf(c2) != -1);
            else {
                return (charEquals(c1, c2, StringAproxComparator.TERTIARY));
            }
        case PRIMARY:
            return (col.compare(String.valueOf(c1), String.valueOf(c2)) == 0 || 
            		charEquals(c1, c2, StringAproxComparator.SECONDARY));
        }
        return false;
    }
	
	private int subst_cost(char c1,char c2,int strentgh){
		if (strentgh==IDENTICAL)
			return (charEquals(c1,c2,IDENTICAL) ? 0 : substCost);
		else {
			int weight=0;
			switch (strentgh){
				case TERTIARY: weight=(IDEN ? 1 : 0);
						  break;
				case SECONDARY:weight=(TER ? 1 : 0);
						  break;
				case PRIMARY:  weight=(SEC ? 1 : 0);
						  break;
			}
			return subst_cost(c1,c2,strentgh+1)*weight+(charEquals(c1,c2,strentgh) ? 0 : substCost);
		}
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

		int slenth=s.length()+1;
		int tlent=t.length()+1;
		
		
		 if (slast==null){
			slast = new int[slenth];
			tlast = new int[tlent];
			tblast = new int[tlent];
			now = new int[tlent];
		 }
		if (slast.length<slenth)
			slast = new int[tlent];
		if (tlast.length<tlent){
			tlast = new int[tlent];
			tblast = new int[tlent];
			now = new int[tlent];
		}
		 
		for (int i=0;i<slenth;i++)
			slast[i]=i*delCost;
		for (int i=0;i<tlent;i++)
			tlast[i]=i*delCost;

		schars=s.toCharArray();
		tchars=t.toCharArray();
		
		int cost=0;
		
		for (int i=1;i<slenth;i++) {
			now[0]=slast[i];
			tlast[0]=slast[i-1];
			cost=now[0];
			for (int j=1;j<tlent;j++){
				int m=min(now[j-1]+delCost,tlast[j]+delCost,tlast[j-1]+subst_cost(schars[i-1],tchars[j-1],strentgh));
				if (i>1 && j>1)
					if (schars[i-2]==tchars[j-1]&&schars[i-1]==tchars[j-2])
						m=Math.min(m,tblast[j-2]+changeCost);
				now[j]=m;
				if (m<cost) cost=m;
			}
			if (cost>maxDiffrence) return cost;
			for (int j=0;j<tlent;j++){
				tblast[j]=tlast[j];
				tlast[j]=now[j];
			}
		}
		
		return tlast[tlent-1];
		
	}

	public int getStrentgh() {
		return strentgh;
	}

	public void setStrentgh(int strenth,boolean i,boolean t,boolean s) {
		IDEN=i;
		TER=t;
		SEC=s;
		this.strentgh = strenth;
		int cost=0;
		if (IDEN) cost++;
		if (TER) cost++;
		if (SEC) cost++;
		if (strentgh==1) cost++;
		changeCost=CHANGE_MULTIPLIER*cost;
		delCost=DEL_MULTIPLIER*cost;
		maxDiffrence=MAX_DIFFRENCE*Math.max(delCost,changeCost);
		if (strenth==StringAproxComparator.PRIMARY&&col==null){
			col=Collator.getInstance(new Locale("US"));
			col.setStrength(Collator.PRIMARY);
		}
	}

	public int getMaxDiffrence() {
		return maxDiffrence;
	}

	public void setMaxDiffrence(int maxCost) {
		this.maxDiffrence = maxCost;
	}

	public int getCHANGE_MULTIPLIER() {
		return CHANGE_MULTIPLIER;
	}

	public void setCHANGE_MULTIPLIER(int change_multiplier) {
		CHANGE_MULTIPLIER = change_multiplier;
	}

	public int getDEL_MULTIPLIER() {
		return DEL_MULTIPLIER;
	}

	public void setDEL_MULTIPLIER(int del_multiplier) {
		DEL_MULTIPLIER = del_multiplier;
	}

	public int getMAX_DIFFRENCE() {
		return MAX_DIFFRENCE;
	}

	public void setMAX_DIFFRENCE(int max_diffrence) {
		MAX_DIFFRENCE = max_diffrence;
	}

}
