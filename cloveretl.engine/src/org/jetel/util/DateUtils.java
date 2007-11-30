
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

import java.text.DateFormatSymbols;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.util.string.StringUtils;

/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Jul 30, 2007
 *
 */
public class DateUtils {

	private final static Pattern SYMBOLS_PATTERN = Pattern.compile("[GyMwWDdFEaHhKkmsSzZ]+");
	private final static Pattern TEXT_SYMBOLS_PATTERN = Pattern.compile("[GEaZz]|M{3,}");
	
	private final static String ERA_INDICATOR = "G";
	private final static String DAY_INDICATOR = "E";
	private final static String AMPM_INDICATOR = "a";
	private final static String ZONE_INDICATOR_GENERAL = "Z";
	private final static String ZONE_INDICATOR_RFC822 = "z";
	private final static String MONTH_INDICATOR = "M";

	private static Locale locale = null;
	private static String[] era = null;
	private static String[] month;
	private static String[] day;
	private static String[] ampm;
	private static String[][] zone;
	
	
	/**
	 * initialization of constants
	 * 
	 * @param loc locale
	 */
	private static void createDateTimeConstants(Locale loc) {
		locale = loc;
		DateFormatSymbols symbols = locale != null ? new DateFormatSymbols(locale) : new DateFormatSymbols();
		era = symbols.getEras();
		String[] months = symbols.getMonths();
		String[] shortMonths = symbols.getShortMonths();
		month = new String[months.length + shortMonths.length];
		for (int i=0; i< months.length; i++) {
			month[i] = months[i];
		}
		for (int i=months.length; i<month.length; i++) {
			month[i] = shortMonths[i - months.length];
		}
		String[] days = symbols.getWeekdays();
		String[] shortdays = symbols.getShortWeekdays();
		day = new String[days.length + shortdays.length];
		for (int i=0; i< days.length; i++) {
			day[i] = days[i];
		}
		for (int i=days.length; i<day.length; i++) {
			day[i] = shortdays[i - days.length];
		}
	}

    /**
     * This method checks if given string can be parsed to date due to given pattern
     * 
     * @param str input string
     * @param pattern date/time pattern
     * @return true if input string is parseable to date
     */
    public static boolean isDate(CharSequence str, String pattern){
    	return isDate(str, pattern, null);
    }

    /**
     * This method checks if given string can be parsed to date due to given pattern
     * 
     * @param str input string
     * @param pattern date/time pattern
     * @param locale locale for time/date symbols
     * @return true if input string is parseable to date
     */
    public static boolean isDate(CharSequence str, String pattern, Locale loc){
    	
    	if (str == null || str.length() == 0) return true;
    	
    	//constants initialization
    	if (era == null || (loc == null && locale != null) || (loc != null && !loc.equals(locale))) {
    		createDateTimeConstants(locale);
    	}
    	
    	Matcher patternMatcher = SYMBOLS_PATTERN.matcher(pattern);
    	if (!patternMatcher.find()) {
    		throw new IllegalArgumentException("Pattern " + StringUtils.quote(pattern) + " has no date/time symbols");
    	}
    	//set first group of date/time symbols
    	CharSequence symbolGroup = patternMatcher.group(); 
       	int index = patternMatcher.end();

       	boolean hasText;
     	CharSequence nextSymbolGroup;
    	CharSequence separator;
    	CharSequence part;
    	StringBuilder textPart = new StringBuilder();
    	int sIndex = 0, separatorStart;
    	
    	//go throw all symbol's groups delimited by separators
    	do {
    		hasText = TEXT_SYMBOLS_PATTERN.matcher(symbolGroup).find();
    		//find next symbol's group
           	if (patternMatcher.find()) {
        		nextSymbolGroup = patternMatcher.group(); 
        		separator = pattern.subSequence(index, patternMatcher.start());
        	}else{//next symbols not found
        		nextSymbolGroup = null;
        		separator = pattern.subSequence(index, pattern.length());
        	}
           	//find separator between actual and next symbol's groups
			if (separator.length() > 0) {
				separatorStart = str.toString().indexOf(separator.toString(),sIndex);
			} else{
				separatorStart = str.length();
			}
			if (separatorStart == -1) return false;//separator not found
			//get substring from input string corresponding to actual symbol group
    		part = str.subSequence(sIndex, separatorStart);
    		//analize this text
    		if (!hasText) {//should have only digits
    			for (int i = 0; i < part.length(); i++) {
					if (!Character.isDigit(part.charAt(i))) return false;
				}
    		}else{//can have names of eras, months, days of week, time zones, pm/am's indicators 
				textPart.setLength(0);
				//divide text for numbers and text
     			for (int i = 0; i < part.length(); i++) {
    				if (!Character.isDigit(part.charAt(i))) {
     					textPart.append(part.charAt(i));
    				}else if (textPart.length() > 0){//analize text part
    					int ind = 0;
    					int k,l = 0;
    					if (symbolGroup.toString().contains(ERA_INDICATOR)) {
	    					for (k=0; k<era.length;k++){
	    						if (era[k].length() > 0 && textPart.toString().contains(era[k])) {
	    							ind = textPart.indexOf(era[k]);
	    							break;
	    						}
	    					}
	    					if (k == era.length) return false;
	    					textPart.delete(ind, ind + era[k].length());
    					}
    					if (symbolGroup.toString().contains(MONTH_INDICATOR)) {
	    					for (k=0; k<month.length;k++){
	    						if (month[k].length() > 0 && textPart.toString().contains(month[k])) {
	    							ind = textPart.indexOf(month[k]);
	    							break;
	    						}
	    					}
	    					if (k == month.length) return false;
	    					textPart.delete(ind, ind + month[k].length());
    					}
    					if (symbolGroup.toString().contains(DAY_INDICATOR)) {
	    					for (k=0; k<day.length;k++){
	    						if (day[k].length() > 0 && textPart.toString().contains(day[k])) {
	    							ind = textPart.indexOf(day[k]);
	    							break;
	    						}
	    					}
	    					if (k == day.length) return false;
	    					textPart.delete(ind, ind + day[k].length());
    					}
    					if (symbolGroup.toString().contains(AMPM_INDICATOR)) {
	    					for (k=0; k<ampm.length;k++){
	    						if (ampm[k].length() > 0 && textPart.toString().contains(ampm[k])) {
	    							ind = textPart.indexOf(ampm[k]);
	    							break;
	    						}
	    					}
	    					if (k == ampm.length) return false;
	    					textPart.delete(ind, ind + ampm[k].length());
    					}
    					if (symbolGroup.toString().contains(ZONE_INDICATOR_GENERAL) || 
    							symbolGroup.toString().contains(ZONE_INDICATOR_RFC822)) {
    						for (k=0; k<zone.length; k++){
	    						for (l=0;l<zone[l].length;l++) {
	    							if (zone[k][l].length() > 0 && zone.toString().contains(zone[k][l])) {
	    								ind = textPart.indexOf(zone[k][l]);
	    								break;
	    							}
	    						}
	    					}
	    					if (k == zone.length) return false;
	    					textPart.delete(ind, ind + zone[k][l].length());
    					}
    					//known text removed, can be only white space
    					for (k=0;k<textPart.length();k++){
    						if (!Character.isWhitespace(textPart.charAt(k))) return false;
    					}
    					textPart.setLength(0);
    				}
    			}
    		}
    		//go to next symbol's group
			if (nextSymbolGroup != null) {
				index += nextSymbolGroup.length() + separator.length();
				sIndex += part.length() + separator.length();
			}    		
			symbolGroup = nextSymbolGroup;
    	}while (symbolGroup != null);

    	return true;
    }
    

}
