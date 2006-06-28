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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;

import com.sun.tools.example.debug.expr.ParseException;

/**
 * @author avackova
 *
 */
public class NumericFormat extends NumberFormat {

	
	private DecimalFormat dFormat;
	private static char[] EXPONENT_SYMBOL = {'E','e'};
	
	/**
	 * 
	 */
	public NumericFormat() {
		super();
		dFormat =(DecimalFormat) super.getNumberInstance();
	}
	
	public NumericFormat(Locale locale){
		super();
		dFormat =(DecimalFormat) super.getNumberInstance(locale);
	}

    public NumericFormat(String pattern) {
    	super();
    	dFormat = new DecimalFormat(pattern);
    }

    public NumericFormat (String pattern, DecimalFormatSymbols symbols) {
    	super();
    	dFormat = new DecimalFormat(pattern,symbols);
   }
   
    /* (non-Javadoc)
	 * @see java.text.NumberFormat#parse(java.lang.String, java.text.ParsePosition)
	 */
	public Number parse(String source, ParsePosition parsePosition){
		boolean exponentForm = false;
		char decimalSeparator = dFormat.getDecimalFormatSymbols().getDecimalSeparator();
		boolean dSeparator = false;
		char groupingSeparator = dFormat.getDecimalFormatSymbols().getGroupingSeparator();
		String negativePrefix = dFormat.getNegativePrefix();
		String positivePrefix = dFormat.getPositivePrefix();
		int charRemoved=0;
		int lenght=source.length();
		char[]	pomChars=new char[lenght];
		StringBuffer toRemove = new StringBuffer(source);
		if (toRemove.toString().startsWith(negativePrefix)){
			toRemove.replace(0,negativePrefix.length(),"-");
			charRemoved = -1;
			source.getChars(negativePrefix.length(),lenght,pomChars,0);
		}else if (toRemove.toString().startsWith(positivePrefix)){
			toRemove.delete(0,positivePrefix.length());
			source.getChars(positivePrefix.length(),lenght,pomChars,0);
		}
		int exponentPart=0;
		for (int j=0;j<lenght;j++){
			if (!Character.isDigit(pomChars[j])){
				if (pomChars[j]==decimalSeparator){
					if (!dSeparator) {
						toRemove.replace(j-charRemoved,j-charRemoved+1,".");
						dSeparator = true;
					}else{
						toRemove.setLength(j-charRemoved);
						break;
					}
				}else if (pomChars[j]==groupingSeparator){
					if (!dSeparator){
						toRemove.deleteCharAt(j-charRemoved++);
					}else{
						toRemove.setLength(j-charRemoved);
						break;
					}
				}else if (pomChars[j]==EXPONENT_SYMBOL[0] || pomChars[j]==EXPONENT_SYMBOL[1]){
					exponentForm = true;
					exponentPart = getExponentPart(toRemove.toString(),j-charRemoved+1);
					toRemove.setLength(j-charRemoved);
					break;
				}else{
					toRemove.setLength(j-charRemoved);
					break;
				}
			}
		}
		try {
			BigDecimal bd = new BigDecimal(toRemove.toString());
			if (exponentForm){
				return bd.movePointRight(exponentPart);
			}else{
				return bd;
			}
		}catch(NumberFormatException e){
			return null;
		}
	}
	
	private int getExponentPart(String str,int offset){
		char ch = str.charAt(offset);
		StringBuffer exp = new StringBuffer();
		int index = 1;
		if (!Character.isDigit(ch)){
			if (!(ch=='+' || ch=='-')){
				return Integer.MIN_VALUE;
			}else{
				if (ch=='-'){
					exp.insert(0,'-');
				}
				ch = str.charAt(offset+1);
				index = 2;
			}
		}
		exp.append(ch);
		while (index<str.length() && Character.isDigit((ch = str.charAt(offset+index)))){
			exp.append(ch);
			index++;
		}
		return Integer.parseInt(exp.toString());
	}

	
	public StringBuffer format(BigDecimal number, StringBuffer toAppendTo,
			FieldPosition pos) {
		//Appending prefixes
		String prefix;
		if (number.signum()==-1){
			prefix = dFormat.getNegativePrefix();
			toAppendTo.append(prefix+number.toString().substring(1));
		}else{
			prefix = dFormat.getPositivePrefix();
			toAppendTo.append(prefix+number.toString());
		}
		int start = prefix.length();
		char zero = dFormat.getDecimalFormatSymbols().getZeroDigit();
		int groupingSize = dFormat.getGroupingSize();
		char groupingSeparator = dFormat.getDecimalFormatSymbols().getGroupingSeparator();
		int maximumFractionDigits = dFormat.getMaximumFractionDigits();
		int maximumIntegerDigits = dFormat.getMaximumIntegerDigits();
		int minimumFractionDigits = dFormat.getMinimumFractionDigits();
		int minimumIntegerdigits = dFormat.getMinimumIntegerDigits();
		int decimalPoint = toAppendTo.indexOf(".");
		if (decimalPoint==-1) {
			decimalPoint = toAppendTo.length();
			if (minimumFractionDigits>0 || isDecimalSeparatorAlwaysShown()){
				toAppendTo.append(dFormat.getDecimalFormatSymbols().getDecimalSeparator());
			}
		}else{
			toAppendTo.replace(decimalPoint,decimalPoint+1,String.valueOf(dFormat.getDecimalFormatSymbols().getDecimalSeparator()));
		}
		//formating integer digits
		int i=decimalPoint-1;
		boolean end = i<start ;
		int groupSize;
		int integerDigits = 0;
		while (i>0){
			groupSize = 0;
			while ((groupSize<groupingSize || groupingSize==0) && !end && integerDigits<maximumIntegerDigits){
				integerDigits++;
				groupSize++;
				end = --i<start;
			}
			if (!end && integerDigits<maximumIntegerDigits) {
				toAppendTo.insert(i+1,groupingSeparator);
			}else if (end){
				while (integerDigits<minimumIntegerdigits){
					if (groupSize<groupingSize || groupingSize==0){
						toAppendTo.insert(start,zero);
						groupSize++;
						integerDigits++;
					}else {
						toAppendTo.insert(start,groupingSeparator);
						groupSize = 0;
					}
				}
			}else break;
		}
		//formating fraction digits
		decimalPoint = toAppendTo.indexOf(".");
		i = decimalPoint+1;
		end = i>toAppendTo.length();
		int fractionDigits = 0;
		while (!end && fractionDigits<maximumFractionDigits){
			fractionDigits++;
			end = ++i>toAppendTo.length();
		}
		if (!end){
			toAppendTo.setLength(i);
		}
		while (fractionDigits<minimumFractionDigits){
			toAppendTo.append(zero);
			fractionDigits++;
		}
		//appending sufixes
		if (number.signum()==-1){
			toAppendTo.append(dFormat.getNegativeSuffix());
		}else{
			toAppendTo.append(dFormat.getPositiveSuffix());
		}
		
		
		return toAppendTo;
	}
	
	public String format(BigDecimal number){
		StringBuffer sb = new StringBuffer();
		return format(number,sb, new FieldPosition(0)).toString();
	}
	
	/* (non-Javadoc)
	 * @see java.text.NumberFormat#format(double, java.lang.StringBuffer, java.text.FieldPosition)
	 */
	public StringBuffer format(double number, StringBuffer toAppendTo,
			FieldPosition pos) {
		return null;
	}

	/* (non-Javadoc)
	 * @see java.text.NumberFormat#format(long, java.lang.StringBuffer, java.text.FieldPosition)
	 */
	public StringBuffer format(long number, StringBuffer toAppendTo,
			FieldPosition pos) {
		return null;
	}
	
	public DecimalFormatSymbols getDecimalFormatSymbols(){
		return dFormat.getDecimalFormatSymbols();
	}

	public void setDecimalFormatSymbols(DecimalFormatSymbols symbols){
		dFormat.setDecimalFormatSymbols(symbols);
	}
	
	public void applyLocalizedPattern(String pattern){
		dFormat.applyLocalizedPattern(pattern);
	}
	
	public void applyPattern(String pattern){
		dFormat.applyPattern(pattern);
	}
	
	public boolean equals(Object o){
		if (!(o instanceof NumericFormat)) return false;
		return dFormat.equals(((NumericFormat)o).dFormat);
	}
	
	public int getGroupingSize(){
		return dFormat.getGroupingSize();
	}
	
	public void setGroupingSize(int newSize){
		dFormat.setGroupingSize(newSize);
	}
	
	public String 	getNegativePrefix() {
		return dFormat.getNegativePrefix();
	}
	
	public void setNegativePrefix(String negativePrefix){
		dFormat.setNegativePrefix(negativePrefix);
	}
	
	public String getNegativeSuffix(){
		return dFormat.getNegativeSuffix();
	}
	
	public void setNegativeSuffix(String negativeSuffix){
		dFormat.setNegativeSuffix(negativeSuffix);
	}
	
	public String getPositivePrefix() {
		return dFormat.getPositivePrefix();
	}
	
	public void setPositivePrefix(String positivePrefix){
		dFormat.setPositivePrefix(positivePrefix);
	}
	
	public String getPositiveSuffix(){
		return dFormat.getPositiveSuffix();
	}
	
	public void setPositiveSuffix(String positiveSuffix){
		dFormat.setPositiveSuffix(positiveSuffix);
	}
	
	public int hashCode(){
		return dFormat.hashCode();
	}
	
	public boolean isDecimalSeparatorAlwaysShown() {
		return dFormat.isDecimalSeparatorAlwaysShown();
	}
	
	public void setDecimalSeparatorAlwaysShown(boolean newValue){
		dFormat.setDecimalSeparatorAlwaysShown(newValue);
	}
	
	public int getMaximumFractionDigits(){
		return dFormat.getMaximumFractionDigits();
	}
	
	public void setMaximumFractionDigits(int newValue){
		dFormat.setMaximumFractionDigits(newValue);
	}
	
	public int getMaximumIntegerDigits(){
		return dFormat.getMaximumIntegerDigits();
	}
	
	public void setMaximumIntegerDigits(int newValue){
		dFormat.setMaximumIntegerDigits(newValue);
	}
	
	public int getMinimumFractionDigits(){
		return dFormat.getMinimumFractionDigits();
	}
	
	public void setMinimumFractionDigits(int newValue) {
		dFormat.setMinimumFractionDigits(newValue);
	}
	
	public int getMinimumIntegerDigits(){
		return dFormat.getMinimumIntegerDigits();
	}
	
	public void setMinimumIntegerDigits(int newValue) {
		dFormat.setMinimumIntegerDigits(newValue);
	}
	
	public String toLocalizedPattern() {
		return dFormat.toLocalizedPattern();
	}
	
	public String toPattern(){
		return dFormat.toPattern();
	}
}
