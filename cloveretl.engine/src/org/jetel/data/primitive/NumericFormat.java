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
package org.jetel.data.primitive;

import java.math.BigDecimal;
import java.nio.CharBuffer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;

/**
 * This class is equivalent of DecimalFormat for BigDecimal
 * 
/**
* @author avackova <agata.vackova@javlinconsulting.cz> ; 
* (c) JavlinConsulting s.r.o.
*	www.javlinconsulting.cz
*	@created August 17, 2006 
*
* DEPRECATED. This class is buggy. Avoid it.
 */
@Deprecated
public class NumericFormat extends NumberFormat {

	private static final long serialVersionUID = -8679114296443485956L;
	
	private DecimalFormat dFormat;
	private static char[] EXPONENT_SYMBOL = {'E','e'};
	
	/**
	 * 
	 */
	public NumericFormat() {
		super();
		dFormat =(DecimalFormat) super.getNumberInstance();
	}
	//
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
   
    /**
     * @return BigDecimal
     * @deprecated
     */
	@Override
	public Number parse(String source, ParsePosition parsePosition){
		boolean exponentForm = false;
		char decimalSeparator = dFormat.getDecimalFormatSymbols().getDecimalSeparator();
		boolean dSeparator = false;
		char groupingSeparator = dFormat.getDecimalFormatSymbols().getGroupingSeparator();
		String negativePrefix = dFormat.getNegativePrefix();
		String positivePrefix = dFormat.getPositivePrefix();
		int start=0;
		int counter = 0;
		int length=source.length();
		char[]	chars=source.toCharArray();
		char[] result = new char[length];
		if (source.startsWith(negativePrefix)){
			result[counter++]='-';
			start = negativePrefix.length();
		}else if (source.startsWith(positivePrefix)){
			start = positivePrefix.length();
		}
		int exponentPart=0;
		for (int j=start;j<length;j++){
			if (Character.isDigit(chars[j])){
				result[counter++]=chars[j];
			}else if (chars[j]==decimalSeparator){
				if (!dSeparator){//there was not decimal separator before
					result[counter++]='.';
					dSeparator = true;
				}else{//second decimal separator found
					throw new NumberFormatException("Cannot interpret \"" + source +"\" as a number. " +
							"Two decimal separators found.");
				}
			}else if (chars[j]==groupingSeparator && !dSeparator){//grouping separator is after decimal separator, rest of string is ignored
				continue;
			}else if (chars[j]==EXPONENT_SYMBOL[0] || chars[j]==EXPONENT_SYMBOL[1]){
				exponentForm = true;
				exponentPart = getExponentPart(chars,counter+1);
				break;
			}else{//unknown char or grouping separator is after decimal separator 
				throw new NumberFormatException("Cannot interpret \"" + source +"\" as a number.");
			}
		}
		try {
			BigDecimal bigDecimal = new BigDecimal(String.copyValueOf(result,0,counter));
			parsePosition.setIndex(chars.length);
			if (exponentForm){
				return bigDecimal.movePointRight(exponentPart);
			}else{
				return bigDecimal;
			}
		}catch(NumberFormatException e){
			return null;
		}
	}
	
	/**
	 * @param source
	 * @param parsePosition
	 * @return
	 */
	public BigDecimal parse(CharSequence source){
		boolean exponentForm = false;
		char decimalSeparator = dFormat.getDecimalFormatSymbols().getDecimalSeparator();
		boolean dSeparator = false;
		char groupingSeparator = dFormat.getDecimalFormatSymbols().getGroupingSeparator();
		String negativePrefix = dFormat.getNegativePrefix();
		String positivePrefix = dFormat.getPositivePrefix();
		int counter = 0;
		int length=source.length();
		char[] result = new char[length];
		int start = 0;
		try {
			if (negativePrefix.contentEquals(source.subSequence(0, negativePrefix.length()))) {
				result[counter++]='-';
				start = negativePrefix.length();			
			}else if (positivePrefix.contentEquals(source.subSequence(0, positivePrefix.length()))) {
				start = positivePrefix.length();
			}
		} catch (IndexOutOfBoundsException e) {	// not enough space for prefix
			// ignore
		}
		char[] chars;
		if (source instanceof CharBuffer && ((CharBuffer)source).hasArray()) {	// optimization
			chars = ((CharBuffer)source).array();
			start += ((CharBuffer)source).arrayOffset() + ((CharBuffer)source).position();
		} else {
			chars = new char[length];
			for (int idx = 0; idx < length; idx++) {
				chars[idx] = source.charAt(idx);
			}
			start = 0;
		}
		int end = start + length;
		int exponentPart=0;
		for (int j=start;j<end;j++){
			if (Character.isDigit(chars[j])){
				result[counter++]=chars[j];
			}else if (chars[j]==decimalSeparator){
				if (!dSeparator){//there was not decimal separator before
					result[counter++]='.';
					dSeparator = true;
				}else{//second decimal separator found
					throw new NumberFormatException("Cannot interpret \"" + source +"\" as a number. " +
							"Two decimal separators found.");
				}
			}else if (chars[j]==groupingSeparator && !dSeparator){//grouping separator is after decimal separator, rest of string is ignored
				continue;
			}else if (chars[j]==EXPONENT_SYMBOL[0] || chars[j]==EXPONENT_SYMBOL[1]){
				exponentForm = true;
				exponentPart = getExponentPart(chars,counter+1);
				break;
			}else{//unknown char or grouping separator is after decimal separator
				throw new NumberFormatException("Cannot interpret \"" + source +"\" as a number.");
			}
		}
		BigDecimal bigDecimal = new BigDecimal(String.copyValueOf(result,0,counter));
		if (exponentForm){
			return bigDecimal.movePointRight(exponentPart);
		}else{
			return bigDecimal;
		}
	}
	
	/**
	 * This method gets digits from string and changes them to int. Before digits 
	 * can be '+' or '-'
	 * 
	 * @param str - input string
	 * @param offset 
	 * @return 
	 */
	private int getExponentPart(char[] str,int offset){
		char ch = str[offset];
		char[] exp = new char[str.length-offset];
		int index = 1;
		if (!Character.isDigit(ch)){
			if (!(ch=='+' || ch=='-')){
				return Integer.MIN_VALUE;
			}else{
				if (ch=='-'){
					exp[0]='-';
				}
				ch = str[offset+1];
				index = 2;
			}
		}
		exp[index-1]=ch;
		//while next character is digit appends it to result string
		while (index<exp.length && Character.isDigit((ch = str[offset+index]))){
			exp[index++]=ch;
		}
		return Integer.parseInt(String.copyValueOf(exp,0,index));
	}

	
	/**
	 * This method formats BigDecimal due to given pattern
	 * 
	 * @param number BigDecimal to format
	 * @param toAppendTo
	 * @param pos
	 * @return StringBuffer with formated number
	 */
	public StringBuffer format(BigDecimal number, StringBuffer toAppendTo,
			FieldPosition pos) {
		//"starting" string is string representation if BigDecimal with appropriate prefix:
		String prefix;
		if (number.signum()==-1){
			prefix = dFormat.getNegativePrefix();
			toAppendTo.append(prefix+number.toString().substring(1));
		}else{
			prefix = dFormat.getPositivePrefix();
			toAppendTo.append(prefix+number.toString());
		}
		//initializing temp variables
		int start = prefix.length();
		int groupingSize = dFormat.getGroupingSize();
		char groupingSeparator = dFormat.getDecimalFormatSymbols().getGroupingSeparator();
		int maximumFractionDigits = dFormat.getMaximumFractionDigits();
		int maximumIntegerDigits = dFormat.getMaximumIntegerDigits();
		int minimumFractionDigits = dFormat.getMinimumFractionDigits();
		int minimumIntegerDigits = dFormat.getMinimumIntegerDigits();
		int decimalPoint = toAppendTo.indexOf(".");  //BigDecimal.toString always uses '.' (and never ',')
		if (decimalPoint==-1) {//if input number has no fractional part, append a decimal separator to the end of string
			decimalPoint = toAppendTo.length();
			if (minimumFractionDigits>0 || isDecimalSeparatorAlwaysShown()){
				toAppendTo.append(dFormat.getDecimalFormatSymbols().getDecimalSeparator());
			}
		} else if (maximumFractionDigits>0 || isDecimalSeparatorAlwaysShown()) {
			toAppendTo.replace(decimalPoint,decimalPoint+1,String.valueOf(dFormat.getDecimalFormatSymbols().getDecimalSeparator()));
		} else {//strip out the entire fractional part
			toAppendTo.setLength(decimalPoint);
		}
		
		//formating integer digits from decimal point to left
		int index=decimalPoint-1;
		boolean end = index<start ;
		int groupSize;
		int integerDigits = 0;
		while (index>=0){
			groupSize = 0;
			//take all digits from one group
			while ((groupSize<groupingSize || groupingSize==0) && !end && integerDigits<maximumIntegerDigits){
				integerDigits++;
				groupSize++;
				end = --index<start;
			}
			if (!end && integerDigits<maximumIntegerDigits) {//locus for grouping separator
				toAppendTo.insert(index+1,groupingSeparator);
				decimalPoint++;
			}else if (end){
				while (integerDigits<minimumIntegerDigits){
					if (groupSize<groupingSize || groupingSize==0){
						toAppendTo.insert(start,'0');
						groupSize++;
						integerDigits++;
						decimalPoint++;
					}else {
						toAppendTo.insert(start,groupingSeparator);
						groupSize = 0;
						decimalPoint++;
					}
				}
			}else break;//integerDigits>=maximumIntegerDigits
		}
		//formating fraction digits
		index = decimalPoint+1;
		end = index>=toAppendTo.length();
		int fractionDigits = 0;
		while (!end && fractionDigits<maximumFractionDigits){
			fractionDigits++;
			end = ++index>toAppendTo.length();
		}
		if (!end){//fractionDigits>=maximumFractionDigits
			toAppendTo.setLength(index);
		}
		while (fractionDigits<minimumFractionDigits){
			toAppendTo.append('0');
			fractionDigits++;
		}
		//remove trailing 0 if we can
		index = toAppendTo.length() - 1;
		int shortBy = 0; 
		while (fractionDigits > minimumFractionDigits && toAppendTo.charAt(index) == '0'){
			shortBy++;
			fractionDigits--;
			index--;
		}
		if (index==decimalPoint && !isDecimalSeparatorAlwaysShown()) {
			shortBy++;
		}
		toAppendTo.setLength(toAppendTo.length() - shortBy);
		//appending suffixes
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
	@Override
	public StringBuffer format(double number, StringBuffer toAppendTo,
			FieldPosition pos) {
		return dFormat.format(number,toAppendTo,pos);
	}

	/* (non-Javadoc)
	 * @see java.text.NumberFormat#format(long, java.lang.StringBuffer, java.text.FieldPosition)
	 */
	@Override
	public StringBuffer format(long number, StringBuffer toAppendTo,
			FieldPosition pos) {
		return dFormat.format(number,toAppendTo,pos);
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
	
	@Override
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
	
	@Override
	public int hashCode(){
		return dFormat.hashCode();
	}
	
	public boolean isDecimalSeparatorAlwaysShown() {
		return dFormat.isDecimalSeparatorAlwaysShown();
	}
	
	public void setDecimalSeparatorAlwaysShown(boolean newValue){
		dFormat.setDecimalSeparatorAlwaysShown(newValue);
	}
	
	@Override
	public int getMaximumFractionDigits(){
		return dFormat.getMaximumFractionDigits();
	}
	
	@Override
	public void setMaximumFractionDigits(int newValue){
		dFormat.setMaximumFractionDigits(newValue);
	}
	
	@Override
	public int getMaximumIntegerDigits(){
		return dFormat.getMaximumIntegerDigits();
	}
	
	@Override
	public void setMaximumIntegerDigits(int newValue){
		dFormat.setMaximumIntegerDigits(newValue);
	}
	
	@Override
	public int getMinimumFractionDigits(){
		return dFormat.getMinimumFractionDigits();
	}
	
	@Override
	public void setMinimumFractionDigits(int newValue) {
		dFormat.setMinimumFractionDigits(newValue);
	}
	
	@Override
	public int getMinimumIntegerDigits(){
		return dFormat.getMinimumIntegerDigits();
	}
	
	@Override
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
