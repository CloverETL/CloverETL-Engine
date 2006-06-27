/**
 * 
 */
package org.jetel.util;

import java.math.BigDecimal;
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
		for (int j=0;j<lenght;j++){
			if (!Character.isDigit(pomChars[j])){
				if (pomChars[j]!=decimalSeparator){
					if (pomChars[j]==groupingSeparator && !dSeparator){
						toRemove.deleteCharAt(j-charRemoved++);
					}else{
						toRemove.setLength(j-charRemoved);
						break;
					}
				}else{
					if (!dSeparator) {
						toRemove.replace(j-charRemoved,j-charRemoved+1,".");
						dSeparator = true;
					}else{
						toRemove.setLength(j-charRemoved);
						break;
					}
				}
			}
		}
		if (toRemove.length()>0){
			return new BigDecimal(toRemove.toString());
		}else{
			return null;
		}
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
			if (minimumFractionDigits>0){
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
