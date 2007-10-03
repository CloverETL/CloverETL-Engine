package org.jetel.data.formatter.getter;

import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.TextTableFormatter;

/**
 * Provides support for getting the structure data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class TextTableFormatterGetter implements FormatterGetter {

	private String charEncoder;
	private String charSet;
	private String[] mask;
	private boolean setOutputFieldNames;
	
	/**
	 * Contructors.
	 */
	public TextTableFormatterGetter() {
	}
	public TextTableFormatterGetter(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter() {
		TextTableFormatter formatter;
		if (charEncoder == null) {
			formatter =	new TextTableFormatter();
		} else {
			formatter =	new TextTableFormatter(charEncoder);
		}
		formatter.setMask(mask);
		charSet = formatter.getCharsetName();
		formatter.setOutputFieldNames(setOutputFieldNames);
		return formatter;
	}

	public String getCharsetName() {
		return charSet ;
	}

	public void setMask(String[] mask) {
		this.mask = mask;
	}

	public void setOutputFieldNames(boolean setOutputFieldNames) {
		this.setOutputFieldNames = setOutputFieldNames;
	}

}
