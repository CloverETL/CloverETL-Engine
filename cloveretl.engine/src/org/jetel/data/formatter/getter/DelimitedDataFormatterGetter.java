package org.jetel.data.formatter.getter;

import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.Formatter;

/**
 * Provides support for getting the delimited data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class DelimitedDataFormatterGetter implements FormatterGetter {

	private String charEncoder;
	private String header;
	private String charSet;
	
	/**
	 * Contructors.
	 */
	public DelimitedDataFormatterGetter() {
	}
	public DelimitedDataFormatterGetter(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter() {
		DelimitedDataFormatter formatter;
		if (charEncoder == null) {
			formatter =	new DelimitedDataFormatter();
		} else {
			formatter =	new DelimitedDataFormatter(charEncoder);
		}
		formatter.setHeader(header);
		charSet = formatter.getCharsetName();
		return formatter;
	}

	/**
	 * Sets header.
	 * 
	 * @param header
	 */
	public void setHeader(String header) {
    	this.header = header;
    }
	
	public String getCharsetName() {
		return charSet ;
	}

}
