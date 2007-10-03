package org.jetel.data.formatter.getter;

import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.formatter.Formatter;

/**
 * Provides support for getting the universal data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class DataFormatterGetter implements FormatterGetter {

	private String charEncoder;
	private String header;
	private String charSet;
	
	/**
	 * Contructors.
	 */
	public DataFormatterGetter() {
	}
	public DataFormatterGetter(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter() {
		DataFormatter formatter;
		if (charEncoder == null) {
			formatter =	new DataFormatter();
		} else {
			formatter =	new DataFormatter(charEncoder);
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
