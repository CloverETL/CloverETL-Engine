package org.jetel.data.formatter.getter;

import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.formatter.Formatter;

/**
 * Provides support for getting the lixlen data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class FixLenDataFormatterGetter implements FormatterGetter {

	private String charEncoder;
	private String header;
	private char chRecordFiller;
	private char chFieldFiller;
	private Character recordFiller;
	private Character fieldFiller;
	private String charSet;
	
	/**
	 * Contructors.
	 */
	public FixLenDataFormatterGetter() {
	}
	public FixLenDataFormatterGetter(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter() {
		FixLenDataFormatter formatter;
		if (charEncoder == null) {
			formatter =	new FixLenDataFormatter();
		} else {
			formatter =	new FixLenDataFormatter(charEncoder);
		}
		formatter.setHeader(header);
		formatter.setFieldFiller(chRecordFiller);
		formatter.setFieldFiller(chFieldFiller);
		recordFiller = formatter.getRecordFiller();
		fieldFiller = formatter.getFieldFiller();
		charSet = formatter.getCharSetName();
		return formatter;
	}

	/**
	 * Gets charset.
	 * 
	 * @return
	 */
	public String getCharSetName() {
		return charSet;
	}

	/**
	 * Sets header.
	 * 
	 * @param header
	 */
	public void setHeader(String header) {
    	this.header = header;
    }
	
	public void setRecordFiller(char filler) {
		this.chRecordFiller = filler;
	}

    public Character getRecordFiller() {
        return recordFiller;
    }

	public void setFieldFiller(char filler) {
        this.chFieldFiller = filler;
	}

    public Character getFieldFiller() {
        return fieldFiller;
    }

}
