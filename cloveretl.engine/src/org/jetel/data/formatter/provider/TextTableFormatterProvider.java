package org.jetel.data.formatter.provider;

import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.TextTableFormatter;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * Provides support for getting the structure data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class TextTableFormatterProvider implements FormatterProvider {

	private String charEncoder;
	private String charSet;
	private String[] mask;
	private boolean setOutputFieldNames;
	
	/**
	 * Contructors.
	 */
	public TextTableFormatterProvider() {
	}
	public TextTableFormatterProvider(String charEncoder) {
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

	@SuppressWarnings(value="EI2")
	public void setMask(String[] mask) {
		this.mask = mask;
	}

	public void setOutputFieldNames(boolean setOutputFieldNames) {
		this.setOutputFieldNames = setOutputFieldNames;
	}

}
