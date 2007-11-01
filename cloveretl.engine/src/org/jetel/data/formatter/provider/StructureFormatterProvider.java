package org.jetel.data.formatter.provider;

import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.StructureFormatter;

/**
 * Provides support for getting the structure data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class StructureFormatterProvider implements FormatterProvider {

	private String charEncoder;
	private String header;
	private String footer;
	private String charSet;
	private String mask;
	
	/**
	 * Contructors.
	 */
	public StructureFormatterProvider() {
	}
	public StructureFormatterProvider(String charEncoder) {
		this.charEncoder = charEncoder;
	}

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter() {
		StructureFormatter formatter;
		if (charEncoder == null) {
			formatter =	new StructureFormatter();
		} else {
			formatter =	new StructureFormatter(charEncoder);
		}
		formatter.setHeader(header);
		formatter.setFooter(footer);
		formatter.setMask(mask);
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

	public void setMask(String mask) {
		this.mask = mask;
	}

    public void setFooter(String footer) {
    	this.footer = footer;
    }

}
