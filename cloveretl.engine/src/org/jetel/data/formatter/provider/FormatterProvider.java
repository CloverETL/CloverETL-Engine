package org.jetel.data.formatter.provider;

import org.jetel.data.formatter.Formatter;

/**
 * This interface provides support for getting a data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public interface FormatterProvider {

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter();
	
}
