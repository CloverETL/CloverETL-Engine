package org.jetel.data.formatter.getter;

import org.jetel.data.formatter.Formatter;

/**
 * This interface provides support for getting a data formatter.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public interface FormatterGetter {

	/**
	 * Creates new data formatter.
	 * 
	 * @return data formatter
	 */
	public Formatter getNewFormatter();
	
}
