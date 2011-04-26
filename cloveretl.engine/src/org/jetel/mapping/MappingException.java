package org.jetel.mapping;

/**
 * The mapping exception class.
 * 
 * @author Martin Zatopek (martin.zatopek@opensys.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *         
 * @comments Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class MappingException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a mapping exception.
	 * 
	 * @param message
	 */
	public MappingException(String message) {
		super(message);
	}
	
}
