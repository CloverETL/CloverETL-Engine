package org.jetel.exception;

/**
 * An exception for all errors joined with data transfering between
 * a clover record and an initiate member row.
 * 
 * @author Martin Zatopek (martin.zatopek@opensys.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class CopyException extends Exception {

	private static final long serialVersionUID = 1L;

	public CopyException(String message) {
		super(message);
	}
	
	public CopyException(String message, Throwable cause) {
		super(message, cause);
	}
	
}
