/**
 * 
 */
package org.jetel.component.aggregate;

/**
 * Indicates failed initialization of the Aggregate Processor.
 * 
 * @author Jaroslav Urban
 *
 */
public class AggregateProcessorException extends Exception {

	/**
	 * Allocates a new <tt>ProcessorInitializationException</tt> object.
	 * @param arg0 error message
	 */
	public AggregateProcessorException(String arg0) {
		super(arg0);
	}

	/**
	 * 
	 * Allocates a new <tt>ProcessorInitializationException</tt> object.
	 *
	 * @param cause
	 */
	public AggregateProcessorException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 * Allocates a new <tt>ProcessorInitializationException</tt> object.
	 *
	 * @param arg0
	 * @param cause
	 */
	public AggregateProcessorException(String arg0, Throwable cause) {
		super(arg0, cause);
	}

}
