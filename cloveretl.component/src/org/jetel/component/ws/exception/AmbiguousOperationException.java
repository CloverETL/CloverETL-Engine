
package org.jetel.component.ws.exception;

/**
 *
 * @author Pavel Pospíchal
 */
public class AmbiguousOperationException extends Exception {

	private static final long serialVersionUID = -5947878827174835693L;


	/**
     * Creates a new instance of <code>AmbiguousOperationException</code> without detail message.
     */
    public AmbiguousOperationException() {
    }


    /**
     * Constructs an instance of <code>AmbiguousOperationException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public AmbiguousOperationException(String msg) {
        super(msg);
    }
}
