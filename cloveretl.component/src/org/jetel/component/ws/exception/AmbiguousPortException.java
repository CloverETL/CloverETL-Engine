
package org.jetel.component.ws.exception;

/**
 *
 * @author Pavel Pospíchal
 */
public class AmbiguousPortException extends Exception {

	private static final long serialVersionUID = 8479891044954718703L;


	/**
     * Creates a new instance of <code>AmbiguousPortException</code> without detail message.
     */
    public AmbiguousPortException() {
    }


    /**
     * Constructs an instance of <code>AmbiguousPortException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public AmbiguousPortException(String msg) {
        super(msg);
    }
}
