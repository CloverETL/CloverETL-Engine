
package org.jetel.component.ws.exception;

/**
 *
 * @author Pavel Pospíchal
 */
public class ResourceException extends Exception {

	private static final long serialVersionUID = -5947878827174835693L;


	/**
     * Creates a new instance of <code>ResourceException</code> without detail message.
     */
    public ResourceException() {
    }


    /**
     * Constructs an instance of <code>ResourceException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public ResourceException(String msg) {
        super(msg);
    }
}
