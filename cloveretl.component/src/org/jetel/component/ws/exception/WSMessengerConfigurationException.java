
package org.jetel.component.ws.exception;

/**
 *
 * @author Pavel Pospichal
 * @see Exception
 */
public class WSMessengerConfigurationException extends Exception {

	private static final long serialVersionUID = 3703202875794974464L;

	/**
     * Creates a new instance of <code>WSMessengerConfigurationException</code> without detail message.
     */
    public WSMessengerConfigurationException() {
    }


    /**
     * Constructs an instance of <code>WSMessengerConfigurationException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public WSMessengerConfigurationException(String msg) {
        super(msg);
    }

    public WSMessengerConfigurationException(String msg, Throwable t) {
        super(msg, t);
    }
}
