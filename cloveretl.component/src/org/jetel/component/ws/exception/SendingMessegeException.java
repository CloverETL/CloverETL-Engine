
package org.jetel.component.ws.exception;

/**
 *
 * @author Pavel Pospichal
 */
public class SendingMessegeException extends Exception {

	private static final long serialVersionUID = 8253013115467579164L;

	/**
     * Creates a new instance of <code>SendingMessegeException</code> without detail message.
     */
    public SendingMessegeException() {
    }


    /**
     * Constructs an instance of <code>SendingMessegeException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public SendingMessegeException(String msg) {
        super(msg);
    }

    public SendingMessegeException(Throwable t) {
        super(t);
    }

    public SendingMessegeException(String msg, Throwable t) {
        super(msg, t);
    }
}
