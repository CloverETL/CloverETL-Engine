
package org.jetel.component.ws.exception;

/**
 *
 * @author Pavel Pospíchal
 */
public class WSDLAnalyzeException extends Exception {

	private static final long serialVersionUID = -4216513205906896336L;

	/**
     * Creates a new instance of <code>WSDLAnalyzeException</code> without detail message.
     */
    public WSDLAnalyzeException() {
    }


    /**
     * Constructs an instance of <code>WSDLAnalyzeException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public WSDLAnalyzeException(String msg) {
        super(msg);
    }

    /**
     *
     * @param e
     */
    public WSDLAnalyzeException(Throwable e) {
        super(e);
    }

    public WSDLAnalyzeException(String msg,Throwable e) {
        super(msg, e);
    }
}
