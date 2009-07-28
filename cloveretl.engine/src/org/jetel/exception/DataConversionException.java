
package org.jetel.exception;

/**
 *
 * @author Pavel Pospichal
 */
public class DataConversionException extends Exception {

    /**
     * Creates a new instance of <code>DataConversionException</code> without detail message.
     */
    public DataConversionException() {
    }


    /**
     * Constructs an instance of <code>DataConversionException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public DataConversionException(String msg) {
        super(msg);
    }
    
    /**
     * Constructs an instance of <code>DataConversionException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public DataConversionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
