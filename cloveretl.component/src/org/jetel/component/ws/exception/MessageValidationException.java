
package org.jetel.component.ws.exception;

import java.util.List;

import org.jetel.component.ws.util.XmlMessageValidator;
import org.jetel.component.ws.util.XmlMessageValidator.ValidationInfo;

/**
 *
 * @author Pavel Pospichal
 */
public class MessageValidationException extends Exception {

	private static final long serialVersionUID = -34009581134234320L;
	
	private List<XmlMessageValidator.ValidationInfo> validationDisruptions = null;

    /**
     * Creates a new instance of <code>MessageValidationException</code> without detail message.
     */
    public MessageValidationException() {
    }


    /**
     * Constructs an instance of <code>MessageValidationException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public MessageValidationException(String msg) {
        super(msg);
    }

    public MessageValidationException(Throwable t) {
        super(t);
    }

    public MessageValidationException(String msg, Throwable t) {
        super(msg, t);
    }

    public List<ValidationInfo> getValidationDisruptions() {
        return validationDisruptions;
    }

    public void setValidationDisruptions(List<ValidationInfo> validationDisruptions) {
        this.validationDisruptions = validationDisruptions;
    }
}
