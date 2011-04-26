package org.jetel.error;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.mapping.MappingSource;
import org.jetel.mapping.MappingTarget;


/**
 * This class contains error mapping element
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class ErrorMappingElement implements MappingSource, MappingTarget {
	
	// error code and massage string
	private static final String ERR_CODE = Error.ERR_CODE.toString();
	private static final String ERR_MESSAGE = Error.ERR_MESSAGE.toString();

	// error code and massage patterns
	private static final Pattern PATTERN_ERRCODE = Pattern.compile(ERR_CODE);
	private static final Pattern PATTERN_ERRMESSAGE = Pattern.compile(ERR_MESSAGE);

	/**
	 * Error enum.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
     *         (c) Javlin, a.s. (www.javlin.eu)
     *
     * @created 14.8.2007
	 */
	public static enum Error {
		ERR_CODE,
		ERR_MESSAGE;
	}

	// inner value of this class
	private Error error;
	
	/**
	 * Constructor.
	 */
	public ErrorMappingElement(Error error) {
		this.error = error;
	}
	
	/**
	 * Gets error value.
	 * 
	 * @return - error
	 */
	public Error getError() {
		return error;
	}
	
	/**
	 * Gets ErrorMappingElement or null if the error code mapping pattern doesn't exist in the statement.
	 * 
	 * @param rawElement
	 * @return
	 */
	public static ErrorMappingElement fromString(String rawElement) {
		Matcher matcherErrCode = PATTERN_ERRCODE.matcher(rawElement);
		Matcher matcherErrMessage = PATTERN_ERRMESSAGE.matcher(rawElement);

		if (matcherErrCode.find()) {
			return new ErrorMappingElement(Error.ERR_CODE);
		}
		if (matcherErrMessage.find()) {
			return new ErrorMappingElement(Error.ERR_MESSAGE);
		}
		return null;
	}

	@Override
    public String toString() {
    	return error.name();
    }

}
