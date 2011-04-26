package org.jetel.mapping.element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.mapping.MappingSource;
import org.jetel.mapping.MappingTarget;


/**
 * This class contains return code mapping element
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class ReturnCodeMappingElement implements MappingSource, MappingTarget {
	
	public static final String TYPE = "com.initiatesystems.etl.mapping.clover";

	// return code string
	private static final String RETURN_CODE = RetCode.RETURN_CODE.toString();

	// return code patterns
	private static final Pattern PATTERN_RETCODE = Pattern.compile(RETURN_CODE);

	/**
	 * Return code enum.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
     *         (c) Javlin, a.s. (www.javlin.eu)
	 */
	public static enum RetCode {
		RETURN_CODE;
	}

	// inner value of this class
	private RetCode retCode;
	
	/**
	 * Constructor.
	 */
	public ReturnCodeMappingElement(RetCode retCode) {
		this.retCode = retCode;
	}
	
	/**
	 * Gets return code.
	 * 
	 * @return - retCode
	 */
	public RetCode getRetCode() {
		return retCode;
	}
	
	/**
	 * Gets ReturnCodeMappingElement or null if the return code mapping pattern doesn't exist in the statement.
	 * 
	 * @param rawElement
	 * @return
	 */
	public static ReturnCodeMappingElement fromString(String rawElement) {
		Matcher matcherErrCode = PATTERN_RETCODE.matcher(rawElement);

		if (matcherErrCode.find()) {
			return new ReturnCodeMappingElement(RetCode.RETURN_CODE);
		}
		return null;
	}

	@Override
    public String toString() {
    	return retCode.name();
    }

}
