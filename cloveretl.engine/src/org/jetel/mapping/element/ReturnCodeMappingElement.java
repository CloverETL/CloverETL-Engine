/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
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
