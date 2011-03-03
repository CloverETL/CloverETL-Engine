/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.util;

import java.util.HashMap;

import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.string.StringUtils;

/**
 * @author whancock (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Feb 14, 2011
 */
public class MappingUtils {
	
	public static HashMap<String, String> parseMappingString(String mappingString) throws ComponentNotReadyException {
		
		HashMap<String, String> mappingHash = new HashMap<String, String>();
		String[] splitMapping = StringUtils.split(mappingString);
		
		for(String mapping: splitMapping) {
			String[] parsedExpression = mapping.split(Defaults.ASSIGN_SIGN);
			String inputField = parsedExpression[1].trim();
			String outputField = parsedExpression[0].trim();
			
			mappingHash.put(inputField, outputField);
		}
		
		return mappingHash;
	}
	
}
