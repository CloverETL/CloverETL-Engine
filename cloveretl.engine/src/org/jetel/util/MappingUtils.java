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
