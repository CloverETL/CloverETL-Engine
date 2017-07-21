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

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.mapping.MappingException;
import org.jetel.mapping.MappingSource;
import org.jetel.mapping.MappingTarget;


/**
 * This class contains clover mapping element that is consisted from port name and field name.
 * 
 * @author Martin Zatopek (martin.zatopek@opensys.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *         
 * @comments Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class CloverMappingElement implements MappingSource, MappingTarget {

	public static final String TYPE = "com.initiatesystems.etl.mapping.clover";
	private final static Pattern PATTERN = Pattern.compile("\\$(.+)\\.(.+)");
	private final static Pattern PATTERN_DEFAULT_PORT = Pattern.compile("\\$(.+)");
	private final static String DOT = ".";
	private final static String DEFAULT_PORT = "0";

	// port name
	private final String portName;
	
	// field name
	private final String fieldName;

	/**
	 * Creates new clover mapping element.
	 * 
	 * @param portName
	 * @param fieldName
	 */
	public CloverMappingElement(String portName, String fieldName) {
		this.portName = portName;
		this.fieldName = fieldName;
	}
	
	/**
	 * Gets field name.
	 * 
	 * @return - field name
	 */
	public String getFieldName() {
		return fieldName;
	}

	/**
	 * Gets port name.
	 * 
	 * @return - port name
	 */
	public String getPortName() {
		return portName;
	}
	
	/**
	 * Gets data field from a data recordu according to field name.
	 * 
	 * @param dataRecord - data record
	 * @return data field
	 * @throws MappingException
	 */
	public DataField getDataField(DataRecord dataRecord) throws MappingException {
		if(dataRecord.hasField(fieldName)) {
			return dataRecord.getField(fieldName);
		} else {
			throw new MappingException("Field '" + fieldName + "' does not exist in clover metadata '" + dataRecord.getMetadata().getName() + "'.");
		}
	}
	
	/**
	 * Gets clover field element from a string.
	 * 
	 * @param rawElement - string containing clover field element
	 * @return CloverMappingElement
	 */
	public static CloverMappingElement fromString(String rawElement) {
		Matcher matcher = PATTERN.matcher(rawElement);

		if (matcher.find()) {
			return new CloverMappingElement(matcher.group(1), matcher.group(2));
		}
		
		matcher = PATTERN_DEFAULT_PORT.matcher(rawElement);

		if (matcher.find()) {
			return new CloverMappingElement(DEFAULT_PORT, matcher.group(1));
		}
		
		return null;
	}
	
	@Override
    public String toString() {
    	return portName + DOT + fieldName;
    }
	
}
