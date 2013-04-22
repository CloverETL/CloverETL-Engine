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
package org.jetel.data.xml.mapping;


/** Represents constants used by a XML mapping
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.6.2012
 */
public class XMLMappingConstants {
	// FIXME: PrimitiveSequence is not accessible from engine (PrimitiveSequence.SEQUENCE_TYPE)
	public static final String PRIMITIVE_SEQUENCE = "PRIMITIVE_SEQUENCE";
	
	public static final String PARENT_MAPPING_REFERENCE_PREFIX = "..";
	public static final String PARENT_MAPPING_REFERENCE_SEPARATOR = "/";
	public static final String PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR = PARENT_MAPPING_REFERENCE_PREFIX + PARENT_MAPPING_REFERENCE_SEPARATOR;
	public static final String ELEMENT_VALUE_REFERENCE = "{}.";
	public static final String ELEMENT_AS_TEXT = "{}+";
	public static final String ELEMENT_CONTENTS_AS_TEXT = "{}-";
	
	
	private XMLMappingConstants() {
	}
}
