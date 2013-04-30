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
package org.jetel.graph.dictionary;

import java.util.Map;

import org.jetel.ctl.ASTBuilder;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.ctl.data.UnknownTypeException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldType;

/**
 * Map dictionary type. 
 * Formatting and parsing Properties is not supported.
 * 
 * In order for CTL to work, content type must be set
 * for the dictionary entries. The content type
 * should be one of the names of primitive CTL datatypes. 
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27.1.2012
 */
public class MapDictionaryType extends DictionaryType {

	public static final String TYPE_ID = "map";
	
	private final TLType mapType = TLType.createMap(TLTypePrimitive.STRING, null);

	/**
	 * Constructor.
	 */
	public MapDictionaryType() {
		super(TYPE_ID, Map.class);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.DictionaryType#init(java.lang.Object, org.jetel.graph.dictionary.Dictionary)
	 */
	@Override
	public Object init(Object value, Dictionary dictionary) throws ComponentNotReadyException {
		return value;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.dictionary.IDictionaryType#isValidValue(java.lang.Object)
	 */
	@Override
	public boolean isValidValue(Object value) {
		return (value == null) || (value instanceof Map);
	}

	@Override
	public TLType getTLType() {
		return mapType;
	}
	
	@Override
	public DataFieldType getFieldType(String contentType) {
		//contentType parameter should contains name of clover data type,
		//which is derived from CTL data types
		TLType tlType = ASTBuilder.getTypeByContentType(contentType);
		if (tlType != null) {
			try {
				return TLTypePrimitive.toCloverType(tlType);
			} catch (UnknownTypeException e) {
				//tlType cannot be converted to clover type
				//return null
			}
		}
		return null;
	}

	@Override
	public DataFieldContainerType getFieldContainerType() {
		return DataFieldContainerType.MAP;
	}
	
}
