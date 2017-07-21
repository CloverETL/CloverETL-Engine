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
package org.jetel.data.xsd;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.string.StringUtils;

/**
 * Class ConvertorRegistry serves as the registry component for convertors between cloverETL data types
 * and another data model system.  
 * @author Pavel Pospichal
 */
public class ConvertorRegistry {

	private static final Log logger = LogFactory.getLog(ConvertorRegistry.class);
	private static List<IGenericConvertor> convertors = new ArrayList<IGenericConvertor>();
	
	static {
		try {
			Class.forName(CloverBooleanConvertor.class.getName());
			Class.forName(CloverByteArrayConvertor.class.getName());
			Class.forName(CloverDateConvertor.class.getName());
			Class.forName(CloverDateTimeConvertor.class.getName());
			Class.forName(CloverDecimalConvertor.class.getName());
			Class.forName(CloverNumericConvertor.class.getName());
			Class.forName(CloverStringConvertor.class.getName());
			Class.forName(CloverIntegerConvertor.class.getName());
			Class.forName(CloverLongConvertor.class.getName());
		} catch (ClassNotFoundException e) {
		}
	}
	
	public static void registerConvertor(IGenericConvertor convertor) {
		if (convertors.contains(convertor)) {
			logger.warn("Convertor [" + convertor.getClass().getName() + "] already registered.");
			return;
		}
		
		convertors.add(convertor);
		logger.debug("Convertor [" + convertor.getClass().getName() + "] registered.");
	}
	
	/* TODO: the selection of particular convertor is elementary based on the resolution of clover-specific data type,
	 * the data types defined by external data model may have more than one appropriate representation 
	 * for particular clover-specific data type, so the concrete convertor have to be specified by addtional criterions  
	 */
	public static IGenericConvertor getConvertor(String cloverDataTypeCriteria, String externalDataTypeCriteria) {
		
		// TODO: should be replaced by some hash access
		for (IGenericConvertor convertor : convertors) {
			boolean selected = false;
			if (convertor.supportsCloverType(cloverDataTypeCriteria)) {
				selected = true;
			}

			if (!StringUtils.isEmpty(externalDataTypeCriteria)) {
				if (convertor.supportsExternalSystemType(externalDataTypeCriteria)) {
					selected = selected && true;
				} else {
					selected = false;
				}
				
			}

			if (selected)
				return convertor;
		}
		
		return null;
	}
	
	public static IGenericConvertor getConvertor(String cloverDataTypeCriteria) {
		return getConvertor(cloverDataTypeCriteria, null);
	}
}
