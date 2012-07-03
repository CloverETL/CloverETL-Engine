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
package org.jetel.component.tree.writer.model.runtime;

import org.jetel.data.DataRecord;
import org.jetel.util.string.StringUtils;

/**
 * Class representing possibly dynamic element/attribute name (i.e. name value depends on input record).
 * 
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.6.2012
 */
public class DynamicName {

	private final WritableValue name;
	private final WritableValue prefix;

	public DynamicName(WritableValue name, WritableValue prefix) {
		this.name = name;
		this.prefix = prefix;
	}
	
	public char[] getValue(DataRecord[] availableData) {
		String nameValue = null;
		
		Object localNameValue = name.getContent(availableData);
		if (localNameValue == null) {
			return null;
		}
		
		nameValue = String.valueOf(localNameValue);
		
		if (prefix != null) {
			Object prefixValue = prefix.getContent(availableData);
			if (prefixValue != null) {
				String prefixString = String.valueOf(prefixValue);
				if (!StringUtils.isEmpty(prefixString)) {
					nameValue =  prefixString + ":" + nameValue;
				}
			}
		}

		return nameValue.toCharArray();
	}
	
}
