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
package org.jetel.component.tree.reader.xml;

import java.util.List;

import net.sf.saxon.om.NodeInfo;

import org.jetel.component.tree.reader.ValueHandler;
import org.jetel.data.DataField;
import org.jetel.data.ListDataField;
import org.jetel.exception.JetelRuntimeException;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19 Jan 2012
 */
public class XmlValueHandler implements ValueHandler {

	@Override
	public void storeValueToField(Object value, DataField field, boolean doTrim) {
		List<?> nodeList;
		if (value instanceof List<?>) {
			nodeList = (List<?>) value;
		} else {
			throw new IllegalArgumentException("Unexpected type of value " + value);
		}

		switch (field.getMetadata().getContainerType()) {
		case LIST:
			ListDataField list = (ListDataField) field;
			for (Object object : nodeList) {
				fillValueToField(object, list.addField(), doTrim);
			}
			break;
		case SINGLE:
			switch (nodeList.size()) {
			case 0:
				return;
			case 1:
				fillValueToField(nodeList.get(0), field, doTrim);
				break;
			default:
				throw new JetelRuntimeException("Result of xpath filling field '" + field.getMetadata().getName() + "' contains two or more values!");
			}
			
			break;
		}
	}
	
	protected String toString(Object value) {
		String stringValue;
		if (value instanceof NodeInfo) {
			stringValue = ((NodeInfo) value).getStringValue();
		} else {
			stringValue = value.toString();
		}
		
		return stringValue;
	}
	
	private void fillValueToField(Object value, DataField field, boolean doTrim) {
		String stringValue = toString(value);
		
		if (doTrim) {
			stringValue = stringValue.trim();
		}
		
		field.fromString(stringValue);
	}

	@Override
	public void storeValueToField(Object value, DataField field) {
		storeValueToField(value, field, false);
	}
}
