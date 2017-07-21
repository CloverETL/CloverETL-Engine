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
package org.jetel.data.tree.bean.parser;

import java.lang.reflect.Array;
import java.util.List;

import org.jetel.data.DataField;
import org.jetel.data.ListDataField;
import org.jetel.data.tree.parser.ValueHandler;
import org.jetel.metadata.DataFieldCardinalityType;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.10.2011
 */
public class BeanValueHandler implements ValueHandler {

	@Override
	public void storeValueToField(Object value, DataField field) {
		if (field.getMetadata().getCardinalityType() == DataFieldCardinalityType.LIST) {
			ListDataField list = (ListDataField) field;
			if (value.getClass().isArray()) {
				for(int i = 0; i < Array.getLength(value); i++) {
					setFieldValue(Array.get(value, i), list.addField());
				}
			} else if (value instanceof List<?>) {
				List<?> valueList = (List<?>) value;
				for (Object object : valueList) {
					setFieldValue(object, list.addField());
				}
			} else {
				setFieldValue(value, list.addField());
			}
		} else {
			setFieldValue(value, field);
		}
	}
	
	private void setFieldValue(Object value, DataField field) {
		if (value instanceof Enum<?>) {
			field.setValue(value.toString());
		} else {
			field.setValue(value);
		}
	}

}
