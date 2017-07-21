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

import org.jetel.data.DataField;
import org.jetel.data.tree.parser.ValueHandler;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.10.2011
 */
public class BeanValueHandler implements ValueHandler {
	
	private Object currentValue;

	@Override
	public Object getCurrentValue() {
		return currentValue;
	}

	@Override
	public void clearCurrentValue() {
		currentValue = null;

	}

	@Override
	public void appendValue(Object value) {
		// Appending does not make any sense for pojo
		currentValue = value;
	}

	@Override
	public void storeValueToField(Object value, DataField field) {
		field.setValue(value);
	}

}
