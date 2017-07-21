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
import org.jetel.metadata.DataFieldContainerType;

/**
 * Static text
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20 Dec 2010
 */
public class StaticValue implements NodeValue {

	private final String value;

	public StaticValue(String value) {
		this.value = value;
	}

	@Override
	public boolean isEmpty(DataRecord[] availableData) {
		return false;
	}

	@Override
	public String getValue(DataRecord[] availableData) {
		return value;
	}

	@Override
	public DataFieldContainerType getFieldContainerType() {
		return null;
	}
}
