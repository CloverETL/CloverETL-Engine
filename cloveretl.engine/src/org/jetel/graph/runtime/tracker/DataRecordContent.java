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
package org.jetel.graph.runtime.tracker;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This class represents unstructured data record content. 
 * TODO this class is only basic skeleton and can be changed in future
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Apr 2012
 */
public class DataRecordContent {

	//TODO this is just first suggestion how the token content could look like

	protected HashMap<String, String> content = new LinkedHashMap<String, String>();

	protected List<String> keyFieldNames;
	
	public void setRecord(DataRecord record) {
		content.clear();
		keyFieldNames = null;
		if (record != null) {
			for (DataField field : record) {
				content.put(field.getMetadata().getName(), field.toString());
			}
			keyFieldNames = record.getMetadata().getKeyFieldNames();
		}
	}
	
	public String getLabel() {
		StringBuilder result = new StringBuilder();
		if (keyFieldNames != null) {
			for (String keyFieldName : keyFieldNames) {
				result.append(keyFieldName).append("=").append(content.get(keyFieldName)).append(";");
			}
		}
		return result.length() == 0 ? "" : result.substring(0, result.length() - 1);
	}
	
	public void serialize(CloverBuffer buffer) {
	
	}
	
	public void deserialize(CloverBuffer buffer) {
	
	}

}
