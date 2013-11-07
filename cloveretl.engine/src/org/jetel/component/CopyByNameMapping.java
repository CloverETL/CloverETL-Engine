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
package org.jetel.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;

/**
 * Simple replacement for {@link org.jetel.component.CustomizedRecordTransform} 
 * in CTL wildcard mapping.
 * 
 * Maps exactly one record to one record,
 * exact matches have priority, then
 * remaining input fields are mapped one by one
 * to the first unmapped case-insensitively matching output field,
 * respectively.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4. 11. 2013
 */
public class CopyByNameMapping {
	
	private List<DataField> sourceFields;
	private List<DataField> targetFields;
	
	public CopyByNameMapping(DataRecord source, DataRecord target) {
		Map<String, String> mapping = new HashMap<String, String>(source.getNumFields());
		Set<String> mappedOutputFields = new HashSet<String>(target.getNumFields());
		
		// first map ALL exact matches
		for (DataField sourceField: source) {
			String sourceFieldName = sourceField.getMetadata().getName();
			if (target.hasField(sourceFieldName)) {
				mapping.put(sourceFieldName, sourceFieldName);
				mappedOutputFields.add(sourceFieldName);
			}
		}
		
		// then process case-insensitive matches; map the current input field to the first unused matching output field
		for (DataField sourceField: source) {
			String sourceFieldName = sourceField.getMetadata().getName();
			if (!mapping.containsKey(sourceFieldName)) {
				for (DataField targetField: target) {
					String targetFieldName = targetField.getMetadata().getName();
					if (targetFieldName.equalsIgnoreCase(sourceFieldName)) {
						if (!mappedOutputFields.contains(targetFieldName)) {
							mapping.put(sourceFieldName, targetFieldName);
							mappedOutputFields.add(targetFieldName);
							break;
						}
					}
				}
			}
		}
		
		sourceFields = new ArrayList<DataField>(mapping.size());
		targetFields = new ArrayList<DataField>(mapping.size());
		
		// build a one-to-one mapping 
		for (Map.Entry<String, String> entry: mapping.entrySet()) {
			addMappedFields(source.getField(entry.getKey()), target.getField(entry.getValue()));
		}
	}
	
	private void addMappedFields(DataField sourceField, DataField targetField) {
		sourceFields.add(sourceField);
		targetFields.add(targetField);
	}

	public void performMapping() {
		Iterator<DataField> sourceIterator = sourceFields.iterator();
		Iterator<DataField> targetIterator = targetFields.iterator();

		while (sourceIterator.hasNext() && targetIterator.hasNext()) {
			try {
				targetIterator.next().setValue(sourceIterator.next().getValue());
			} catch (BadDataFormatException bdfe) {
				// CLO-331: ignore - same implementation as in CustomizedRecordTransform
			}
		}
	}

}