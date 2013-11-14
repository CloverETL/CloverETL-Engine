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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

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
	
	private MappingPair[] mappings;
	
	public CopyByNameMapping(DataRecordMetadata source, DataRecordMetadata target) {
		Map<String, String> mapping = new HashMap<String, String>(source.getNumFields());
		Set<String> mappedOutputFields = new HashSet<String>(target.getNumFields());
		
		// first map ALL exact matches
		for (DataFieldMetadata sourceField: source) {
			String sourceFieldName = sourceField.getName();
			if (target.getField(sourceFieldName) != null) { // has field
				mapping.put(sourceFieldName, sourceFieldName);
				mappedOutputFields.add(sourceFieldName);
			}
		}
		
		// then process case-insensitive matches; map the current input field to the first unused matching output field
		for (DataFieldMetadata sourceField: source) {
			String sourceFieldName = sourceField.getName();
			if (!mapping.containsKey(sourceFieldName)) {
				for (DataFieldMetadata targetField: target) {
					String targetFieldName = targetField.getName();
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
		
		mappings = new MappingPair[mapping.size()];
		
		// build a one-to-one mapping 
		int i = 0;
		Map<String, Integer> sourceMap = source.getFieldNamesMap();
		Map<String, Integer> targetMap = target.getFieldNamesMap();
		for (Map.Entry<String, String> entry: mapping.entrySet()) {
			mappings[i++] = new MappingPair(sourceMap.get(entry.getKey()), targetMap.get(entry.getValue()));
		}
	}
	
	public void performMapping(DataRecord source, DataRecord target) {
		for (MappingPair mapping: mappings) {
			try {
				mapping.execute(source, target);
			} catch (BadDataFormatException bdfe) {
				// CLO-331: ignore - same implementation as in CustomizedRecordTransform
			}
		}
	}
	
	private static class MappingPair {
		private final int sourceIndex;
		private final int targetIndex;

		public MappingPair(int sourceIndex, int targetIndex) {
			this.sourceIndex = sourceIndex;
			this.targetIndex = targetIndex;
		}
		
		public void execute(DataRecord source, DataRecord target) {
			target.getField(targetIndex).setValue(source.getField(sourceIndex));
		}
	}

}