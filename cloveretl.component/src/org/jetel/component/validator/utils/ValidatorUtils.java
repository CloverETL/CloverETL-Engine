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
package org.jetel.component.validator.utils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Stack;

import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.rules.CustomValidationRule;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Common routines used in validator
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 26.2.2013
 */
public class ValidatorUtils {
	
	public static String[] parseTargets(String target) {
		if(target == null || target.trim().isEmpty()) {
			return new String[0];
		}
		return target.trim().split(",");
	}
	
	public static Map<String, String> parseStringToMap(String input, String assignChar, String delimeter, boolean trim) throws ParseException {
		HashMap<String, String> temp = new HashMap<String, String>();
		String[] rows = input.split(delimeter);
		String[] row;
		int line = 0;
		for(String tempRow : rows) {
			row = tempRow.split(assignChar);
			if(row == null || row.length != 2) {
				throw new ParseException("Wrong mapping on part: " + line,-1);
			}
			if(trim) {
				temp.put(row[0], row[1]);
			} else {
				temp.put(row[0].trim(), row[1].trim());
			}
			line++;
		}
		return temp;
	}
	
	public static boolean isValidField(String fieldName, DataRecordMetadata inputMetadata) {
		if(inputMetadata.getField(fieldName) == null) {
			return false;
		}
		return true;
	}
	
	public static boolean areValidFields(String fields, DataRecordMetadata inputMetadata) {
		String[] temp = ValidatorUtils.parseTargets(fields);
		if (temp.length == 0) {
			return false;
		}
		for(int i = 0; i < temp.length; i++) {
			if(inputMetadata.getField(temp[i]) == null) {
				return false;
			}
		}
		return true;
	}
	
	public static Locale localeFromString(String locale) {
		String[] temp = locale.split("\\.");
		if(temp.length == 0) {
			return Locale.ENGLISH;
		}
		if(temp.length == 3) {
			return new Locale(temp[0], temp[1], temp[2]);
		}
		if(temp.length == 2) {
			return new Locale(temp[0], temp[1]);
		}
		return new Locale(temp[0]);
	}
	
	public static Map<ValidationNode, ValidationGroup> createParentTable(ValidationGroup root) {
		Map<ValidationNode, ValidationGroup> output = new HashMap<ValidationNode, ValidationGroup>();
		// Special treatment with root node
		output.put(root, null);
		// Non-recursive traversing algorithm
		Stack<ValidationGroup> nodesToProcess = new Stack<ValidationGroup>();
		nodesToProcess.push(root);
		ValidationGroup current;
		while (!nodesToProcess.isEmpty()) {
			current = nodesToProcess.pop();
			for(ValidationNode temp : current.getChildren()) {
				if(temp instanceof ValidationGroup) {
					nodesToProcess.push((ValidationGroup) temp);
				}
				output.put(temp, current);
			}
		}
		return output;
	}
	
	public static List<String> getNodePath(ValidationNode needle, Map<ValidationNode, ValidationGroup> haystack) {
		if(needle == null) {
			return null;
		}
		List<String> output = new ArrayList<String>();
		if(needle.getName().isEmpty()) {
			output.add(0, needle.getCommonName());
		} else {
			output.add(0, needle.getName());
		}
		
		ValidationGroup result = haystack.get(needle);
		while(result != null) {
			if(result.getName().isEmpty()) {
				output.add(0, result.getCommonName());
			} else {
				output.add(0, result.getName());
			}
			result = haystack.get(result);
			
		}
		return output;
	}
	
	public static LanguageSetting prepareLanguageSetting(ValidationGroup root, ValidationNode node) {
		Map<ValidationNode, ValidationGroup> parentTable = createParentTable(root);
		
		ValidationGroup currentParent = parentTable.get(node);
		LanguageSetting tempLanguageSetting = null;
		while(currentParent != null) {
			tempLanguageSetting = LanguageSetting.hierarchicMerge(tempLanguageSetting, currentParent.getLanguageSetting());
			currentParent = parentTable.get(currentParent);
		}
		return tempLanguageSetting;
	}
	
	public static void removeCustomRuleRecursive(int id, ValidationGroup root) {
		Stack<ValidationGroup> nodesToProcess = new Stack<ValidationGroup>();
		nodesToProcess.push(root);
		ValidationGroup current;
		while (!nodesToProcess.isEmpty()) {
			current = nodesToProcess.pop();
			Iterator<ValidationNode> iterator = current.getChildren().iterator();
			ValidationNode temp;
			while(iterator.hasNext()) {
				temp = iterator.next();
				if(temp instanceof ValidationGroup) {
					nodesToProcess.push((ValidationGroup) temp);
				}
				if(temp instanceof CustomValidationRule) {
					Integer ref = ((CustomValidationRule) temp).getRef().getValue();
					if(ref == null || ref.equals(Integer.valueOf(id))) {
						iterator.remove();
					}
				}
			}
		}
	}
	
	/**
	 * Returns array of all CTL2 primitive types.
	 * @see {@link TLTypePrimitive}
	 * @return all primitive types
	 */
	public static String[] getCTLTypes() {
		return new String[]{
				TLTypePrimitive.INTEGER.name(),
				TLTypePrimitive.LONG.name(),
				TLTypePrimitive.STRING.name(),
				TLTypePrimitive.BOOLEAN.name(),
				TLTypePrimitive.DATETIME.name(),
				TLTypePrimitive.DOUBLE.name(),
				TLTypePrimitive.DECIMAL.name(),
				TLType.BYTEARRAY.name()
			};
	}
	
}
