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
package org.jetel.util.joinKey;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.Defaults;
import org.jetel.enums.OrderEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author Agata Vackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Jun 13, 2008
 *
 */

public class JoinKeyUtils {

	private final static Pattern TAIL_PATTERN = Pattern.compile("\\s*[;|:#]+\\s*$");
	
	//                                                                  keyName      [(    order    )]? 
	private final static Pattern ORDERED_KEY_PATTERN = Pattern.compile("([^\\(]+)(([\\(]([^\\)]+)[\\)])|$)");

	private final static int MASTER = 0;
	private final static int SLAVE = 1;
	
	private final static int RECORD_INDEX = 0;
	private final static int FIELD_INDEX = 1;
	
	/**
	 * Parses join string for HashJoin component.
	 * @param joinBy Join string
	 * @param inMetadata collection of input metadata, master metadata must be first in the collection
	 * @return Each element of outer array contains array of arrays of strings. Each subarray represents one driver/slave key list.
	 * First element of outer array is for driver key lists, the second one is for slave key lists.(res[MASTER|SLAVE}[slaveNo][fieldNo])
	 * @throws ComponentNotReadyException
	 */
	public static String[][][] parseHashJoinKey(String joinBy, List<DataRecordMetadata> inMetadata) 
			throws ComponentNotReadyException {	
		if (StringUtils.isEmpty(joinBy)) return new String[2][0][0];
		
		Iterator<DataRecordMetadata> itor = inMetadata != null ? inMetadata.iterator() : null;
		DataRecordMetadata masterMetadata = itor != null ? itor.next() : null;
		Map<String, Object[]> slaveMetadata = new LinkedHashMap<String, Object[]>(itor != null && inMetadata.size() > 1 ? 
				inMetadata.size()  - 1 : 0);
		if (itor != null) {
			DataRecordMetadata metadata;
			for (int i = 0; i < inMetadata.size() - 1; i++) {
				metadata = itor.next();
    			if (metadata == null) {
    				throw new ComponentNotReadyException("Input metadata on port number " + i + " are null!!!");
    			}
				slaveMetadata.put(metadata.getName(), new Object[]{metadata, i});
			}
		}
		String[][][] res = new String[2][][];
		String[] mappings = joinBy.split("#");
		String[] recName = new String[mappings.length]; 
		res[MASTER] = new String[mappings.length][];
		res[SLAVE] = new String[mappings.length][];
		String[] tmp;
		Boolean isNumber = null;
		for (int slaveNum = 0; slaveNum < mappings.length; slaveNum++) {
			if (slaveNum > 0 && mappings[slaveNum].length() == 0) {
				// use first mapping instead of the empty one
				res[MASTER][slaveNum] = res[MASTER][0];
				res[SLAVE][slaveNum] = res[SLAVE][0];
				continue;
			}
			String[] pairs = mappings[slaveNum].split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			res[MASTER][slaveNum] = new String[pairs.length];	// master key
			res[SLAVE][slaveNum] = new String[pairs.length];	// slave key
			for (int fieldNum = 0; fieldNum < pairs.length; fieldNum++) {
				String[] fields = pairs[fieldNum].split("\\s*=\\s*", 2);
				if (fields.length == 0) {
					throw new ComponentNotReadyException("Invalid key mapping: " + mappings[slaveNum]);
				}
				if (isNumber == null) {
					tmp = splitFieldName(fields[0]);
					if (tmp[RECORD_INDEX] != null) {
						isNumber = StringUtils.isInteger(tmp[RECORD_INDEX]) == 0;
					}else if (fields.length == 2){
						tmp = splitFieldName(fields[1]);
						if (tmp[RECORD_INDEX] != null) {
							isNumber = StringUtils.isInteger(tmp[RECORD_INDEX]) == 0;
						}
					}
				}
				if (fields.length == 1 || fields[1].length() == 0) {	// only master field is specified
					res[MASTER][slaveNum][fieldNum] = res[SLAVE][slaveNum][fieldNum] = splitFieldName(fields[0])[FIELD_INDEX];	// use it for both master and slave
				} else if (fields[0].length() == 0 && slaveNum > 0) {			// only slave key is specified
					res[MASTER][slaveNum][fieldNum] = res[MASTER][0][fieldNum];	// inherit master from first mapping 
					tmp = splitFieldName(fields[1]);
					res[SLAVE][slaveNum][fieldNum] = tmp[1];
					if (tmp[0] != null && recName[slaveNum] == null) {
						recName[slaveNum] = tmp[RECORD_INDEX];
						if (slaveMetadata.size() > 0) {//check name of metadata or port number
							if ((isNumber && Integer.valueOf(tmp[RECORD_INDEX]) > inMetadata.size() - 1) 
								|| (!isNumber && !slaveMetadata.containsKey(tmp[RECORD_INDEX]))) {
								throw new ComponentNotReadyException("Metadata " + StringUtils.quote(tmp[RECORD_INDEX]) + 
										" are not connected!!!");
							}
						}
					}else if (tmp[RECORD_INDEX] != null && !tmp[RECORD_INDEX].equals(recName[slaveNum])) {
						throw new ComponentNotReadyException("Wrong slave definition in " + 
								StringUtils.quote(mappings[slaveNum]) + " at or near " + StringUtils.quote(tmp[RECORD_INDEX]));
					}
				} else {//masterField=slaveField
					res[MASTER][slaveNum][fieldNum] = splitFieldName(fields[0])[FIELD_INDEX];
					tmp = splitFieldName(fields[1]);
					res[SLAVE][slaveNum][fieldNum] = tmp[FIELD_INDEX];
					if (tmp[RECORD_INDEX] != null && recName[slaveNum] == null) {
						recName[slaveNum] = tmp[RECORD_INDEX];
						if (slaveMetadata.size() > 0) {//check name of metadata or port number
							if ((isNumber && Integer.valueOf(tmp[RECORD_INDEX]) > inMetadata.size() - 1) 
								|| (!isNumber && !slaveMetadata.containsKey(tmp[RECORD_INDEX]))) {
								throw new ComponentNotReadyException("Metadata " + StringUtils.quote(tmp[RECORD_INDEX]) + 
										" are not connected!!!");
							}
						}
					}else if (tmp[RECORD_INDEX] != null && !tmp[RECORD_INDEX].equals(recName[slaveNum])) {
						throw new ComponentNotReadyException("Wrong slave definition in " + 
								StringUtils.quote(mappings[slaveNum]) + " at or near " + StringUtils.quote(tmp[RECORD_INDEX]));
					}
				}
			}
		}
		
		//eventually reorder result
		if (slaveMetadata != null && slaveMetadata.size() > 0) {
			if (masterMetadata == null) {
				throw new ComponentNotReadyException("Master metadata are null!!!");
			}
			String[][][] reorder = new String[2][mappings.length][];
			int slaveNum;
			DataRecordMetadata metadata;
			for(int i = 0; i < recName.length; i++){
				try {
					if (recName[i] != null) {
						slaveNum = isNumber ? Integer.parseInt(recName[i]) - 1 : (Integer) slaveMetadata.get(recName[i])[1];
						metadata = isNumber ? inMetadata.get(slaveNum + 1) : (DataRecordMetadata) slaveMetadata.get(recName[i])[0];
						reorder[MASTER][slaveNum] = res[MASTER][i];
						reorder[SLAVE][slaveNum] = res[SLAVE][i];
					} else {
						slaveNum = i;
						metadata = inMetadata.get(i + 1);
						reorder[MASTER][i] = res[MASTER][i];
						reorder[SLAVE][i] = res[SLAVE][i];
					}
				} catch (IndexOutOfBoundsException e) {
					// reference to metadata number, that doesn't exist or more mappings, that number if input metadata
					Pattern indexPattern = Pattern.compile("Index\\:\\s*(\\d+)");
					Pattern sizePattern = Pattern.compile("Size\\:\\s*(\\d+)");
					Matcher indexMatcher = indexPattern.matcher(e.getMessage());
					Matcher sizeMatcher = sizePattern.matcher(e.getMessage());
					throw new ComponentNotReadyException("Input port " + (indexMatcher.find() ? indexMatcher.group(1) : "?") + 
							" not defined or mapping has too many elements (number of input ports: " + 
							(sizeMatcher.find() ? sizeMatcher.group(1) : "?") + ")");
				}
					//check fields in metadata
					//we can't do that, because if only master is given we can set slave directly later
					//				for (int j = 0; j < reorder[MASTER][slaveNum].length; j++) {
					//					if (masterMetadata.getFieldPosition(reorder[MASTER][slaveNum][j]) == -1){
					//						throw new ComponentNotReadyException("Field " + StringUtils.quote(reorder[MASTER][slaveNum][j]) +
					//							" specified as key field, doesn't exist in master metadata.");
					//					}
					//					if (metadata.getFieldPosition(reorder[SLAVE][slaveNum][j]) == -1){
					//						throw new ComponentNotReadyException("Field " + StringUtils.quote(reorder[SLAVE][slaveNum][j]) +
					//							" specified as key field, doesn't exist in " + StringUtils.quote(metadata.getName()) + " metadata.");
					//					}
					//				}
			}
			res = reorder;
			
		}
		
		return res;
	}

	/**
	 * Parses join string for MergeJoin component
	 * 
	 * Magda's notes: before this function was used also in GUI for keys of type "join" (CheckForeignKey, DataIntersection), 
	 * parses also the "$field1=$aa;$field2=$bb" format, but expects complete mapping
	 * 
	 * @param joinBy input string
	 * @param inMetadata collection of input metadata, master metadata must be first in the collection
	 * @return arrays of join keys: master keys and then slave keys in order as order of slave metadata in collection
	 * 	(res[portNo][fieldNo]). Difference from hash join key is that master key for all slaves has to be the same and
	 * 	length of join key must be the same for each slave.
	 * @throws ComponentNotReadyException
	 */
	public static OrderedKey[][] parseMergeJoinOrderedKey(String joinBy, List<DataRecordMetadata> inMetadata) throws ComponentNotReadyException {
		if (StringUtils.isEmpty(joinBy)) return new OrderedKey[0][0];
		Iterator<DataRecordMetadata> itor = inMetadata != null ? inMetadata.iterator() : null;
		DataRecordMetadata masterMetadata = itor != null ? itor.next() : null;
		Map<String, Object[]> slaveMetadata = new LinkedHashMap<String, Object[]>(itor != null && inMetadata.size() > 1 ? 
				inMetadata.size()  - 1 : 0);
		if (itor != null) {
			if (masterMetadata == null) {
				throw new ComponentNotReadyException("Master metadata are null!!!");
			}
			DataRecordMetadata metadata;
			for (int i = 0; i < inMetadata.size() - 1; i++) {
				metadata = itor.next();
    		if (metadata == null) {
    				throw new ComponentNotReadyException("Input metadata on port number " + (i + 1) + " are null!!!");
    			}
				slaveMetadata.put(metadata.getName(), new Object[]{metadata, i});
			}
		}
		
		String[] mappings = joinBy.split("#");
		Map<Integer, String[]> result = new HashMap<Integer, String[]>();
		int keyLenght = -1;
		String[] masterTmp;
		String[] slaveTmp;
		String[] pairs;
		String[] fields;
		int portNo;
		int tmpPortNo;
		String[] tmpKey;
		String[] tmpMasterKey;
		Boolean isNumber = null;
		for (int i = 0; i < mappings.length; i++) {
			pairs = mappings[i].split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			if (i == 0) {
				keyLenght = pairs.length;
			}else if (pairs.length != keyLenght){
				throw new ComponentNotReadyException("Wrong key length for " + StringUtils.quote(mappings[i]));
			}
			portNo = -1;
			tmpKey = null;
			for (int j = 0; j < pairs.length; j++) {
				fields = pairs[j].split("\\s*=\\s*", 2);
				if (fields.length == 0) {
					throw new ComponentNotReadyException("Invalid key mapping: " + StringUtils.quote(mappings[i]));
				}
				//set one key or key pair: masterField=slaveField
				masterTmp = fields.length == 1 ? null : splitFieldName(fields[0]);
				slaveTmp = fields.length == 1 ? splitFieldName(fields[0]) : splitFieldName(fields[1]);
				if (isNumber == null) {//notatation [$]recName.fieldName or [$]recNumber.fieldName ?
					isNumber = (masterTmp != null && masterTmp[RECORD_INDEX] != null && StringUtils.isInteger(masterTmp[RECORD_INDEX]) == 0) 
						|| (slaveTmp[RECORD_INDEX] != null && StringUtils.isInteger(slaveTmp[RECORD_INDEX]) == 0);
				}
				//find port number for setting key
				if (slaveTmp[RECORD_INDEX] == null) {//record name not given - we take order 
					tmpPortNo = masterTmp == null ? i : i + 1;//setting master or master and slave (masterfield = slaveField)
				}else if ((isNumber && Integer.valueOf(slaveTmp[RECORD_INDEX]) == 0) 
						|| (!isNumber && slaveTmp[RECORD_INDEX].equals(masterMetadata.getName()))) {//set key for master
					if (masterTmp != null) {//trying to set master on the right side of key pair (masterfield = slaveField)
						throw new ComponentNotReadyException("Invalid key definition - master key has to be on the right side: " +
								StringUtils.quote(pairs[j]));
					}
					tmpPortNo = 0;
				}else if ((isNumber && Integer.valueOf(slaveTmp[RECORD_INDEX]) > inMetadata.size() -1) 
						|| (!isNumber && !slaveMetadata.containsKey(slaveTmp[RECORD_INDEX]))) {//found record name, which there isn't in input metadata collection
					throw new ComponentNotReadyException("Metadata " + StringUtils.quote(slaveTmp[RECORD_INDEX]) + 
							" are not connected!!!");
				}else{//found requested slave
					tmpPortNo = isNumber ? Integer.valueOf(slaveTmp[RECORD_INDEX]) : ((Integer)slaveMetadata.get(slaveTmp[RECORD_INDEX])[1]) + 1;
				}
				if (portNo == -1) {//set new key
					portNo = tmpPortNo;
					tmpKey = new String[keyLenght];
					result.put(portNo, tmpKey);
				}else if (tmpPortNo != portNo) {//requested port number different then current one
					throw new ComponentNotReadyException("Invalid slave in " + StringUtils.quote(mappings[i]) + 
							" at or near " + StringUtils.quote(pairs[j]));
				}
				if (masterTmp != null) {//mapping
					if (!result.containsKey(MASTER)) {//master key has not been set yet
						tmpMasterKey = new String[keyLenght];
						result.put(MASTER, tmpMasterKey);
					}else{//get master key set before
						tmpMasterKey = result.get(MASTER);
					}
					if (tmpMasterKey[j] == null) {//master key has not been set yet
						tmpMasterKey[j] = masterTmp[FIELD_INDEX];
					}else if (!masterTmp[FIELD_INDEX].equals(tmpMasterKey[j])) {//curent master field different then the one set before
						throw new ComponentNotReadyException("Master field was set to " + StringUtils.quote(tmpMasterKey[j]) +
								" but found " + StringUtils.quote(masterTmp[FIELD_INDEX]) + " in " +
								StringUtils.quote(mappings[i]));
					}
				}
				tmpKey[j] = slaveTmp[FIELD_INDEX];
			}
		}
		
		//set proper order in result
		OrderedKey[][] keyOrdered = new OrderedKey[result.size()][];
		DataRecordMetadata metadata = null;
		String aTmp[];
		for (int i = 0; i < keyOrdered.length; i++) {
			aTmp = result.get(i);
			keyOrdered[i] = new OrderedKey[aTmp.length];
			if (itor != null) {
				if (i >= inMetadata.size()) {
					throw new ComponentNotReadyException("Key size (" + keyOrdered.length + ") is greater then number of input metadata ("
							+ inMetadata.size() + ").");
				}
				metadata = inMetadata.get(i);
				for (int k = 0; k < aTmp.length; k++) {
					keyOrdered[i][k] = parseOrderedKey(aTmp[k]);
					if (metadata.getFieldPosition(keyOrdered[i][k].getKeyName()) == -1) {
						throw new ComponentNotReadyException("Field " + StringUtils.quote(keyOrdered[i][k].getKeyName()) +
								" specified as key field, doesn't exist in " + StringUtils.quote(metadata.getName()) + " metadata.");
					}
				}
			}
		}
		return keyOrdered;
	}

	/**
	 * Parses a field string with optional order notation.
	 * @param orderedKey
	 * @return
	 */
	private static OrderedKey parseOrderedKey(String orderedKey) {
		Matcher matcher = ORDERED_KEY_PATTERN.matcher(orderedKey);
		String sOrder;
		if (!matcher.find() || (sOrder = matcher.group(4)) == null) {
			return new OrderedKey(orderedKey, null);			
		}
		return new OrderedKey(matcher.group(1), OrderEnum.fromString(sOrder));			
	}
	
	public static String[][] parseMergeJoinKey(String joinBy, List<DataRecordMetadata> inMetadata)
			throws ComponentNotReadyException{
		OrderedKey[][] keyOrdered = parseMergeJoinOrderedKey(joinBy, inMetadata);
		if (keyOrdered.length == 0) return new String[0][0];
		String[][] sKeyOrdered;
		sKeyOrdered = new String[keyOrdered.length][keyOrdered[0].length];
		for (int i=0; i<keyOrdered.length; i++) {
			for (int j=0; j<keyOrdered[0].length; j++) {
				sKeyOrdered[i][j] = keyOrdered[i][j].getKeyName();
			}
		}
		return sKeyOrdered;
	}
	
	/**
	 * Parses field name from form: <i>[$][recName.]fieldName</i> to array: <i>{recName, fieldName}</i>
	 * 
	 * @param field
	 * @return
	 */
	public static String[] splitFieldName(String field){
		if (field.startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {
			field = field.substring(Defaults.CLOVER_FIELD_INDICATOR.length());
		}
		String[] res = field.split("\\.");
		if (res.length == 2) return res;
		if (res.length == 1) return new String[]{null, res[0]};
		return new String[]{res[res.length - 1], res[res.length - 2]};
	}
	
	
	/**
	 * Splits mapping in form "target assignSign source" to array {target, source}. If target or source starts with
	 * CLOVER_FIELD_INDICATOR ($), removes it. If after <i>source</i> there are characters from set: ";|:#" they are
	 * removed (used for join keys). <br>
	 * Default second parameter is: Defaults.ASSIGN_SIGN + "|=". It is ":=" or "=". Eg. for string <i>$field1:=$field2</i>
	 * returns {field1, field2}
	 * 
	 * @param mappingStr
	 * @param assignSignRegex
	 * @return
	 */
	public static String[] getMappingItemsFromMappingString(String mappingStr, String assignSignRegex) {
		String[] mapping = mappingStr.split(assignSignRegex);
		String target = mapping[0].trim();
		if (target.startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {
			target = target.substring(Defaults.CLOVER_FIELD_INDICATOR.length());
		}
		target = StringUtils.stringToSpecChar(target);
		String source = null;
		if (mapping.length > 1) {
			source = mapping[1].trim();
			if (source.startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {
				source = source.substring(Defaults.CLOVER_FIELD_INDICATOR.length());
			}
			Matcher matcher = TAIL_PATTERN.matcher(source);
			int index = -1;
			while (matcher.find()) {
				index = matcher.start();
			}
			if (index > -1) {
				source = source.substring(0, index);
			}
			source = StringUtils.stringToSpecChar(source);
		}
		return new String[] { target, source };
	}

	/**
	 * Splits mapping in form "target := source" or "target = source" to array {target, source}. If target or source
	 * starts with CLOVER_FIELD_INDICATOR ($), removes it. <br>
	 * Eg. for string <i>$field1:=$field2</i> returns {field1, field2}
	 * 
	 * @param mappingStr
	 * @return
	 */
	public static String[] getMappingItemsFromMappingString(String mappingStr) {
		return getMappingItemsFromMappingString(mappingStr, Defaults.ASSIGN_SIGN + "|=");
	}

	public static String toString(OrderedKey[][] joiners) {
		//TODO
		StringBuffer joinStr = new StringBuffer();		
		for (int i = 0; i < joiners.length; i++) {
			for (int j=0; j < joiners[i].length; j++) {
				joinStr.append(joiners[i][j].getKeyName() + "(" + (joiners[i][j].getOrdering() == OrderEnum.ASC ? "a" : "d") + ")");
			}
			joinStr.append('#');
		}
		
		return joinStr.toString();		
		
	}


}
