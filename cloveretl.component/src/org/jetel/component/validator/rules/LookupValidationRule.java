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
package org.jetel.component.validator.rules;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 25.3.2013
 */
@XmlRootElement(name="lookup")
@XmlType(propOrder={"lookup", "keyMapping", "policyJAXB"})
public class LookupValidationRule extends AbstractValidationRule {
	public static final int ERROR_INIT = 1001;
	public static final int ERROR_MAPPING = 1002;
	public static final int ERROR_RECORD_PRESENT = 1003;
	public static final int ERROR_RECORD_MISSING= 1004;
	
	public static enum POLICY {
		REJECT_MISSING, REJECT_PRESENT;
		@Override
		public String toString() {
			if(this.equals(REJECT_MISSING)) {
				return "Reject missing";
			}
			return "Reject present";
		}
	}
	
	@XmlElement(name="lookup",required=true)
	private StringEnumValidationParamNode lookup = new StringEnumValidationParamNode();
	
	@XmlElement(name="keyMapping",required=true)
	private StringValidationParamNode keyMapping = new StringValidationParamNode();
	
	private EnumValidationParamNode policy = new EnumValidationParamNode(POLICY.values(), POLICY.REJECT_MISSING);
	@XmlElement(name="policy", required=true)
	@SuppressWarnings("unused")
	private String getPolicyJAXB() { return ((Enum<?>) policy.getValue()).name(); }
	@SuppressWarnings("unused")
	private void setPolicyJAXB(String input) { this.policy.setFromString(input); }
	
	private LookupTable tempLookupTable;
	private Lookup tempLookup;
	private DataRecord tempRecord;
	private Map<String, String> tempMapping;

	@Override
	protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		lookup.setName("Lookup name");
		lookup.setOptions(graphWrapper.getLookupTables().toArray(new String[0]));
		params.add(lookup);
		keyMapping.setName("Key mapping");
		keyMapping.setTooltip("Mapping selected target fields to parts of lookup key.\nFor example: key1=field3,key2=field1,key3=field2");
		params.add(keyMapping);
		policy.setName("Rule policy");
		params.add(policy);
		return params;
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.UNORDERED_FIELDS;
	}
	
	private void init(GraphWrapper graphWrapper) throws IllegalArgumentException {
		if(tempLookupTable == null) {
			tempLookupTable = graphWrapper.getLookupTable(resolve(lookup.getValue()));
			if(tempLookupTable == null) {
				throw new IllegalArgumentException();
			}
			try {
				tempLookupTable.init();
			} catch (ComponentNotReadyException ex) {
				throw new IllegalArgumentException(ex);
			}
		}
		if(tempLookup == null) {
			DataRecordMetadata metadata = null;
			try {
				metadata = tempLookupTable.getKeyMetadata();
				tempLookup = tempLookupTable.createLookup(new RecordKey(metadata.getFieldNamesArray(), metadata));
			} catch (ComponentNotReadyException ex) {
				throw new IllegalArgumentException(ex);
			}
			tempRecord = DataRecordFactory.newRecord(metadata);
			tempRecord.init();
		}
	}
	private void populateTempRecord(Map<String, DataField> values) throws ParseException {
		tempRecord.reset();
		tempRecord.init();
		if(tempMapping == null) {
			tempMapping = ValidatorUtils.parseStringToMap(resolve(keyMapping.getValue()), "=", ",", true);
		}
		for(Map.Entry<String, String> temp : tempMapping.entrySet()) {
			tempRecord.getField(temp.getKey()).setValue(values.get(temp.getValue()));
		}
	}
	
	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		setPropertyRefResolver(graphWrapper);
		logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		
		String resolvedTarget = resolve(target.getValue());
		
		// Extract target fields
		String[] fields = ValidatorUtils.parseTargets(resolvedTarget);
		Map<String, DataField> values = new HashMap<String, DataField>();
		Map<String, String> valuesInString = new HashMap<String, String>();
		for(String field : fields) {
			values.put(field, record.getField(field));
			valuesInString.put(field, record.getField(field).toString());
		}
		
		try {
			init(graphWrapper);
		} catch (IllegalArgumentException ex) {
			raiseError(ea, ERROR_INIT, "Error on initializing lookup table, lookup or record.", graphWrapper.getNodePath(this), fields, valuesInString);
			return State.INVALID;
		}
		try {
			populateTempRecord(values);
		} catch (Exception ex) {
			raiseError(ea, ERROR_MAPPING, "Mapping is incorrect.", graphWrapper.getNodePath(this), fields, valuesInString);
			return State.INVALID;
		}
		tempLookup.seek(tempRecord);
		if(policy.getValue() == POLICY.REJECT_MISSING && tempLookup.getNumFound() > 0) {
			logSuccess("Given field(s) values was found in the lookup table.");
			return State.VALID;
		}
		if(policy.getValue() == POLICY.REJECT_PRESENT && tempLookup.getNumFound() == 0) {
			logSuccess("Given field(s) values was not present in the lookup table.");
			return State.VALID;
		}
		if(policy.getValue() == POLICY.REJECT_MISSING) {
			raiseError(ea, ERROR_RECORD_MISSING, "Given field(s) values was not present in the lookup table. Missing are invalid.", graphWrapper.getNodePath(this), fields, valuesInString);
		} else {
			raiseError(ea, ERROR_RECORD_PRESENT, "Given field(s) values was found in the lookup table. Present are invalid.", graphWrapper.getNodePath(this), fields, valuesInString);
		}
		return State.INVALID;
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		setPropertyRefResolver(graphWrapper);
		boolean state = true;
		String resolvedTarget = resolve(target.getValue());
		String resolvedLookup = resolve(lookup.getValue());
		String resolvedKeyMapping = resolve(keyMapping.getValue());
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(!ValidatorUtils.areValidFields(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Some of target fields are not present in input metadata.");
			state = false;
		}
		LookupTable lookupTable = null;
		if(resolvedLookup.isEmpty()) {
			accumulator.addError(lookup, this, "No lookup table provided.");
			state = false;
		} else {
			lookupTable = graphWrapper.getLookupTable(resolvedLookup);
			if(lookupTable == null) {
				accumulator.addError(lookup, this, "Unknown lookup table.");
				state = false;
			}
		}
		if(resolvedKeyMapping.isEmpty()) {
			accumulator.addError(keyMapping, this, "Key mapping is empty.");
			state = false;
		}
		try {
			Map<String, String> map = ValidatorUtils.parseStringToMap(resolvedKeyMapping, "=", ",", true);
			String[] targets = ValidatorUtils.parseTargets(resolvedTarget);
			Arrays.sort(targets);
			Object[] mapped = map.values().toArray();
			Arrays.sort(mapped);
			if(!Arrays.equals(mapped, targets)) {
				accumulator.addError(keyMapping, this, "Key mapping contains field which is not selected in target fields.");
				state = false;
			}
			if(lookupTable != null) {
				Object[] keys = map.keySet().toArray();
				Arrays.sort(keys);
				try {
					Object[] lookupKeys = lookupTable.getKeyMetadata().getFieldNamesArray();
					Arrays.sort(lookupKeys);
					if(!Arrays.equals(keys, lookupKeys)) {
						accumulator.addError(keyMapping, this, "Key mapping contains field which is not key part of lookup table.");
						state = false;
					}
				} catch(Exception ex) {
					// Check cannot be done -> skip
				}
			}
			
		} catch (ParseException ex) {
			accumulator.addError(keyMapping, this, "Key mapping is invalid: " + ex.getMessage());
			state = false;
		}
		return state;
	}

	@Override
	public String getCommonName() {
		return "Lookup";
	}

	@Override
	public String getCommonDescription() {
		return "Checks if there is a record in lookup table which match value of chosen field."; 
	}
	

}
