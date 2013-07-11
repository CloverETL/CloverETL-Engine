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

import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.ChangeHandler;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * <p>Rule to check whether given field(s) is present or not present in selected lookup table.</p>
 * 
 * Available parameters:
 * <ul>
 * 	<li>Lookup table. ID of lookup table from graph.</li>
 *  <li>Key mapping. Mapping key to fields, comma separated. For example: part_of_lookup_key=field</li>
 *  <li>Policy. @see {@link POLICY}</li>
 * </ul>
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 25.3.2013
 * @see GraphWrapper
 */
@XmlRootElement(name="lookup")
@XmlType(propOrder={"lookupParam", "policyJAXB"})
public class LookupValidationRule extends AbstractMappingValidationRule {
	public static final int ERROR_INIT = 1001;
	public static final int ERROR_MAPPING = 1002;
	public static final int ERROR_RECORD_PRESENT = 1003;
	public static final int ERROR_RECORD_MISSING= 1004;
	
	/**
	 * Ways of treating the lookup result
	 */
	public static enum POLICY {
		REJECT_MISSING, REJECT_PRESENT;
		@Override
		public String toString() {
			if(this.equals(REJECT_MISSING)) {
				return ValidatorMessages.getString("LookupValidationRule.PolicyRejectMissing"); //$NON-NLS-1$
			}
			return ValidatorMessages.getString("LookupValidationRule.PolicyRejectPresent"); //$NON-NLS-1$
		}
	}
	
	@XmlElement(name="lookup",required=true)
	private StringEnumValidationParamNode lookupParam = new StringEnumValidationParamNode();
	
	private EnumValidationParamNode<POLICY> policy = new EnumValidationParamNode<POLICY>(POLICY.values(), POLICY.REJECT_MISSING);
	@XmlElement(name="policy", required=true)
	@SuppressWarnings("unused")
	private String getPolicyJAXB() { return policy.getValue().name(); }
	@SuppressWarnings("unused")
	private void setPolicyJAXB(String input) { this.policy.setFromString(input); }
	
	private LookupTable lookupTable;
	private RecordKey lookupKey;
	private String[] lookupKeyFieldNamesArrays = new String[0];
	private String[] inputDataKeyFields;
	private Lookup lookup;
	
	private Map<String, String> keyMappingMap;

	@Override
	protected void initializeParameters(final DataRecordMetadata inMetadata, final GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		target.setPlaceholder(ValidatorMessages.getString("LookupValidationRule.TargetParameterPlaceholder")); //$NON-NLS-1$
		lookupParam.setName(ValidatorMessages.getString("LookupValidationRule.LookupTableParameterName")); //$NON-NLS-1$
		lookupParam.setOptions(graphWrapper.getLookupTables().toArray(new String[0]));
		lookupParam.setChangeHandler(new ChangeHandler() {
			@Override
			public void changed(Object o) {
				try {
					initLookupTable(graphWrapper);
				} catch (ComponentNotReadyException e) {
					throw new JetelRuntimeException(e);
				}
			}
		});
		mappingParam.setName(ValidatorMessages.getString("LookupValidationRule.MappingParameterName")); //$NON-NLS-1$
		mappingParam.setTooltip(ValidatorMessages.getString("LookupValidationRule.MappingParameterTooltip")); //$NON-NLS-1$
		policy.setName(ValidatorMessages.getString("LookupValidationRule.PolicyParameterName")); //$NON-NLS-1$
		
		try {
			initLookupTable(graphWrapper);
		} catch (ComponentNotReadyException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(lookupParam);
		parametersContainer.add(mappingParam);
		parametersContainer.add(policy);
	}
	
	@Override
	public String[] getMappingTargetFields() {
		return lookupKeyFieldNamesArrays;
	}

	private void initLookupTable(GraphWrapper graphWrapper) throws ComponentNotReadyException {
		lookupKeyFieldNamesArrays = new String[0];
		lookupTable = graphWrapper.getLookupTable(lookupParam.getValue());
		
		if (lookupTable == null) {
			return;
		}
		lookupTable.init();
		
		DataRecordMetadata lookupKeyMetadata = lookupTable.getKeyMetadata();
		lookupKeyFieldNamesArrays = lookupKeyMetadata.getFieldNamesArray();
	}
	
	private void parseKeyMapping() throws ParseException {
		keyMappingMap = ValidatorUtils.parseMappingToMap(mappingParam.getValue());
	}
	
	@Override
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
		
		initLookupTable(graphWrapper);
		if(lookupTable == null) {
			throw new ComponentNotReadyException(MessageFormat.format(ValidatorMessages.getString("LookupValidationRule.InitErrorLookupTableNotFound"), lookupParam.getValue())); //$NON-NLS-1$
		}
		
		try {
			parseKeyMapping();
		} catch (ParseException e) {
			throw new ComponentNotReadyException(e);
		}
		
		DataRecordMetadata lookupKeyMetadata = lookupTable.getKeyMetadata();
		inputDataKeyFields = new String[lookupKeyMetadata.getNumFields()];
		
		for (int i = 0; i < inputDataKeyFields.length; i++) {
			DataFieldMetadata lookupKeyField = lookupKeyMetadata.getField(i);
			String inputFieldName = keyMappingMap.get(lookupKeyField.getName());
			if (inputFieldName == null) {
				throw new ComponentNotReadyException(MessageFormat.format(ValidatorMessages.getString("LookupValidationRule.InitErrorLookupKeyNotMapped"), lookupKeyField.getName())); //$NON-NLS-1$
			}
			inputDataKeyFields[i] = inputFieldName;
		}
		
		lookupKey = new RecordKey(inputDataKeyFields, metadata);
		lookupKey.init();
		lookup = lookupTable.createLookup(lookupKey);
	}
	
	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		
		lookup.seek(record);
		if(policy.getValue() == POLICY.REJECT_MISSING && lookup.getNumFound() > 0) {
			return State.VALID;
		}
		else if(policy.getValue() == POLICY.REJECT_PRESENT && lookup.getNumFound() == 0) {
			return State.VALID;
		}
		else {
			if (ea != null) {
				Map<String, String> valuesInString = null;
				valuesInString = new HashMap<String, String>();
				for(String field : keyMappingMap.values()) {
					valuesInString.put(field, record.getField(field).toString());
				}
				if(policy.getValue() == POLICY.REJECT_MISSING) {
					raiseError(ea, ERROR_RECORD_MISSING, ValidatorMessages.getString("LookupValidationRule.InvalidRecordMessageValuesNotFound"), inputDataKeyFields, valuesInString); //$NON-NLS-1$
				} else {
					raiseError(ea, ERROR_RECORD_PRESENT, ValidatorMessages.getString("LookupValidationRule.InvalidRecordMessageValuesFound"), inputDataKeyFields, valuesInString); //$NON-NLS-1$
				}
			}
			return State.INVALID;
		}
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		String resolvedLookup = (lookupParam.getValue());
		String resolvedKeyMapping = (mappingParam.getValue());
		try {
			parseKeyMapping();
		} catch (ParseException e) {
			accumulator.addError(mappingParam, this, e.getMessage());
			state = false;
			return state;
		}
		if(!ValidatorUtils.areValidFields(keyMappingMap.values(), inputMetadata)) { 
			accumulator.addError(target, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorTargetFieldsNotPresent")); //$NON-NLS-1$
			state = false;
		}
		LookupTable lookupTable = null;
		if(resolvedLookup.isEmpty()) {
			accumulator.addError(lookupParam, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorLookupTableNotSpecified")); //$NON-NLS-1$
			state = false;
		} else {
			lookupTable = graphWrapper.getLookupTable(resolvedLookup);
			if(lookupTable == null) {
				accumulator.addError(lookupParam, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorLookupTableNotFound")); //$NON-NLS-1$
				state = false;
			}
		}
		if(resolvedKeyMapping.isEmpty()) {
			accumulator.addError(mappingParam, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorKeyMappingEmpty")); //$NON-NLS-1$
			state = false;
		}
		try {
			Map<String, String> mapping = ValidatorUtils.parseMappingToMap(resolvedKeyMapping);
			if(lookupTable != null) {
				Set<String> mappingKeys = mapping.keySet();
				try {
					lookupTable.init();
					Set<String> lookupKeys = lookupTable.getKeyMetadata().getFieldNamesMap().keySet();
					if (!mappingKeys.containsAll(lookupKeys)) {
						accumulator.addError(mappingParam, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorKeyMappingMissesLookupTableKey")); //$NON-NLS-1$
						state = false;
					}
					if (!lookupKeys.containsAll(mappingKeys)) {
						accumulator.addError(mappingParam, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorKeyMappingMapsUnknownField")); //$NON-NLS-1$
						state = false;
					}
				} catch (ComponentNotReadyException e) {
					accumulator.addError(mappingParam, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorLookupTableInitError")); //$NON-NLS-1$
					state = false;
				}
			}
		} catch (ParseException ex) {
			accumulator.addError(mappingParam, this, ValidatorMessages.getString("LookupValidationRule.ConfigurationErrorInvalidKeyMapping") + ex.getMessage()); //$NON-NLS-1$
			state = false;
		}
		return state;
	}
	
	/**
	 * @return Param node with lookup name
	 */
	public StringEnumValidationParamNode getLookupParam() {
		return lookupParam;
	}
	
	/**
	 * @return Param node with current policy
	 */
	public EnumValidationParamNode<POLICY> getPolicy() {
		return policy;
	}

	@Override
	public String getCommonName() {
		return ValidatorMessages.getString("LookupValidationRule.CommonName"); //$NON-NLS-1$
	}

	@Override
	public String getCommonDescription() {
		return ValidatorMessages.getString("LookupValidationRule.CommonDescription");  //$NON-NLS-1$
	}
	
	@Override
	public String getDetailName() {
		return String.format("%s ('%s')", getName(), (lookupParam.getValue())); //$NON-NLS-1$
	}
	
	@Override
	public String getMappingName() {
		return ValidatorMessages.getString("LookupValidationRule.MappingName"); //$NON-NLS-1$
	}
	
	@Override
	public String getTargetMappedItemName() {
		return ValidatorMessages.getString("LookupValidationRule.TargetMappedItemName"); //$NON-NLS-1$
	}

}
