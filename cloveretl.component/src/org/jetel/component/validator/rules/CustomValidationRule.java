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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.CTLRecordTransform;
import org.jetel.component.Validator;
import org.jetel.component.validator.CustomRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.CTLCustomRuleUtils;
import org.jetel.component.validator.utils.CTLCustomRuleUtils.Function;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.data.TLType;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLMapping;

/**
 * <p>Rule for executing CTL2 custom rule carried in {@link CustomRule} saved in the root of
 * validation tree (@see {@link ValidationGroup}.</p>
 * 
 * <p>User rules are CTL2 function with arguments. This arguments comes from target fields.</p>
 * 
 * <p>Checks whether the code is compilable and arguments types are OK.</p>
 * 
 * <p>Param node ref contains ID of {@link CustomRule} as it is stored in root {@link ValidationGroup}.
 * Not accesible by user.</p>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.4.2013
 * @see CustomRule
 * @see CTLMapping
 * @see Validator#createCustomRuleOutputMetadata()
 */
@XmlRootElement(name="custom")
@XmlType(propOrder={"ref"})
public class CustomValidationRule extends AbstractMappingValidationRule {
	
	public static final int ERROR = 1101;			/** Custom rule returned false thus the record is invalid */
	public static final int ERROR_EXECUTION = 1102;	/** Error executing rule (invalid code, some runtime problems...) */
	
	@XmlElement(name="ref",required=true)
	private IntegerValidationParamNode ref = new IntegerValidationParamNode();
	
	private CTLMapping tempMapping;
	private DataRecord tempCustomRuleOutputRecord;
	
	private String[] ruleParameters;
	private String functionName;
	private Map<String, String> mapping;
	private Function firstFunction;
	private String[] orderedParameterFields;

	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		initializeRuleDetails(inMetadata, graphWrapper);
		
		target.setPlaceholder(ValidatorMessages.getString("CustomValidationRule.TargetFieldsPlaceholder")); //$NON-NLS-1$
		mappingParam.setName(ValidatorMessages.getString("CustomValidationRule.MappingParameterName")); //$NON-NLS-1$
		mappingParam.setTooltip(ValidatorMessages.getString("CustomValidationRule.MappingParameterTooltip")); //$NON-NLS-1$
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(mappingParam);
	}

	private void initializeRuleDetails(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		CustomRule selectedRule = getSelectedRule(graphWrapper);
		Function firstFunction = getFirstFunction(selectedRule, inMetadata, graphWrapper);
		TLType[] parameterTypes = firstFunction.getParametersType();
		String[] parameterNames = firstFunction.getParameterNames();
		functionName = firstFunction.getName();
		
		ruleParameters = new String[parameterTypes.length];
		for (int i = 0; i < parameterTypes.length; i++) {
			ruleParameters[i] = parameterNames[i];
		}
	}
	
	@Override
	public String[] getMappingTargetFields() {
		return ruleParameters;
	}
	
	@Override
	public void init(DataRecord record, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(record, graphWrapper);
		
		try {
			initializeParametersMapping();
		} catch (ParseException e) {
			throw new ComponentNotReadyException(e);
		}
		
		CustomRule selectedRule = getSelectedRule(graphWrapper);
		firstFunction = getFirstFunction(selectedRule, record.getMetadata(), graphWrapper);
		orderedParameterFields = getOrderedParameterFields(firstFunction.getParameterNames());
		String codeToExecute = getCustomValidationRuleTransformation(selectedRule.getCode(), firstFunction, orderedParameterFields);
		
		initMapping(codeToExecute, record, graphWrapper);
		
		tempMapping.init("dummy"); //$NON-NLS-1$
	}
	
	private void initializeParametersMapping() throws ParseException {
		mapping = ValidatorUtils.parseMappingToMap(resolve(mappingParam.getValue()));
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated(ValidatorMessages.getString("CustomValidationRule.LogNotEnabled")); //$NON-NLS-1$
			return State.NOT_VALIDATED;
		}
		if (!isInitialized()) {
			throw new IllegalStateException(ValidatorMessages.getString("CustomValidationRule.NotInitializedError")); //$NON-NLS-1$
		}
		
		// tempCustomRuleOutputRecord.reset();
		if (tempMapping.getInputRecord(0) != record) {
			throw new IllegalStateException(ValidatorMessages.getString("CustomValidationRule.RecordReferenceError")); //$NON-NLS-1$
		}
		
		try {
			tempMapping.execute();
		} catch (Exception ex) {
			if (isLoggingEnabled()) {
				logError(MessageFormat.format(ValidatorMessages.getString("CustomValidationRule.CouldNotExecuteFunctionLogMessage"), firstFunction.getName())); //$NON-NLS-1$
			}
			if (ea != null) {
				HashMap<String, String> values = getAnalyzedValuesSnapshot(record, orderedParameterFields);
				raiseError(ea, ERROR_EXECUTION, ValidatorMessages.getString("CustomValidationRule.CannotExecuteFunctionError"), orderedParameterFields, values); //$NON-NLS-1$
			}
			return State.INVALID;
		}
		Boolean value = (Boolean) tempCustomRuleOutputRecord.getField(Validator.CUSTOM_RULE_RESULT).getValue(); 
		if(value != null && value) {
			if (isLoggingEnabled()) {
				logSuccess(MessageFormat.format(ValidatorMessages.getString("CustomValidationRule.SuccessLogMessage"), Arrays.toString(orderedParameterFields), firstFunction.getName())); //$NON-NLS-1$
			}
			return State.VALID;
		} else {
			if (isLoggingEnabled()) {
				logError(MessageFormat.format(ValidatorMessages.getString("CustomValidationRule.InvalidLogMessage"), Arrays.toString(orderedParameterFields), firstFunction.getName())); //$NON-NLS-1$
			}
			if (ea != null) {
				String message = new String();
				if(tempCustomRuleOutputRecord.getField(Validator.CUSTOM_RULE_MESSAGE).getValue() != null) {
					message = tempCustomRuleOutputRecord.getField(Validator.CUSTOM_RULE_MESSAGE).getValue().toString();
				}
				HashMap<String, String> values = getAnalyzedValuesSnapshot(record, orderedParameterFields);
				raiseError(ea, ERROR, message , orderedParameterFields, values);
			}
			return State.INVALID;
		}
	}

	private HashMap<String, String> getAnalyzedValuesSnapshot(DataRecord record, String[] orderedParameterFields) {
		HashMap<String, String> values = new HashMap<String, String>();
		for(String field: orderedParameterFields) {
			values.put(field, record.getField(field).toString());
		}
		return values;
	}

	private Function getFirstFunction(CustomRule selectedRule, DataRecordMetadata metadata, GraphWrapper graphWrapper) {
		List<Function> functions = CTLCustomRuleUtils.findFunctions(
				graphWrapper.getTransformationGraph(),
				new DataRecordMetadata[] { metadata },
				new DataRecordMetadata[] { Validator.createCustomRuleOutputMetadata() },
				selectedRule.getCode()
			);
		Function firstFunction = functions.get(0);
		return firstFunction;
	}

	private CustomRule getSelectedRule(GraphWrapper graphWrapper) {
		Map<Integer, CustomRule> customRules = graphWrapper.getCustomRules();
		CustomRule selectedRule = customRules.get(ref.getValue());
		return selectedRule;
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator,
			GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		setPropertyRefResolver(graphWrapper);
		boolean state = true;
		boolean fieldsAreValid = true;
		try {
			initializeParametersMapping();
		} catch (ParseException e) {
			accumulator.addError(mappingParam, this, ValidatorMessages.getString("CustomValidationRule.CannotParseMappingError") + e.getMessage()); //$NON-NLS-1$
			return false;
		}
		Collection<String> mappingInputFields = mapping.values();
		if(!ValidatorUtils.areValidFields(mappingInputFields, inputMetadata)) {
			accumulator.addError(mappingParam, this, ValidatorMessages.getString("CustomValidationRule.MappingSourceFieldsNotInMetadata")); //$NON-NLS-1$
			state = false;
			fieldsAreValid = false;
		}
		Integer customRuleId = ref.getValue();
		if(customRuleId == null) {
			accumulator.addError(ref, this, ValidatorMessages.getString("CustomValidationRule.InvalidCustomRuleId")); //$NON-NLS-1$
			state = false;
		}
		Map<Integer, CustomRule> customRules = graphWrapper.getCustomRules();
		if(customRules == null) {
			accumulator.addError(ref, this, ValidatorMessages.getString("CustomValidationRule.NoCustomRulesError")); //$NON-NLS-1$
			return false; // Stop check here
		}
		CustomRule selectedRule = customRules.get(ref.getValue());
		if(selectedRule == null) {
			accumulator.addError(ref, this, ValidatorMessages.getString("CustomValidationRule.InvalidCustomRuleError")); //$NON-NLS-1$
			return false; // Stop check here
		}
		List<Function> functions;
		try { 
			functions = CTLCustomRuleUtils.findFunctions(
					graphWrapper.getTransformationGraph(),
					new DataRecordMetadata[] { inputMetadata },
					new DataRecordMetadata[] { Validator.createCustomRuleOutputMetadata() },
					selectedRule.getCode()
				);
		} catch (JetelRuntimeException ex) {
			accumulator.addError(ref, this, ValidatorMessages.getString("CustomValidationRule.ValidationRuleParsingError") + ex.getMessage()); //$NON-NLS-1$
			return false; // Stop check here 
		}
		if(functions.size() == 0) {
			accumulator.addError(ref, this, ValidatorMessages.getString("CustomValidationRule.NoFunctionFoundError")); //$NON-NLS-1$
			return false; // Stop check here
		}
		Function firstFunction = functions.get(0);
		if(firstFunction.getName().equals("transform")) { //$NON-NLS-1$
			accumulator.addError(ref, this, ValidatorMessages.getString("CustomValidationRule.TransformReservedWordError")); //$NON-NLS-1$
			return false; // Stop check here
		}
		Set<String> parameterNames = new HashSet<String>();
		String[] parameterNamesArray = firstFunction.getParameterNames();
		for (String param : parameterNamesArray) {
			parameterNames.add(param);
		}
		Set<String> mappingTargetFields = mapping.keySet();
		if (!parameterNames.containsAll(mappingTargetFields)) {
			accumulator.addError(mappingParam, this, ValidatorMessages.getString("CustomValidationRule.UnknownMappingTargetFields")); //$NON-NLS-1$
			return false;
		}
		if (!mappingTargetFields.containsAll(parameterNames)) {
			accumulator.addError(mappingParam, this, ValidatorMessages.getString("CustomValidationRule.MappingUnmappedFields")); //$NON-NLS-1$
			return false;
		}
		String[] orderedParameterFields = getOrderedParameterFields(parameterNamesArray);
		if(fieldsAreValid) {
			String codeToExecute = getCustomValidationRuleTransformation(selectedRule.getCode(), firstFunction, orderedParameterFields);
			state &= tryToCompile(codeToExecute, accumulator, inputMetadata, graphWrapper);
		}
		return state;
	}
	
	private String[] getOrderedParameterFields(String[] parameterNamesArray) {
		String[] retval = new String[parameterNamesArray.length];
		
		for (int i = 0; i < retval.length; i++) {
			retval[i] = mapping.get(parameterNamesArray[i]);
		}
		
		return retval;
	}

	/**
	 * Prepares transformation code for compilation/execution.
	 * Prepends function transform() which call function from custom validation rule provided by user.
	 * 
	 * @param oldCode Function code provided from validation code
	 * @param function Function declaration (name and params)
	 * @param parsedTargets Target fields in order in which they will be passed to the function.
	 * @return New code which can be executed as transformation
	 */
	private String getCustomValidationRuleTransformation(String oldCode, Function function, String[] parsedTargets) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("//#CTL2\n\n"); //$NON-NLS-1$
		buffer.append("function integer transform() {\n"); //$NON-NLS-1$
		buffer.append("\t" + function.getName() + "("); //$NON-NLS-1$ //$NON-NLS-2$
		for(int i = 0; i < parsedTargets.length; i++) {
			buffer.append("$in.0." + parsedTargets[i]); //$NON-NLS-1$
			if(i + 1 < parsedTargets.length) {
				buffer.append(", "); //$NON-NLS-1$
			}
		}
		buffer.append(");\n"); //$NON-NLS-1$
		buffer.append("\treturn OK;\n"); //$NON-NLS-1$
		buffer.append("}\n"); //$NON-NLS-1$
		
		String newCode = new String(oldCode);
		return newCode.replace("//#CTL2", buffer.toString()); //$NON-NLS-1$
	}
	
	/**
	 * Tries to compile given CTL2 source code and return all errors into given accumulator.
	 * @param sourceCode CTL2 source code to compile
	 * @param accumulator Accumulator to store all errors in
	 * @param inputMetadata Input metadata
	 * @param graphWrapper GraphWrapper (to access graph)
	 * @return
	 */
	private boolean tryToCompile(String sourceCode, ReadynessErrorAcumulator accumulator, DataRecordMetadata inputMetadata, GraphWrapper graphWrapper) {
		ITLCompiler compiler = TLCompilerFactory.createCompiler(graphWrapper.getTransformationGraph(), new DataRecordMetadata[]{inputMetadata}, new DataRecordMetadata[]{Validator.createCustomRuleOutputMetadata()}, "UTF-8"); //$NON-NLS-1$
		List<ErrorMessage> msgs = compiler.compile(sourceCode, CTLRecordTransform.class, "DUMMY"); //$NON-NLS-1$
		if (compiler.errorCount() > 0) {
    		for (ErrorMessage msg: msgs) {
    			accumulator.addError(target, this, msg.getErrorMessage());
    		}
    		return false;
    	}
		return true;
	}
	
	private void initMapping(String sourceCode, DataRecord record, GraphWrapper graphWrapper) {
		// Dummy Node as there is not much need of it
		tempMapping = new CTLMapping(ValidatorMessages.getString("CustomValidationRule.CTLMappingInternalName"), new Node("DUMMY", graphWrapper.getTransformationGraph()) { //$NON-NLS-1$ //$NON-NLS-2$
			
			@Override
			public String getType() {
				return null;
			}
			
			@Override
			protected Result execute() throws Exception {
				return null;
			}
		});
		tempMapping.setTransformation(sourceCode);
		tempMapping.addInputRecord("in", record); //$NON-NLS-1$
		tempMapping.setOutputSetDefaults(false);
		tempCustomRuleOutputRecord = tempMapping.addOutputMetadata("out", Validator.createCustomRuleOutputMetadata()); //$NON-NLS-1$
	}
	
	@Override
	public String getCommonName() {
		return ValidatorMessages.getString("CustomValidationRule.CommonName"); //$NON-NLS-1$
	}

	@Override
	public String getCommonDescription() {
		return ValidatorMessages.getString("CustomValidationRule.CommonDescription"); //$NON-NLS-1$
	}
	
	public IntegerValidationParamNode getRef() {
		return ref;
	}
	
	@Override
	public String getDetailName() {
		return String.format(ValidatorMessages.getString("CustomValidationRule.DetailName"), getName(), functionName); //$NON-NLS-1$
	}
	
	@Override
	public String getMappingName() {
		return ValidatorMessages.getString("CustomValidationRule.MappingName"); //$NON-NLS-1$
	}
	
	@Override
	public String getTargetMappedItemName() {
		return ValidatorMessages.getString("CustomValidationRule.TargetMappedItemName"); //$NON-NLS-1$
	}

}
