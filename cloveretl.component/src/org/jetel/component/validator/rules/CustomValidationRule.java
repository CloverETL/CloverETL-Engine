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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.CTLRecordTransform;
import org.jetel.component.Validator;
import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.CustomRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.CTLCustomRuleUtils;
import org.jetel.component.validator.utils.CTLCustomRuleUtils.Function;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.MetadataErrorDetail;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLType.TLTypeByteArray;
import org.jetel.ctl.data.TLType.TLTypeList;
import org.jetel.ctl.data.TLTypePrimitive.TLTypeBoolean;
import org.jetel.ctl.data.TLTypePrimitive.TLTypeDateTime;
import org.jetel.ctl.data.TLTypePrimitive.TLTypeDecimal;
import org.jetel.ctl.data.TLTypePrimitive.TLTypeDouble;
import org.jetel.ctl.data.TLTypePrimitive.TLTypeLong;
import org.jetel.ctl.data.TLTypePrimitive.TLTypeString;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.MissingFieldException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLMapping;
import org.jetel.util.string.StringUtils;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.4.2013
 */
@XmlRootElement(name="custom")
@XmlType(propOrder={"ref"})
public class CustomValidationRule extends AbstractValidationRule {
	
	public static final int ERROR = 1101;
	public static final int ERROR_EXECUTION = 1102;
	
	@XmlElement(name="ref",required=true)
	private IntegerValidationParamNode ref = new IntegerValidationParamNode();
	
	private CTLMapping tempMapping;
	private DataRecord tempCustomRuleOutputRecord;
	private DataRecord tempCustomRuleInputRecord;

	@Override
	protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		// TODO Auto-generated method stub
		return new ArrayList<ValidationParamNode>();
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ORDERED_FIELDS;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		
		setPropertyRefResolver(graphWrapper);
		logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		logParentLangaugeSetting();
		
		String resolvedTarget = resolve(target.getValue());
		String parsedTargets[] = ValidatorUtils.parseTargets(resolvedTarget);
		
		Map<Integer, CustomRule> customRules = graphWrapper.getCustomRules();
		CustomRule selectedRule = customRules.get(ref.getValue());
		List<Function> functions = CTLCustomRuleUtils.findFunctions(graphWrapper.getTransformationGraph(), new DataRecordMetadata[]{record.getMetadata()}, new DataRecordMetadata[]{Validator.createCustomRuleOutputMetadata()}, selectedRule.getCode());
		String codeToExecute = getCustomValidationRuleTransformation(selectedRule.getCode(), functions.get(0), parsedTargets);
		
		initMapping(codeToExecute, record.getMetadata(), graphWrapper);
		
		tempCustomRuleOutputRecord.reset();
		tempCustomRuleInputRecord.reset();
		tempCustomRuleInputRecord.copyFrom(record);
		
		HashMap<String, String> values = new HashMap<String, String>();
		for(String field: parsedTargets) {
			values.put(field, record.getField(field).toString());
		}
		
		try {
			tempMapping.init("dummy");
			tempMapping.execute();
		} catch (Exception ex) {
			logError("Function '" + functions.get(0).getName() + "' could not be executed.");
			raiseError(ea, ERROR_EXECUTION, "Given function could not be executed.", graphWrapper.getNodePath(this), parsedTargets, values);
			return State.INVALID;
		}
		Boolean value = (Boolean) tempCustomRuleOutputRecord.getField(Validator.CUSTOM_RULE_RESULT).getValue(); 
		if(value != null && value) {
			logSuccess("Fields '" + resolvedTarget + "' passed function '" + functions.get(0).getName() + "'.");
			return State.VALID;
		} else {
			String message = new String();
			if(tempCustomRuleOutputRecord.getField(Validator.CUSTOM_RULE_MESSAGE).getValue() != null) {
				message = tempCustomRuleOutputRecord.getField(Validator.CUSTOM_RULE_MESSAGE).getValue().toString();
			}
			logError("Fields '" + resolvedTarget + "' didn't passed function '" + functions.get(0).getName() + "'.");
			raiseError(ea, ERROR, message , graphWrapper.getNodePath(this), parsedTargets, values);
			return State.INVALID;
		}
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
		String resolvedTarget = resolve(target.getValue());
		if(!resolvedTarget.isEmpty() && !ValidatorUtils.areValidFields(resolvedTarget, inputMetadata)) {
			accumulator.addError(target, this, "Some of target fields are not present in input metadata.");
			state = false;
			fieldsAreValid = false;
		}
		Integer customRuleId = ref.getValue();
		if(customRuleId == null) {
			accumulator.addError(target, this, "Invalid custom rule ID.");
			state = false;
		}
		Map<Integer, CustomRule> customRules = graphWrapper.getCustomRules();
		if(customRules == null) {
			accumulator.addError(target, this, "No custom validation rules.");
			return false; // Stop check here
		}
		CustomRule selectedRule = customRules.get(ref.getValue());
		if(selectedRule == null) {
			accumulator.addError(target, this, "Invalid custom rule. Delete this validation rule.");
			return false; // Stop check here
		}
		List<Function> functions;
		try { 
			functions = CTLCustomRuleUtils.findFunctions(graphWrapper.getTransformationGraph(), new DataRecordMetadata[]{inputMetadata}, new DataRecordMetadata[]{Validator.createCustomRuleOutputMetadata()}, selectedRule.getCode());
		} catch (JetelRuntimeException ex) {
			accumulator.addError(target, this, "Parsing of validation rule failed with message: " + ex.getMessage());
			return false; // Stop check here 
		}
		if(functions.size() == 0) {
			accumulator.addError(target, this, "No function found in custom validation rule.");
			return false; // Stop check here
		}
		if(functions.get(0).getName().equals("transform")) {
			accumulator.addError(target, this, "Invalid name of custom validaton rule. Name 'transform' is reserved.");
			return false; // Stop check here
		}

		String parsedTargets[] = ValidatorUtils.parseTargets(resolvedTarget);
		if(parsedTargets.length < functions.get(0).getParametersType().length) {
			accumulator.addError(target, this, "Not enough parameters for the custom validation rule.");
			return false; // Stop check here
		}
		if(parsedTargets.length > functions.get(0).getParametersType().length) {
			accumulator.addError(target, this, "Too much parameters for the custom validation rule.");
			return false; // Stop check here
		}
		if(fieldsAreValid) {
			String codeToExecute = getCustomValidationRuleTransformation(selectedRule.getCode(), functions.get(0), parsedTargets);
			state &= tryToCompile(codeToExecute, accumulator, inputMetadata, graphWrapper);
		}	
		return state;
	}
	
	/**
	 * Prepares transformation code for compilation/execution.
	 * Prepends function transform() which call function from custom validation rule provided by user.
	 * @param oldCode Function code provided from validation code
	 * @param function Function declaration (name and params)
	 * @param parsedTargets Target fields in order in which they will be passed to the function.
	 * @return New code which can be executed as transformation
	 */
	private String getCustomValidationRuleTransformation(String oldCode, Function function, String[] parsedTargets) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("//#CTL2\n\n");
		buffer.append("function integer transform() {\n");
		buffer.append("\t" + function.getName() + "(");
		for(int i = 0; i < parsedTargets.length; i++) {
			buffer.append("$in.0." + parsedTargets[i]);
			if(i + 1 < parsedTargets.length) {
				buffer.append(", ");
			}
		}
		buffer.append(");\n");
		buffer.append("\treturn OK;\n");
		buffer.append("}\n");
		
		String newCode = new String(oldCode);
		return newCode.replace("//#CTL2", buffer.toString());
	}
	
	private boolean tryToCompile(String sourceCode, ReadynessErrorAcumulator accumulator, DataRecordMetadata inputMetadata, GraphWrapper graphWrapper) {
		ITLCompiler compiler = TLCompilerFactory.createCompiler(graphWrapper.getTransformationGraph(), new DataRecordMetadata[]{inputMetadata}, new DataRecordMetadata[]{Validator.createCustomRuleOutputMetadata()}, "UTF-8");
		List<ErrorMessage> msgs = compiler.compile(sourceCode, CTLRecordTransform.class, "DUMMY");
		if (compiler.errorCount() > 0) {
    		for (ErrorMessage msg: msgs) {
    			accumulator.addError(target, this, msg.getErrorMessage());
    		}
    		return false;
    	}
		return true;
	}
	
	private void initMapping(String sourceCode, DataRecordMetadata inputMetadata, GraphWrapper graphWrapper) {
		if(tempMapping != null) {
			return;
		}
		// Dummy Node as there is not much need of it
		tempMapping = new CTLMapping("Custom validation rule", new Node("DUMMY", graphWrapper.getTransformationGraph()) {
			
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
		tempCustomRuleInputRecord = tempMapping.addInputMetadata("in", inputMetadata);
		tempCustomRuleOutputRecord = tempMapping.addOutputMetadata("out", Validator.createCustomRuleOutputMetadata());
	}
	
	@Override
	public String getCommonName() {
		return "Custom user rule";
	}

	@Override
	public String getCommonDescription() {
		return "Custom rule written in CTL. Validity of incoming record is determined by result of CTL code provider from user.";
	}
	
	public IntegerValidationParamNode getRef() {
		return ref;
	}

}
