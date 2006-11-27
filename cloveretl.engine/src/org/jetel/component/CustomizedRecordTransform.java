/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.component;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DecimalDataField;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;
import org.jetel.util.WcardPattern;

/**
 * 
 * Class used for generating data transformation. It has methods for mapping input
 *  fields on output fields, assigning constants, sequence methods and parameter's 
 *  values to output fields.
 *  
 *  <h4>Patterns for data fields can be given in three ways:</h4>
 *  <ol>
 *  <li> <i>record.field</i> where <i>record</i> is number, name or wild card of input or output
 *  	record and <i>field</i> is name, number or wild card of <i>record's</i> 
 *  	data field </li>
 *  <li> <i>${record.field}</i> where <i>record</i> and <i>field</i> have to be as described above</li>
 *  <li> <i>${in/out.record.field}</i> where <i>record</i> and <i>field</i> have to be as described above</li>
 *  </ol>
 * 
 * <h4>Order of execution/methods call</h4>
 * <ol>
 * <li>setGraph()</li>
 * <li>setFieldPolicy(PolicyType)</li>
 * <li>add...Rule()<br>
 * .<br>
 * .<br></li>
 * <li>init()</li>
 * <li>transform() <i>for each input&amp;output records pair</i></li>
 * <li><i>optionally</i> getMessage() <i>or</i> signal() <i>or</i> getSemiResult()</li>
 * <li>finished()
 * </ol>
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Nov 16, 2006
 * @see         org.jetel.component.RecordTransform
 * @see			org.jetel.component.DataRecordTransform		
 */
public class CustomizedRecordTransform implements RecordTransform {
	
	protected Properties parameters;
	protected DataRecordMetadata[] sourceMetadata;
	protected DataRecordMetadata[] targetMetadata;

	protected TransformationGraph graph;
	
	protected PolicyType fieldPolicy = PolicyType.LENIENT;
	protected Log logger;
	protected String errorMessage;
	
	/**
	 * Map "rules" stores rules given by user in following form:
	 * key: patternOut
	 * value: ruleType:ruleString, where ruleType is one of: Rule.FIELD, Rule.CONSTANT,
	 * 	Rule.SEQUENCE, Rule.PARAMETER, and ruleString can be patternIn, constant, sequence ID
	 * 	(optionally with method) or parameter name 
	 */
	protected Map<String, String> rules = new LinkedHashMap<String, String>();
	protected Rule[][] transformMapArray;//rules from "rules" map translated for concrete metadata
	protected int[][] order;//order for assigning output fields (importent if assigning sequence values)
	
	protected static final int REC_NO = 0;
	protected static final int FIELD_NO = 1;
	
	protected static final char DOT = '.';
	protected static final char COLON =':';
	protected static final char PARAMETER_CHAR = '$'; 
	
	private	 int ruleType;
	private String ruleString;
	private String sequenceID;


	/**
	 * @param logger
	 */
	public CustomizedRecordTransform(Log logger) {
		this.logger = logger;
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param patternIn input field's pattern
	 */
	public void addFieldToFieldRule(String patternOut, String patternIn) {
		rules.put(patternOut, String.valueOf(Rule.FIELD) + COLON + patternIn);
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param recNo output record number
	 * @param fieldNo output record's field number
	 * @param patternIn input field's pattern
	 */
	public void addFieldToFieldRule(int recNo, int fieldNo, String patternIn){
		addFieldToFieldRule(String.valueOf(recNo) + DOT + fieldNo, patternIn);
	}
	
	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param recNo output record number
	 * @param field output record's field name
	 * @param patternIn input field's pattern
	 */
	public void addFieldToFieldRule(int recNo, String field, String patternIn){
		addFieldToFieldRule(String.valueOf(recNo) + DOT + field, patternIn);
	}
	
	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param fieldNo output record's field number
	 * @param patternIn input field's pattern
	 */
	public void addFieldToFieldRule(int fieldNo, String patternIn){
		addFieldToFieldRule(0, fieldNo, patternIn);
	}
	
	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param recNo input record's number
	 * @param fieldNo input record's field number
	 */
	public void addFieldToFieldRule(String patternOut, int recNo, int fieldNo){
		addFieldToFieldRule(patternOut, String.valueOf(recNo) + DOT + fieldNo);
	}
	
	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param recNo input record's number
	 * @param field input record's field name
	 */
	public void addFieldToFieldRule(String patternOut, int recNo, String field){
		addFieldToFieldRule(patternOut, String.valueOf(recNo) + DOT + field);
	}
	
	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param outRecNo output record's number
	 * @param outFieldNo output record's field number
	 * @param inRecNo input record's number
	 * @param inFieldNo input record's field number
	 */
	public void addFieldToFieldRule(int outRecNo, int outFieldNo, int inRecNo, int inFieldNo){
		addFieldToFieldRule(String.valueOf(outRecNo) + DOT + outFieldNo,
				String.valueOf(inRecNo) + DOT + inFieldNo);
	}
	
	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param value value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(String patternOut, String value){
		rules.put(patternOut, String.valueOf(Rule.CONSTANT) + COLON + value);
	}
	
	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param value value to assign
	 */
	public void addConstantToFieldRule(String patternOut, int value){
		rules.put(patternOut, String.valueOf(Rule.CONSTANT) + COLON + value);
	}
	
	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param value value to assign
	 */
	public void addConstantToFieldRule(String patternOut, double value){
		rules.put(patternOut,String.valueOf(Rule.CONSTANT) + COLON + value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param value value to assign
	 */
	public void addConstantToFieldRule(String patternOut, Date value){
		rules.put(patternOut, String.valueOf(Rule.CONSTANT) + COLON + 
					SimpleDateFormat.getDateInstance().format(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param value value to assign
	 */
	public void addConstantToFieldRule(String patternOut, Numeric value){
		rules.put(patternOut, String.valueOf(Rule.CONSTANT) + COLON + value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param value value value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, String value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, int value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, double value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}
	
	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, Date value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, Numeric value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field name
	 * @param value value value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(int recNo, String field, String value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field name
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, String field, int value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field name
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, String field, double value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field name
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, String field, Date value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field name
	 * @param value value value to assign 
	 */
	public void addConstantToFieldRule(int recNo, String field, Numeric value){
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo output record's field number
	 * @param value value value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(int fieldNo, String value){
		addConstantToFieldRule(0, fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo output record's field number
	 * @param value value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, int value){
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo output record's field number
	 * @param value value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, double value){
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo output record's field number
	 * @param value value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, Date value){
		addConstantToFieldRule(0, fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo output record's field number
	 * @param value value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, Numeric value){
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields 
	 * 
	 * @param patternOut output fields' pattern
	 * @param sequence sequence ID, optionally with sequence method (can be in form
	 * 	${seq.seqID}, eg. "MySequence" is the same as "MySequence.nextIntValue()"
	 *  or "${seq.MySequence.nextIntValue()}" 
	 */
	public void addSequenceToFieldRule(String patternOut, String sequence){
		String sequenceString = sequence.startsWith("${") ? 
				sequence.substring(sequence.indexOf(DOT)+1, sequence.length() -1) 
				: sequence;
		rules.put(patternOut, String.valueOf(Rule.SEQUENCE) + COLON + sequenceString);
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 *  
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param sequence sequence ID, optionally with sequence method (can be in form
	 * 	${seq.seqID}, eg. "MySequence" is the same as "MySequence.nextIntValue()"
	 *  or "${seq.MySequence.nextIntValue()}" 
	 */
	public void addSequenceToFieldRule(int recNo, int fieldNo, String sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + fieldNo, sequence);
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param recNo output record's number
	 * @param field output record's field name
	 * @param sequence sequence ID, optionally with sequence method (can be in form
	 * 	${seq.seqID}, eg. "MySequence" is the same as "MySequence.nextIntValue()"
	 *  or "${seq.MySequence.nextIntValue()}" 
	 */
	public void addSequenceToFieldRule(int recNo, String field, String sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + field, sequence);
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields in 0th record
	 * 
	 * @param fieldNo output record's field number
	 * @param sequence sequence ID, optionally with sequence method (can be in form
	 * 	${seq.seqID}, eg. "MySequence" is the same as "MySequence.nextIntValue()"
	 *  or "${seq.MySequence.nextIntValue()}" 
	 */
	public void addSequenceToFieldRule(int fieldNo, String sequence){
		addSequenceToFieldRule(0,fieldNo, sequence);
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param patternOut output fields' pattern
	 * @param sequence sequence for getting value
	 */
	public void addSequenceToFieldRule(String patternOut, Sequence sequence){
		addSequenceToFieldRule(patternOut, sequence.getId());
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param sequence sequence for getting value
	 */
	public void addSequenceToFieldRule(int recNo, int fieldNo, Sequence sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + fieldNo, sequence.getId());
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param recNo output record's number
	 * @param field output record's field name
	 * @param sequence sequence for getting value
	 */
	public void addSequenceToFieldRule(int recNo, String field, Sequence sequence){
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + field, sequence.getId());
	}
	
	/**
	 * Mathod for adding rule: assigning value from sequence to output fields in 0th output record
	 * 
	 * @param fieldNo output record's field number
	 * @param sequence sequence for getting value
	 */
	public void addSequenceToFieldRule(int fieldNo, Sequence sequence){
		addSequenceToFieldRule(0,fieldNo, sequence.getId());
	}

	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param patternOut output fields' pattern
	 * @param parameterName
	 */
	public void addParameterToFieldRule(String patternOut, String parameterName){
		rules.put(patternOut, String.valueOf(Rule.PARAMETER) + COLON + parameterName);
	}

	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param parameterName
	 */
	public void addParameterToFieldRule(int recNo, int fieldNo, String parameterName){
		addParameterToFieldRule(String.valueOf(recNo) + DOT + fieldNo, parameterName);
	}
	
	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param recNo output record's number
	 * @param field output record's field name
	 * @param parameterName
	 */
	public void addParameterToFieldRule(int recNo, String field, String parameterName){
		addParameterToFieldRule(String.valueOf(recNo) + DOT + field, parameterName);
	}
	
	/**
	 * Mathod for adding rule: assigning parameter value to output fields in 0th output record
	 * 
	 * @param fieldNo output record's field number
	 * @param parameterName
	 */
	public void addParameterToFieldRule(int fieldNo, String parameterName){
		addParameterToFieldRule(0,fieldNo, parameterName);
	}
	
	public void finished() {
		// TODO Auto-generated method stub

	}

	public TransformationGraph getGraph() {
		return graph;
	}

	public String getMessage() {
		return errorMessage;
	}

	public Object getSemiResult() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#init(java.util.Properties, org.jetel.metadata.DataRecordMetadata[], org.jetel.metadata.DataRecordMetadata[])
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourcesMetadata,
			DataRecordMetadata[] targetMetadata) throws ComponentNotReadyException {
		if (sourcesMetadata == null || targetMetadata == null)
			return false;
		this.parameters=parameters;
		this.sourceMetadata=sourcesMetadata;
		this.targetMetadata=targetMetadata;
	    return init();
	}

	/**
	 * Method with initialize user customized transformation with concrete metadata 
	 * 
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private boolean init() throws ComponentNotReadyException{
		//map storing transformation for concrete output fields
		//key is in form: recNumber.fieldNumber 
		Map<String, Rule> transformMap = new LinkedHashMap<String, Rule>();
		Entry<String, String> rulesEntry;
		Rule rule;
		int type;
		String field;
		String ruleString;
		String[] outFields = new String[0];
		String[] inFields;
		//iteration over each user given rule
		for (Iterator<Entry<String, String>> iterator = rules.entrySet().iterator();iterator.hasNext();){
			rulesEntry = iterator.next();
			field = resolveField(rulesEntry.getKey());
			if (field == null){
				errorMessage = "Wrong pattern for output fields: " + rulesEntry.getKey();
				logger.error(errorMessage);
				throw new ComponentNotReadyException(errorMessage);
			}
			//find output fields from pattern
			outFields = findFields(field, targetMetadata).toArray(new String[0]);
			inFields = new String[0];
			//find type: Rule.FIELD, Rule.CONSTANT,	Rule.SEQUENCE, Rule.PARAMETER
			type = Integer.parseInt(rulesEntry.getValue().substring(0, rulesEntry.getValue().indexOf(COLON)));
			//find rule: patternIn, constant, sequence ID (optionally with method) or parameter name
			ruleString = rulesEntry.getValue().substring(rulesEntry.getValue().indexOf(COLON)+1);
			if (type == Rule.FIELD) {
				//find input fields from pattern
				ruleString = resolveField(ruleString);
				if (ruleString == null){
					errorMessage = "Wrong pattern for output fields: " + ruleString;
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
				}
				inFields = findFields(ruleString, sourceMetadata).toArray(new String[0]);
				if (inFields.length == 0){
					errorMessage = "There is no input field matching \""
						+ ruleString + "\" pattern";
					logger.warn(errorMessage);
					continue;
					
				}
			}
			if (type == Rule.FIELD && inFields.length > 1){
				//find mapping by names
				putMappingByNames(transformMap,outFields,inFields);
			}else{
				//for each output field from pattern put rule to map
				for (int i=0;i<outFields.length;i++){
					field = outFields[i];
					//check if there is just any rule for given output field
					rule = transformMap.remove(getRecNo(field) + DOT + getFieldNo(field));
					if (type == Rule.FIELD) {
						ruleString = inFields[0];
					}
					rule = validateRule(getRecNo(field),getFieldNo(field),type,ruleString);
					if (rule != null) {
						transformMap.put(outFields[i], rule);
					}						
				}
			}
		}
		//changing map to array
		transformMapArray = new Rule[targetMetadata.length][maxNumFields(targetMetadata)];
		order = new int[transformMap.size()][2];
		int index = 0;
		for (Entry<String, Rule> i : transformMap.entrySet()) {
			field = i.getKey();
			order[index][REC_NO] = getRecNo(field);
			order[index][FIELD_NO] = getFieldNo(field);
			transformMapArray[order[index][REC_NO] ][order[index][FIELD_NO]] = i.getValue();
			index++;
		}
		return true;
	}
	
	protected Rule validateRule(int recNo, int fieldNo, int ruleType,String ruleString) throws ComponentNotReadyException {
		char fieldType = targetMetadata[recNo].getFieldType(fieldNo);
		switch (ruleType) {
		case Rule.PARAMETER:
			String parameterValue;
			if (ruleString.startsWith("${")){//get graph parameter
				parameterValue = getGraph().getGraphProperties().getProperty(
						ruleString.substring(2, ruleString.lastIndexOf('}')));
			}else if (ruleString.startsWith(String.valueOf(PARAMETER_CHAR))){
				//get parameter from node properties
				parameterValue = parameters.getProperty((ruleString));
			}else{
				//try to find parameter with given name in node properties
				parameterValue = parameters.getProperty(PARAMETER_CHAR + ruleString);
				if (parameterValue == null ){
					//try to find parameter with given name among graph parameters 
					parameterValue = getGraph().getGraphProperties().getProperty(ruleString);
				}
				if (parameterValue == null){
					errorMessage = "Not found parameter: " + ruleString;
					logger.warn(errorMessage);
					if (!(targetMetadata[recNo].getField(fieldNo).isNullable() || 
							targetMetadata[recNo].getField(fieldNo).isDefaultValue())){
						return null;
					}
				}
			}
			if ((fieldType != DataFieldMetadata.BYTE_FIELD || 
					fieldType != DataFieldMetadata.STRING_FIELD ) &&
					parameterValue != null) {
				checkConstant(recNo, fieldNo, parameterValue);
			}
			return new Rule(Rule.CONSTANT,parameterValue);
		case Rule.CONSTANT:
			if (ruleString.equals("null") && !(targetMetadata[recNo].getField(fieldNo).isNullable() || 
							targetMetadata[recNo].getField(fieldNo).isDefaultValue())){
				errorMessage = "Null value not allowed to record: " + targetMetadata[recNo].getName() 
				+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
				logger.warn(errorMessage);
				return null;
			}
			if ((fieldType != DataFieldMetadata.BYTE_FIELD || 
					fieldType != DataFieldMetadata.STRING_FIELD ) &&
					ruleString != null) {
				checkConstant(recNo, fieldNo, ruleString);
			}
			break;
		case Rule.SEQUENCE:
			sequenceID = ruleString.indexOf(DOT) == -1 ? ruleString
					: ruleString.substring(0, ruleString.indexOf(DOT));
			Sequence sequence = getGraph().getSequence(sequenceID );
			if (sequence == null){
				logger.warn("There is no sequence \"" + sequenceID + "\" in graph");
				if (!(targetMetadata[recNo].getField(fieldNo).isNullable() || 
							targetMetadata[recNo].getField(fieldNo).isDefaultValue())){
					errorMessage = "Null value not allowed to record: " + targetMetadata[recNo].getName() 
					+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
					logger.warn(errorMessage);
					return null;
				}
			}
			String method = ruleString.indexOf(DOT) > -1 ? 
					ruleString.substring(ruleString.indexOf(DOT) +1) : "nextValueInt()";
			char methodType;
			if (method.toLowerCase().contains("int")) {
				methodType = DataFieldMetadata.INTEGER_FIELD;
			}else if (method.toLowerCase().contains("long")){
				methodType = DataFieldMetadata.LONG_FIELD;
			}else {
				methodType = DataFieldMetadata.STRING_FIELD;
			}
			if (!checkTypes(fieldPolicy, fieldType, null, methodType, null)){
				if (fieldPolicy == PolicyType.STRICT) {
					errorMessage = "Sequence method:" + ruleString + " does not " +
							"match field type:\n"+ targetMetadata[recNo].getName() + 
							DOT + targetMetadata[recNo].getField(fieldNo).getName() + 
							" type - " + targetMetadata[recNo].getField(fieldNo).getTypeAsString() + 
							getDecimalParams(targetMetadata[recNo].getField(fieldNo));
					logger.warn(errorMessage);
					return null;
				}
				if (fieldPolicy == PolicyType.CONTROLLED){
					errorMessage = "Sequence method:" + ruleString + " does not " +
					"match field type:\n"+ targetMetadata[recNo].getName() + 
					DOT + targetMetadata[recNo].getField(fieldNo).getName() + 
					" type - " + targetMetadata[recNo].getField(fieldNo).getTypeAsString() + 
					getDecimalParams(targetMetadata[recNo].getField(fieldNo));
					logger.warn(errorMessage);
					return null;
				}
			}
			break;
		case Rule.FIELD:
			if (!checkTypes(fieldPolicy, recNo, fieldNo, getRecNo(ruleString), getFieldNo(ruleString))){
				if (fieldPolicy == PolicyType.STRICT) {
					errorMessage = "Output field type does not match input field " +
							"type:\n" +targetMetadata[recNo].getName() + DOT + 
							targetMetadata[recNo].getField(fieldNo).getName() + 
							" type - " + targetMetadata[recNo].getField(fieldNo).getTypeAsString() + 
							getDecimalParams(targetMetadata[recNo].getField(fieldNo)) + "\n" +
							sourceMetadata[getRecNo(ruleString)].getName() + DOT +
							sourceMetadata[getRecNo(ruleString)].getField(getFieldNo(ruleString)).getName() +
							" type - " + sourceMetadata[getRecNo(ruleString)].getField(getFieldNo(ruleString)).getTypeAsString() +
							getDecimalParams(sourceMetadata[getRecNo(ruleString)].getField(getFieldNo(ruleString)));
					logger.warn(errorMessage);
					return null;
				}
				if (fieldPolicy == PolicyType.CONTROLLED){
					errorMessage = "Output field type is not compatible with input field " +
					"type:\n" +targetMetadata[recNo].getName() + DOT + 
					targetMetadata[recNo].getField(fieldNo).getName() + 
					" type - " + targetMetadata[recNo].getField(fieldNo).getTypeAsString() + 
					getDecimalParams(targetMetadata[recNo].getField(fieldNo)) + "\n" +
					sourceMetadata[getRecNo(ruleString)].getName() + DOT +
					sourceMetadata[getRecNo(ruleString)].getField(getFieldNo(ruleString)).getName() +
					" type - " + sourceMetadata[getRecNo(ruleString)].getField(getFieldNo(ruleString)).getTypeAsString() +
					getDecimalParams(sourceMetadata[getRecNo(ruleString)].getField(getFieldNo(ruleString)));
					logger.warn(errorMessage);
					return null;
				}
			}
		}
		return new Rule(ruleType,ruleString);
	}
	
	private boolean checkTypes(PolicyType policy, char outType, int[] outTypeDecimalParams,
			char inType, int[] inTypeDEcimalParams){
		boolean checkTypes;
		if (outType == inType){
			if (outType == DataFieldMetadata.DECIMAL_FIELD ){
				checkTypes = inTypeDEcimalParams[0] <= outTypeDecimalParams[0] && 
				inTypeDEcimalParams[1] <= outTypeDecimalParams[1];
			}else{
				checkTypes = true;
			}
		}else {
			checkTypes = false;
		}
		if (fieldPolicy == PolicyType.STRICT && !checkTypes){
			return false;
		}else if (fieldPolicy == PolicyType.CONTROLLED && 
				!DataFieldMetadata.isSubtype(inType, inTypeDEcimalParams, outType, outTypeDecimalParams)){
			return false;
		}
		return true;
	}
	
	private boolean checkTypes(PolicyType policyType, int outRecNo, int outFieldNo, 
			int inRecNo, int inFieldNo){
		DataFieldMetadata outField = targetMetadata[outRecNo].getField(outFieldNo);
		DataFieldMetadata inField = sourceMetadata[inRecNo].getField(inFieldNo);
		boolean checkTypes;
		//check if both fields are of type DECIMAL, if yes inField must be subtype of outField
		if (outField.getType() == inField.getType()){
			if (outField.getType() == DataFieldMetadata.DECIMAL_FIELD ){
				checkTypes = inField.isSubtype(outField);
			}else{
				checkTypes = true;
			}
		}else {
			checkTypes = false;
		}
		if (fieldPolicy == PolicyType.STRICT && !checkTypes){
//			logger.warn("Found fields with the same names but other types: ");
//			logger.warn(targetMetadata[outRecNo].getName() + DOT + 
//					outField.getName() + " type - " + outField.getTypeAsString() + getDecimalParams(outField));
//			logger.warn(sourceMetadata[inRecNo].getName() + DOT + 
//					inField.getName() + " type - " + inField.getTypeAsString() + getDecimalParams(inField));
			return false;
		}else if (fieldPolicy == PolicyType.CONTROLLED && 
				!inField.isSubtype(outField)){
//			logger.warn("Found fields with the same names but incompatible types: ");
//			logger.warn(targetMetadata[outRecNo].getName() + DOT + 
//					outField.getName() + " type - " + outField.getTypeAsString() + getDecimalParams(outField));
//			logger.warn(sourceMetadata[inRecNo].getName() + DOT + 
//					inField.getName() + " type - " + inField.getTypeAsString() + getDecimalParams(inField));
			return false;
		}
		return true;
	}
	
	private boolean checkConstant(int recNo, int fieldNo, String constant) throws ComponentNotReadyException{
		char type = targetMetadata[recNo].getFieldType(fieldNo);
		Object value;
		Format format = null; 
        String formatString = targetMetadata[recNo].getField(fieldNo).getFormatStr();;
        Locale locale;
        // handle locale
        if (targetMetadata[recNo].getField(fieldNo).getLocaleStr() != null) {
            String[] localeLC = targetMetadata[recNo].getField(fieldNo).getLocaleStr()
            			.split(Defaults.DEFAULT_LOCALE_STR_DELIMITER_REGEX);
            if (localeLC.length > 1) {
                locale = new Locale(localeLC[0], localeLC[1]);
            } else {
                locale = new Locale(localeLC[0]);
            }
        } else {
            locale = null;
        }
		switch (type) {
		case DataFieldMetadata.DATE_FIELD:
		case DataFieldMetadata.DATETIME_FIELD:
            if ((formatString != null) && (formatString.length() != 0)) {
                if (locale != null) {
                    format = new SimpleDateFormat(formatString, locale);
                } else {
                    format = new SimpleDateFormat(formatString);
                }
                ((DateFormat)format).setLenient(false);
            } else if (locale != null) {
            	format = DateFormat.getDateInstance(DateFormat.DEFAULT, locale);
            	((DateFormat)format).setLenient(false);
            }else{
            	format = DateFormat.getDateInstance();
            	((DateFormat)format).setLenient(false);
             }
            try{
            	value = ((SimpleDateFormat)format).parse(constant);
            }catch(ParseException e){
				errorMessage = e.getLocalizedMessage() + " to record: " + targetMetadata[recNo].getName() 
				+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
				logger.error(errorMessage);
				throw new ComponentNotReadyException(e);
            }
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
			try {
					value = DecimalFactory.getDecimal(constant);
				} catch (NumberFormatException e) {
					errorMessage = e.getLocalizedMessage() + " to record: " + targetMetadata[recNo].getName() 
					+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
					logger.error(errorMessage);
					throw new ComponentNotReadyException(e);
				}
			break;
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
            if ((formatString != null) && (formatString.length() != 0)) {
                if (locale != null) {
                    format = new DecimalFormat(formatString, new DecimalFormatSymbols(locale));
                } else {
                    format = new DecimalFormat(formatString);
                }
            } else if (locale != null) {
            	format = DecimalFormat.getInstance(locale);
            }else{
            	format = DecimalFormat.getInstance();
            }
            try{
            	value = ((DecimalFormat)format).parse(constant);
            }catch(ParseException e){
				errorMessage = e.getLocalizedMessage() + " to record: " + targetMetadata[recNo].getName() 
				+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
				logger.error(errorMessage);
				throw new ComponentNotReadyException(e);
            }
            if (type == DataFieldMetadata.LONG_FIELD || type == DataFieldMetadata.INTEGER_FIELD && !(value instanceof Long)){
				errorMessage = constant + " is not Long type to record: " + targetMetadata[recNo].getName() 
				+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
				logger.error(errorMessage);
				throw new ComponentNotReadyException(errorMessage);
           }
            if (type == DataFieldMetadata.INTEGER_FIELD && 
            		((Long)value > Integer.MAX_VALUE || (Long)value < Integer.MIN_VALUE )){
				errorMessage = constant + " not in range to record: " + targetMetadata[recNo].getName() 
				+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
				logger.error(errorMessage);
				throw new ComponentNotReadyException(errorMessage);
           }
			break;
		}
		return true;
	}
	
	/**
	 * Method, which puts mapping rules to map. First it tries to find fields with
	 * 	identical names in corresponding input and output metadata. If not all 
	 * 	output fields were found it tries to find them in other input records. If
	 * 	not all output fields were found it tries to find fields with the same names
	 * 	ignoring case in corresponding record. If still there are some output fields
	 * 	without paire it tries to find fields with the same name ignoring case in
	 * 	other records, eg.<br>
	 * 	outFieldsNames:<br>
	 * <ul>
	 * 		<li>lname, fname, address, phone</li>
	 * 		<li>Lname, fname, id</li></ul>
	 * 	inFieldsNames:
	 * <ul>
	 * 		<li>Lname, fname, id, address</li>
	 * 		<li>lname, fname, phone</li></ul>
	 * Mapping:
	 * <ul>
	 * 		<li>0.0 <-- 1.0</li>
	 * 		<li>0.1 <-- 0.1</li>
	 * 		<li>0.2 <-- 0.3</li>
	 * 		<li>0.3 <-- 1.2</li>
	 * 		<li>1.0 <-- 0.0</li>
	 * 		<li>1.1 <-- 1.1</li>
	 * 		<li>1.2 <-- 0.2</li></ul>
	 * 
	 * @param transformMap map to put rules
	 * @param outFields output fields to mapping
	 * @param inFields input fields for mapping
	 */
	protected void putMappingByNames(Map<String, Rule> transformMap, 
			String[] outFields, String[] inFields){
		String[][] outFieldsName = new String[targetMetadata.length][maxNumFields(targetMetadata)];
		for (int i = 0; i < outFields.length; i++) {
			outFieldsName[getRecNo(outFields[i])][getFieldNo(outFields[i])] = 
				targetMetadata[getRecNo(outFields[i])].getField(getFieldNo(outFields[i])).getName();
		}
		String[][] inFieldsName = new String[sourceMetadata.length][maxNumFields(sourceMetadata)];
		for (int i = 0; i < inFields.length; i++) {
			inFieldsName[getRecNo(inFields[i])][getFieldNo(inFields[i])] = 
				sourceMetadata[getRecNo(inFields[i])].getField(getFieldNo(inFields[i])).getName();
		}
		int index;
		//find identical in corresponding records
		for (int i = 0; (i < outFieldsName.length) && (i < inFieldsName.length); i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				if (outFieldsName[i][j] != null) {
					index = StringUtils.findString(outFieldsName[i][j],
							inFieldsName[i]);
					if (index > -1) {//output field name found amoung input fields
						if (putMapping(i, j, i, index, transformMap)){
							outFieldsName[i][j] = null;
							inFieldsName[i][index] = null;
							break;
						}
					}
				}				
			}
		}
		//find identical in other records
		for (int i = 0; i < outFieldsName.length; i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				for (int k = 0; k < inFieldsName.length; k++) {
					if ((outFieldsName[i][j] != null) && (k != i)) {
						index = StringUtils.findString(outFieldsName[i][j],
								inFieldsName[k]);
						if (index > -1) {//output field name found amoung input fields
							if (putMapping(i, j, i, index, transformMap)){
								outFieldsName[i][j] = null;
								inFieldsName[k][index] = null;
								break;
							}
						}
					}					
				}
			}
		}
		//find ignore case in corresponding records
		for (int i = 0; (i < outFieldsName.length) && (i < inFieldsName.length); i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				if (outFieldsName[i][j] != null) {
					index = StringUtils.findStringIgnoreCase(
							outFieldsName[i][j], inFieldsName[i]);
					if (index > -1) {//output field name found amoung input fields
						if (putMapping(i, j, i, index, transformMap)){
							outFieldsName[i][j] = null;
							inFieldsName[i][index] = null;
							break;
						}
					}
				}				
			}
		}
		//find ignore case in other records
		for (int i = 0; i < outFieldsName.length; i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				for (int k = 0; k < inFieldsName.length; k++) {
					if ((outFieldsName[i][j] != null) && (k != i)) {
						index = StringUtils.findStringIgnoreCase(
								outFieldsName[i][j], inFieldsName[k]);
						if (index > -1) {//output field name found amoung input fields
							if (putMapping(i, j, i, index, transformMap)){
								outFieldsName[i][j] = null;
								inFieldsName[k][index] = null;
								break;
							}
						}
					}					
				}
			}
		}
	}
	
	/**
	 * This method puts mapping into given output and input field to transform
	 * 	map if theese fields have correct types due to policy type
	 * 
	 * @param outRecNo number of record from output metadata
	 * @param outFieldNo number of field from output metadata
	 * @param inRecNo number of record from input metadata
	 * @param inFieldNo number of field from input metadata
	 * @param transformMap
	 * @return true if mapping was put into map, false in other case
	 */
	protected boolean putMapping(int outRecNo,int outFieldNo,int inRecNo, int inFieldNo, 
			Map<String, Rule> transformMap){
		Rule rule;
		if (!checkTypes(fieldPolicy, outRecNo, outFieldNo, inRecNo, inFieldNo)){
			if (fieldPolicy == PolicyType.STRICT){
				logger.warn("Found fields with the same names but other types: ");
				logger.warn(targetMetadata[outRecNo].getName() + DOT + 
						targetMetadata[outRecNo].getField(outFieldNo).getName() + 
						" type - " + targetMetadata[outRecNo].getFieldTypeAsString(outFieldNo) 
						+ getDecimalParams(targetMetadata[outRecNo].getField(outFieldNo)));
				logger.warn(sourceMetadata[inRecNo].getName() + DOT + 
						sourceMetadata[inRecNo].getField(inFieldNo).getName() + 
						" type - " + sourceMetadata[inRecNo].getFieldTypeAsString(inFieldNo) 
						+ getDecimalParams(sourceMetadata[inRecNo].getField(inFieldNo)));
			}
			if (fieldPolicy == PolicyType.CONTROLLED){
				logger.warn("Found fields with the same names but incompatible types: ");
				logger.warn(targetMetadata[outRecNo].getName() + DOT + 
						targetMetadata[outRecNo].getField(outFieldNo).getName() + 
						" type - " + targetMetadata[outRecNo].getFieldTypeAsString(outFieldNo) 
						+ getDecimalParams(targetMetadata[outRecNo].getField(outFieldNo)));
				logger.warn(sourceMetadata[inRecNo].getName() + DOT + 
						sourceMetadata[inRecNo].getField(inFieldNo).getName() + 
						" type - " + sourceMetadata[inRecNo].getFieldTypeAsString(inFieldNo) 
						+ getDecimalParams(sourceMetadata[inRecNo].getField(inFieldNo)));
			}
			return false;
		}else{//map fields
			rule = transformMap.remove(String.valueOf(outRecNo) + DOT	+ outFieldNo);
			if (rule == null) {
				rule = new Rule(Rule.FIELD, String.valueOf(inRecNo)
						+ DOT + inFieldNo);
			} else {
				rule.setType(Rule.FIELD);
				rule.setValue(String.valueOf(inRecNo) + DOT + inFieldNo);
			}
			transformMap.put(String.valueOf(outRecNo) + DOT + outFieldNo, rule);
			return true;
		}
	}
	
	/**
	 * Finds fields from metadata matching given pattern
	 * 
	 * @param pattern
	 * @param metadata
	 * @return list of fields matching given metadata
	 */
	protected ArrayList<String> findFields(String pattern,DataRecordMetadata[] metadata){
		ArrayList<String> list = new ArrayList<String>();
		String recordNoString = pattern.substring(0,pattern.indexOf(DOT));
		String fieldNoString = pattern.substring(pattern.indexOf(DOT)+1);
		int fieldNo;
		int recNo;
		try {//check if first part of pattern is "real" pattern or number of record
			recNo = Integer.parseInt(recordNoString);
			try {//we have one record Number
				//check if second part of pattern is "real" pattern or number of field
				fieldNo = Integer.parseInt(fieldNoString);
				//we have one record field number
				list.add(pattern);
			}catch(NumberFormatException e){//second part of pattern is not a number
				//find matching fields
				for (int i=0;i<metadata[recNo].getNumFields();i++){
					if (WcardPattern.checkName(fieldNoString, metadata[recNo].getField(i).getName())){
						list.add(String.valueOf(recNo) + DOT + i);
					}
				}
			}
		}catch (NumberFormatException e){//first part of pattern is not a number
			//check all matadata names if match pattern
			for (int i=0;i<metadata.length;i++){
				if (WcardPattern.checkName(recordNoString, metadata[i].getName()))
					try {//check if second part of pattern is "real" pattern or number of field
						fieldNo = Integer.parseInt(fieldNoString);
						//we have matching metadata name and field number
						list.add(String.valueOf(i) + DOT + fieldNoString);
					}catch(NumberFormatException e1){//second part of pattern is not a number
						//find matching fields
						for (int j=0;j<metadata[i].getNumFields();j++){
							if (WcardPattern.checkName(fieldNoString, metadata[i].getField(j).getName())){
								list.add(String.valueOf(i) + DOT + j);
							}
						}
					}
				}
			}
		return list;
	}
	
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public void signal(Object signalObject) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#transform(org.jetel.data.DataRecord[], org.jetel.data.DataRecord[])
	 */
	public boolean transform(DataRecord[] sources, DataRecord[] target)
			throws TransformException {
		//array "order" stores coordinates of output fields in order they will be assigned
		for (int i = 0; i < order.length; i++) {
			ruleType = transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getType();
			ruleString = transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue();
			switch (ruleType) {
			case Rule.FIELD:
				target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(
						transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue(sources));
				break;
			case Rule.SEQUENCE:
				//ruleString can be only sequence ID or with method eg. sequenceID.getNextLongValue()
				sequenceID = ruleString.indexOf(DOT) == -1 ? ruleString
						: ruleString.substring(0, ruleString.indexOf(DOT));
				target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(
						transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]]
								.getValue(getGraph().getSequence(sequenceID)));
				break;
			case Rule.PARAMETER://in method init changed to constant
				break;
			default:// constant
				//for DateDataField constant could be stored in string representation
				if (target[order[i][REC_NO]].getField(order[i][FIELD_NO]).getType() == DataFieldMetadata.DATE_FIELD
						|| target[order[i][REC_NO]].getField(order[i][FIELD_NO]).getType() == DataFieldMetadata.DATETIME_FIELD) {
					try {
						//get date from string
						Date date = SimpleDateFormat.getDateInstance()
								.parse(ruleString);
						target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(date);
					} catch (ParseException e) {
						// value was set as String not a Date
						try {
							target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(ruleString);
						} catch (BadDataFormatException e1) {
							errorMessage = e1.getLocalizedMessage();
							logger.error(errorMessage);
							throw new TransformException("",e1);
						}
					}
				} else {//not DateDataField
					try {
						target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(ruleString);
					} catch (BadDataFormatException e) {
						errorMessage = e.getLocalizedMessage();
						logger.error(errorMessage);
						throw new TransformException("",e);
					}
				}
				break;
			}
		}
		return true;
	}
	
	/**
	 * Changes pattern given in one of possible format to record.field
	 * 
	 * @param pattern
	 * @return pattern in format record.field of null if it is not possible
	 */
	protected static String resolveField(String pattern){
		String[] parts = pattern.split("\\.");
		switch (parts.length) {
		case 2:
			if (parts[0].startsWith("$")){// ${recNo.field}
				return parts[0].substring(2) + DOT + parts[1].substring(0,parts[1].length()-1);
			}else{// recNo.field
				return pattern;
			}
		case 3:
			if (parts[0].startsWith("$")){// ${in/out.recNo.field}
				return parts[1] + DOT + parts[2].substring(0,parts[2].length() -1);
			}else{//in/out.recNo.field
				return parts[1] + DOT + parts[2];
			}
		default:return null;
		}
	}

	public Map<String, String> getRules() {
		return rules;
	}

	/**
	 * Gets rules for concrete metadata in well readable form 
	 * 
	 * @return resolved rules
	 */
	public ArrayList<String> getResolvedRules() {
		ArrayList<String> list = new ArrayList<String>();
		StringBuilder ruleString = new StringBuilder();
		int recordNumber;
		int fieldNumber;
		for (int recNo = 0;recNo < transformMapArray.length; recNo++){
			for (int fieldNo=0;fieldNo < transformMapArray[0].length; fieldNo++){
				if (transformMapArray[recNo][fieldNo] != null) {
					ruleString.setLength(0);
					switch (transformMapArray[recNo][fieldNo].getType()) {
					case Rule.FIELD:
						recordNumber = getRecNo(transformMapArray[recNo][fieldNo].getValue());
						fieldNumber = getFieldNo(transformMapArray[recNo][fieldNo].getValue());
						ruleString.append(sourceMetadata[recordNumber].getName());
						ruleString.append(DOT);
						ruleString.append(sourceMetadata[recordNumber].getField(fieldNumber).getName());
						break;
					case Rule.PARAMETER:
						if (transformMapArray[recNo][fieldNo].getValue().startsWith("$")) {
							ruleString.append(transformMapArray[recNo][fieldNo].getValue());
						}else{
							ruleString.append("Parameter: ");
							ruleString.append(transformMapArray[recNo][fieldNo].getValue());
						}
						break;
					case Rule.SEQUENCE:
						ruleString.append("${seq.");
						ruleString.append(transformMapArray[recNo][fieldNo].getValue());
						ruleString.append("}");
						break;
					default:
						ruleString.append(transformMapArray[recNo][fieldNo].getValue());
						break;
					}
					list.add(targetMetadata[recNo].getName() + DOT + 
							targetMetadata[recNo].getField(fieldNo).getName() + "="
							+ ruleString);
				}				
			}
		}
		return list;
	}

	public PolicyType getFieldPolicy() {
		return fieldPolicy;
	}

	public void setFieldPolicy(PolicyType fieldPolicy) {
		this.fieldPolicy = fieldPolicy;
	}

	/**
	 * Finds maximal length of metadata
	 * 
	 * @param metadata
	 * @return maximal length of metadatas
	 */
	protected int maxNumFields(DataRecordMetadata[] metadata){
		int numFields = 0;
		for (int i = 0; i < metadata.length; i++) {
			if (metadata[i].getNumFields() > numFields) {
				numFields = metadata[i].getNumFields();
			}
		}
		return numFields;
	}
	
	/**
	 * Gets part of string before . and changed it to Integer
	 * 
	 * @param recField recNo.FieldNo
	 * @return record number
	 */
	protected Integer getRecNo(String recField){
		return Integer.valueOf(recField.substring(0, recField.indexOf(DOT)));
	}
	
	/**
	 * Gets part of string after . and changed it to Integer
	 * 
	 * @param recField
	 * @return field number
	 */
	protected Integer getFieldNo(String recField){
		return Integer.valueOf(recField.substring(recField.indexOf(DOT) + 1));
	}
	
	protected String getDecimalParams(DataFieldMetadata field){
		if (field.getType() != DataFieldMetadata.DECIMAL_FIELD){
			return "";
		}
		StringBuilder params = new StringBuilder(5);
		params.append('(');
		params.append(field.getProperty(DataFieldMetadata.LENGTH_ATTR));
		params.append(',');
		params.append(field.getProperty(DataFieldMetadata.SCALE_ATTR));
		params.append(')');
		return params.toString();
	}
	
	/**
	 *Private class for storing transformation rules
	 */
	class Rule {
		
		//Types of rule
		final static int FIELD = 0;
		final static int CONSTANT = 1;
		final static int SEQUENCE = 2;
		final static int PARAMETER = 3;
		
		int type;
		String value;
		
		Rule(int type, String value){
			this.type = type;
			this.value = value;
		}
		
		String getValue(){
			return value;
		}
		
		void setValue(String value){
			this.value = value;
		}
		
		int getType() {
			return type;
		}

		void setType(int type) {
			this.type = type;
		}

		/**
		 * When rule type is FIELD it means that "value" is in form recNo.fieldNo,
		 * 	where <i> recNo</i> and <i>fieldNo</i> are integers. This method gets 
		 *  proper data field from proper record
		 * 
		 * @param records
		 * @return proper data field from proper record
		 */
		DataField getValue(DataRecord[] records){
			int dotIndex = value.indexOf(CustomizedRecordTransform.DOT);
			int recNo = dotIndex > -1 ? Integer.parseInt(value.substring(0, dotIndex)) : 0;
			int fieldNo = dotIndex > -1 ? Integer.parseInt(value.substring(dotIndex + 1)) : Integer.parseInt(value); 
			return records[recNo].getField(fieldNo);
		}
		
		/**
		 * When rule type is SEQUENCE it means that "value" stores sequence Id 
		 * 	optionally with method name. If method name is lacking there is used
		 * 	nextValueInt() method. This method gets proper value from sequence.
		 * 
		 * @param sequence
		 * @return value from sequence
		 * @throws TransformException 
		 */
		Object getValue(Sequence sequence) throws TransformException{
			if (sequence == null){
				return null;
			}
			int dotIndex = value.indexOf(CustomizedRecordTransform.DOT);
			String method = dotIndex > -1 ? value.substring(dotIndex +1) : "nextValueInt()";
			if (method.equals("currentValueString()")){
				return sequence.currentValueString();
			}
			if (method.equals("nextValueString()")){
				return sequence.nextValueString();
			}
			if (method.equals("currentValueInt()")){
				return sequence.currentValueInt();
			}
			if (method.equals("nextValueInt()")){
				return sequence.nextValueInt();
			}
			if (method.equals("currentValueLong()")){
				return sequence.currentValueLong();
			}
			if (method.equals("nextValueLong()")){
				return sequence.nextValueLong();
			}
			throw new TransformException("Unknown method \"" + method + "\".");
		}
		
	}

}
