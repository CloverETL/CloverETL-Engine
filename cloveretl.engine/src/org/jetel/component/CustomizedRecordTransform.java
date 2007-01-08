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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.data.primitive.NumericFormat;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.StringUtils;
import org.jetel.util.TypedProperties;
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
 * <li>add...Rule(...)<br>
 * .<br></li>
 * <li>deleteRule(...)<br>
 * .<br></li>
 * <li>init()</li>
 * <li>transform() <i>for each input&amp;output records pair</i></li>
 * <li><i>optionally</i> getMessage() <i>or</i> signal() <i>or</i> getSemiResult()</li>
 * <li>finished()
 * </ol>
 * 
 * <h4>Example:</h4>
 * <b>Records:</b>
 * <pre>
 * in:
 * #0|Name|S->  HELLO 
 * #1|Age|N->135.0
 * #2|City|S->Some silly longer string.
 * #3|Born|D->
 * #4|Value|d->-999.0000000000
 * 
 * in1:
 * #0|Name|S->  My name 
 * #1|Age|N->13.25
 * #2|City|S->Prague
 * #3|Born|D->Thu Nov 30 09:54:07 CET 2006
 * #4|Value|i->
 *
 * out:
 * #0|Name|B->
 * #1|Age|N->
 * #2|City|S->
 * #3|Born|D->
 * #4|Value|d->
 *
 * out1:
 * #0|Name|S->  
 * #1|Age|d->
 * #2|City|S->
 * #3|Born|D->
 * #4|Value|i->
 * </pre>
 * <b>Java code:</b>
 * <pre>
 *  CustomizedRecordTransform transform = new CustomizedRecordTransform(LogFactory.getLog(this.getClass()));
 *  transform.setGraph(graph);
 *  transform.addFieldToFieldRule("${1.?a*}", "${1.*e}");
 *  transform.addFieldToFieldRule("*.City", 0, 2);
 *  transform.addConstantToFieldRule(1,3, new GregorianCalendar(1973,3,23).getTime());
 *  transform.addConstantToFieldRule(4, "1.111111111");
 *  transform.addSequenceToFieldRule("*.Age", graph.getSequence("ID"));
 *  transform.addRule("out.City", "${seq.ID.nextString}");
 *  transform.addParameterToFieldRule(1, 0, "${WORKSPACE}");
 *  transform.addParameterToFieldRule(1, "City", "YourCity");
 *  transform.deleteRule(1, "Age");
 *  transform.init(properties, new DataRecordMetadata[]{metadata, metadata1}, new DataRecordMetadata[]{metaOut,metaOut1});
 *  List<String> rules = transform.getRules();
 *  System.out.println("Rules:");
 *  for (Iterator<String> i = rules.iterator();i.hasNext();){
 *  	System.out.println(i.next());
 *  }
 *  rules = transform.getResolvedRules();
 *  System.out.println("Resolved rules:");
 *  for (Iterator<String> i = rules.iterator();i.hasNext();){
 *  	System.out.println(i.next());
 *  }
 *  List<Integer[]> fields = transform.getFieldsWithoutRules();
 *  System.out.println("Fields without rules:");
 *  Integer[] index;
 *  for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
 *  	index = i.next();
 *  	System.out.println(outMatedata[index[0]].getName() + CustomizedRecordTransform.DOT + 
 *  			outMatedata[index[0]].getField(index[1]).getName());
 *  }
 *  fields = transform.getNotUsedFields();
 *  System.out.println("Not used input fields:");
 *  for (Iterator<Integer[]> i = fields.iterator();i.hasNext();){
 *  	index = i.next();
 *  	System.out.println(inMetadata[index[0]].getName() + CustomizedRecordTransform.DOT + 
 *  			inMetadata[index[0]].getField(index[1]).getName());
 *  }
 *  transform.transform(new DataRecord[]{record, record1}, new DataRecord[]{out,out1});
 *  System.out.println(record.getMetadata().getName() + ":\n" + record.toString());
 *  System.out.println(record1.getMetadata().getName() + ":\n" + record1.toString());
 *  System.out.println(out.getMetadata().getName() + ":\n" + out.toString());
 *  System.out.println(out1.getMetadata().getName() + ":\n" + out1.toString());
 * </pre>  
 *<b>Output:</b>
 * <pre>
 *  Rules:
 *  FIELD_RULE:${1.?a*}=${1.*e}
 *  FIELD_RULE:*.City=0.2
 *  CONSTANT_RULE:1.3=Apr 23, 1973
 *  CONSTANT_RULE:0.4=1.111111111
 *  SEQUENCE_RULE:*.Age=ID
 *  SEQUENCE_RULE:out.City=ID.nextString
 *  PARAMETER_RULE:1.0=${WORKSPACE}
 *  PARAMETER_RULE:1.City=YourCity
 *  DELETE_RULE:1.Age=1.Age
 *  Resolved rules:
 *  out.Age=${seq.ID}
 *  out.City=${seq.ID.nextString}
 *  out.Value=1.111111111
 *  out1.Name=/home/avackova/workspace
 *  out1.City=London
 *  out1.Born=23-04-1973
 *  out1.Value=in1.Value
 *  Fields without rules:
 *  out.Name
 *  out.Born
 *  out1.Age
 *  Not used input fields:
 *  in.Name
 *  in.Age
 *  in.City
 *  in.Born
 *  in.Value
 *  in1.Name
 *  in1.Age
 *  in1.City
 *  in1.Born
 *  in:
 *  #0|Name|S->  HELLO 
 *  #1|Age|N->135.0
 *  #2|City|S->Some silly longer string.
 *  #3|Born|D->
 *  #4|Value|d->-999.0000000000
 *  
 *  in1:
 *  #0|Name|S->  My name 
 *  #1|Age|N->13.25
 *  #2|City|S->Prague
 *  #3|Born|D->Thu Nov 30 10:40:15 CET 2006
 *  #4|Value|i->
 *  
 *  out:
 *  #0|Name|B-> 
 *  #1|Age|N->2.0
 *  #2|City|S->1
 *  #3|Born|D->
 *  #4|Value|d->1.1
 *  
 *  out1:
 *  #0|Name|S->/home/user/workspace
 *  #1|Age|d->
 *  #2|City|S->London
 *  #3|Born|D->23-04-1973
 *  #4|Value|i->
 *</pre>
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
	
	protected final static String PARAM_OPCODE_REGEX = "\\$\\{par\\.(.*)\\}";
	protected final static Pattern PARAM_PATTERN = Pattern.compile(PARAM_OPCODE_REGEX);
	protected final static String SEQ_OPCODE_REGEX = "\\$\\{seq\\.(.*)\\}";
	protected final static Pattern SEQ_PATTERN = Pattern.compile(SEQ_OPCODE_REGEX);
	protected final static String FIELD_OPCODE_REGEX = "\\$\\{in\\.(.*)\\}";
	protected final static Pattern FIELD_PATTERN = Pattern.compile(FIELD_OPCODE_REGEX);
	
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
	 * @param patternOut output field's pattern
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
		if (value == null) {
			value = "null";
		}
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
	public void addConstantToFieldRule(String patternOut, long value){
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
		if (value == null) {
			rules.put(patternOut, "null");
		}else{
			rules.put(patternOut, String.valueOf(Rule.CONSTANT) + COLON + 
						SimpleDateFormat.getDateInstance().format(value));
		}
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut output fields' pattern
	 * @param value value to assign
	 */
	public void addConstantToFieldRule(String patternOut, Numeric value){
		if (value == null) {
			rules.put(patternOut, "null");
		}else{
			rules.put(patternOut, String.valueOf(Rule.CONSTANT) + COLON + value);
		}
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
	public void addConstantToFieldRule(int recNo, int fieldNo, long value){
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
	public void addConstantToFieldRule(int recNo, String field, long value){
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
	public void addConstantToFieldRule(int fieldNo, long value){
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
	 * @param parameterName (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(String patternOut, String parameterName){
		if (parameterName.indexOf(DOT) > -1 ) {
			parameterName = parameterName.substring(parameterName.indexOf(DOT) + 1, parameterName.length() -1);
		}
		rules.put(patternOut, String.valueOf(Rule.PARAMETER) + COLON + parameterName);
	}

	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param recNo output record's number
	 * @param fieldNo output record's field number
	 * @param parameterName (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(int recNo, int fieldNo, String parameterName){
		addParameterToFieldRule(String.valueOf(recNo) + DOT + fieldNo, parameterName);
	}
	
	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param recNo output record's number
	 * @param field output record's field name
	 * @param parameterName (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(int recNo, String field, String parameterName){
		addParameterToFieldRule(String.valueOf(recNo) + DOT + field, parameterName);
	}
	
	/**
	 * Mathod for adding rule: assigning parameter value to output fields in 0th output record
	 * 
	 * @param fieldNo output record's field number
	 * @param parameterName (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(int fieldNo, String parameterName){
		addParameterToFieldRule(0,fieldNo, parameterName);
	}
	
	/**
	 * Method for adding rule in CloverETL syntax
	 * This method calls proper add....Rule depending on syntax of pattern
	 * 
	 * @see org.jetel.util.CodeParser
	 * @param patternOut output field's pattern
	 * @param pattern rule for output field as: ${par.parameterName}, ${seq.sequenceID},
	 * 	${in.inRecordPattern.inFieldPattern}. If "pattern" doesn't much to any above, 
	 * 	it is regarded as constant. 
	 */
	public void addRule(String patternOut, String pattern){
		Matcher matcher = PARAM_PATTERN.matcher(pattern);
		if (matcher.find()) {
			addParameterToFieldRule(patternOut, pattern);
		}else{
			matcher = SEQ_PATTERN.matcher(pattern);
			if (matcher.find()){
				addSequenceToFieldRule(patternOut, pattern);
			}else{
				matcher = FIELD_PATTERN.matcher(pattern);
				if (matcher.find()){
					addFieldToFieldRule(patternOut, pattern);
				}else{
					addConstantToFieldRule(patternOut, pattern);
				}
			}
		}
	}
	
	/**
	 * This method deletes rule for given fields, which was set before
	 * 
	 * @param patternOut output field pattern for deleting rule
	 */
	public void deleteRule(String patternOut){
		rules.put(patternOut, String.valueOf(Rule.DELETE) + COLON + patternOut);
	}
	
	/**
	 * This method deletes rule for given field, which was set before
	 * 
	 * @param outRecNo output record number
	 * @param outFieldNo output record's field number
	 */
	public void deleteRule(int outRecNo, int outFieldNo){
		String patternOut = String.valueOf(outRecNo) + DOT + outFieldNo;
		rules.put(patternOut, String.valueOf(Rule.DELETE) + COLON + patternOut);
	}
	
	/**
	 * This method deletes rule for given field, which was set before
	 * 
	 * @param outRecNo  output record number
	 * @param outField output record's field name
	 */
	public void deleteRule(int outRecNo, String outField){
		String patternOut = String.valueOf(outRecNo) + DOT + outField;
		rules.put(patternOut, String.valueOf(Rule.DELETE) + COLON + patternOut);
	}

	/**
	 * This method deletes rule for given field in 0th output record, which was set before
	 * 
	 * @param outFieldNo output record's field number
	 */
	public void deleteRule(int outFieldNo){
		String patternOut = String.valueOf(0) + DOT + outFieldNo;
		rules.put(patternOut, String.valueOf(Rule.DELETE) + COLON + patternOut);
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
			//find output fields pattern
			field = resolveField(rulesEntry.getKey());
			if (field == null){
				errorMessage = "Wrong pattern for output fields: " + rulesEntry.getKey();
				logger.error(errorMessage);
				throw new ComponentNotReadyException(errorMessage);
			}
			//find output fields from pattern
			outFields = findFields(field, targetMetadata).toArray(new String[0]);
			if (outFields.length == 0){
				errorMessage = "There is no output field matching \""
					+ field + "\" pattern";
				logger.error(errorMessage);
				throw new ComponentNotReadyException(errorMessage);
				
			}
			inFields = new String[0];
			//find type: Rule.FIELD, Rule.CONSTANT,	Rule.SEQUENCE, Rule.PARAMETER
			type = Integer.parseInt(rulesEntry.getValue().substring(0, rulesEntry.getValue().indexOf(COLON)));
			//find rule: patternIn, constant, sequence ID (optionally with method) or parameter name
			ruleString = rulesEntry.getValue().substring(rulesEntry.getValue().indexOf(COLON)+1);
			if (type == Rule.DELETE){
				for (int i = 0; i < outFields.length; i++) {
					rule = transformMap.remove(outFields[i]);
				}		
				continue;
			}
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
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
					
				}
			}
			if (type == Rule.FIELD && inFields.length > 1){
				//find mapping by names
				if (putMappingByNames(transformMap,outFields,inFields, rulesEntry.getKey() + "=" + rulesEntry.getValue().substring(2)) == 0) {
					errorMessage = "Not found any field for mapping by names due to rule:\n" + 
					field + " - output fields pattern\n" + 
					ruleString + " - input fields pattern";
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
				}
			}else{//for each output field the same rule
				//for each output field from pattern put rule to map
				for (int i=0;i<outFields.length;i++){
					field = outFields[i];
					//check if there is just any rule for given output field
					rule = transformMap.remove(getRecNo(field) + DOT + getFieldNo(field));
					if (type == Rule.FIELD) {
						ruleString = inFields[0];
					}
					rule = validateRule(getRecNo(field),getFieldNo(field),type,
							ruleString, rulesEntry.getKey() + "=" + rulesEntry.getValue().substring(2));
					transformMap.put(outFields[i], rule);
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
	
	/**
	 * This method checks if given rule can be applied to given output field
	 * 
	 * @param recNo output record number
	 * @param fieldNo output record's field number
	 * @param ruleType type of rule (Rule.FIELD, Rule.CONSTANT,	Rule.SEQUENCE, Rule.PARAMETER)
	 * @param ruleString
	 * @return rule with correct parameters
	 * @throws ComponentNotReadyException
	 */
	protected Rule validateRule(int recNo, int fieldNo, int ruleType,String ruleString,
			String ruleSource) throws ComponentNotReadyException {
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
					if (!(targetMetadata[recNo].getField(fieldNo).isNullable() || 
							targetMetadata[recNo].getField(fieldNo).isDefaultValue())){
						logger.error(errorMessage);
						throw new ComponentNotReadyException(errorMessage);
					}else{
						logger.warn(errorMessage);
					}
				}
			}
			//check if parameter value can be set to given field
			StringBuilder correctParameterValue = new StringBuilder(
					parameterValue == null ? "null" : parameterValue); 
			if ((fieldType != DataFieldMetadata.BYTE_FIELD || 
					fieldType != DataFieldMetadata.BYTE_FIELD_COMPRESSED ||
					fieldType != DataFieldMetadata.STRING_FIELD ) &&
					parameterValue != null) {
				checkConstant(recNo, fieldNo, correctParameterValue);
			}
			//change parameter rule to constant rule with parameter value
			return new Rule(Rule.CONSTANT,correctParameterValue.toString(), ruleSource);
		case Rule.CONSTANT:
			if (ruleString.equals("null") && !(targetMetadata[recNo].getField(fieldNo).isNullable() || 
							targetMetadata[recNo].getField(fieldNo).isDefaultValue())){
				errorMessage = "Null value not allowed to record: " + targetMetadata[recNo].getName() 
				+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName();
				logger.error(errorMessage);
				throw new ComponentNotReadyException(errorMessage);
			}
			//check if constant can be set to given field
			StringBuilder correctConstant = new StringBuilder(ruleString);
			if ((fieldType != DataFieldMetadata.BYTE_FIELD ||
					fieldType != DataFieldMetadata.BYTE_FIELD_COMPRESSED ||
					fieldType != DataFieldMetadata.STRING_FIELD ) &&
					ruleString != null) {
				if (checkConstant(recNo, fieldNo, correctConstant)) {
					ruleString = correctConstant.toString();
				}
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
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
				}
			}
			//check sequence method
			String method = ruleString.indexOf(DOT) > -1 ? 
					ruleString.substring(ruleString.indexOf(DOT) +1) : "nextValueInt()";
			char methodType = DataFieldMetadata.UNKNOWN_FIELD;
			if (method.toLowerCase().startsWith("currentvaluestring") || 
					method.toLowerCase().startsWith("currentstring") || 
					method.toLowerCase().startsWith("nextvaluestring") || 
					method.toLowerCase().startsWith("nextstring")){
				methodType = DataFieldMetadata.STRING_FIELD;
			}
			if (method.toLowerCase().startsWith("currentvalueint") || 
					method.toLowerCase().startsWith("currentint") || 
					method.toLowerCase().startsWith("nextvalueint") || 
					method.toLowerCase().startsWith("nextint")){
				methodType = DataFieldMetadata.INTEGER_FIELD;
			}
			if (method.toLowerCase().startsWith("currentvaluelong") || 
					method.toLowerCase().startsWith("currentlong") || 
					method.toLowerCase().startsWith("nextvaluelong") || 
					method.toLowerCase().startsWith("nextlong")){
				methodType = DataFieldMetadata.LONG_FIELD;
			}
			if (methodType == DataFieldMetadata.UNKNOWN_FIELD){
				errorMessage = "Unknown sequence method: " + method;
				logger.error(errorMessage);
				throw new ComponentNotReadyException(errorMessage);
			}
			//check if value from sequence can be set to given field
			if (!checkTypes(fieldType, null, methodType, null)){
				if (fieldPolicy == PolicyType.STRICT) {
					errorMessage = "Sequence method:" + ruleString + " does not " +
							"match field type:\n"+ targetMetadata[recNo].getName() + 
							DOT + targetMetadata[recNo].getField(fieldNo).getName() + 
							" type - " + targetMetadata[recNo].getField(fieldNo).getTypeAsString() + 
							getDecimalParams(targetMetadata[recNo].getField(fieldNo));
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
				}
				if (fieldPolicy == PolicyType.CONTROLLED){
					errorMessage = "Sequence method:" + ruleString + " does not " +
					"match field type:\n"+ targetMetadata[recNo].getName() + 
					DOT + targetMetadata[recNo].getField(fieldNo).getName() + 
					" type - " + targetMetadata[recNo].getField(fieldNo).getTypeAsString() + 
					getDecimalParams(targetMetadata[recNo].getField(fieldNo));
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
				}
			}
			break;
		case Rule.FIELD:
			//check input and output fields types
			if (!checkTypes(recNo, fieldNo, getRecNo(ruleString), getFieldNo(ruleString))){
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
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
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
					logger.error(errorMessage);
					throw new ComponentNotReadyException(errorMessage);
				}
			}
		}
		return new Rule(ruleType,ruleString, ruleSource);
	}
	
	/**
	 * This method checks if data field of type "inType" is subtype of data field
	 *  of type "outType". If types are DECIMAL can be check decimal parameters 
	 *  (LENGTH and SCALE)
	 * 
	 * @param outType type to be supertype
	 * @param outTypeDecimalParams if outType=DataFieldMetadata.DECIMAL_FIELD
	 * 	represents LENGTH and SCALE
	 * @param inType type to be subtype
	 * @param inTypeDEcimalParams if inType=DataFieldMetadata.DECIMAL_FIELD
	 * 	represents LENGTH and SCALE
	 * @return "true" if inType is subtype of outType, "false" in other cases
	 */
	private boolean checkTypes(char outType, int[] outTypeDecimalParams,
			char inType, int[] inTypeDEcimalParams){
		boolean checkTypes;
		if (outType == inType){
			//if DECIMAL type check LENGTH and SCALE 
			if (outType == DataFieldMetadata.DECIMAL_FIELD ){
				checkTypes = inTypeDEcimalParams[0] <= outTypeDecimalParams[0] && 
				inTypeDEcimalParams[1] <= outTypeDecimalParams[1];
			}else{
				checkTypes = true;
			}
		}else {
			checkTypes = false;
		}
		DataFieldMetadata outField = new DataFieldMetadata("out",outType,(short)1);
		if (outTypeDecimalParams != null){
			TypedProperties properties = new TypedProperties();
			properties.put(DataFieldMetadata.LENGTH_ATTR, outTypeDecimalParams[0]);
			properties.put(DataFieldMetadata.SCALE_ATTR, outTypeDecimalParams[1]);
			outField.setFieldProperties(properties);
		}
		DataFieldMetadata inField = new DataFieldMetadata("out",inType,(short)1);
		if (inTypeDEcimalParams != null){
			TypedProperties properties = new TypedProperties();
			properties.put(DataFieldMetadata.LENGTH_ATTR, inTypeDEcimalParams[0]);
			properties.put(DataFieldMetadata.SCALE_ATTR, inTypeDEcimalParams[1]);
			inField.setFieldProperties(properties);
		}
		if (fieldPolicy == PolicyType.STRICT && !checkTypes){
			return false;
		}else if (fieldPolicy == PolicyType.CONTROLLED && !inField.isSubtype(outField)){
			return false;
		}
		return true;
	}
	
	/**
	 * This method checks if input field is subtype of output type
	 * 
	 * @param outRecNo output record number
	 * @param outFieldNo output record's field number
	 * @param inRecNo input record number
	 * @param inFieldNo input record's field number
	 * @return "true" if input field is subtype of output field, "false" in other cases
	 */
	private boolean checkTypes(int outRecNo, int outFieldNo, int inRecNo, int inFieldNo){
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
			return false;
		}else if (fieldPolicy == PolicyType.CONTROLLED && !inField.isSubtype(outField)){
			return false;
		}
		return true;
	}
	
	/**
	 * Check if constant can be set to given fields. In some cases the method can 
	 * 	change constant string representation to proper form
	 * 
	 * @param recNo output record number
	 * @param fieldNo output record's field number
	 * @param constant string representation of constatnt to be checked
	 * @return "true" if constant can be set to given field. In some cases string
	 * 	representation of constant can be changed
	 * @throws ComponentNotReadyException
	 */
	private boolean checkConstant(int recNo, int fieldNo, StringBuilder constant) throws ComponentNotReadyException{
		char type = targetMetadata[recNo].getFieldType(fieldNo);
		Object value;
		Format format = null; 
		//field format string
        String formatString = targetMetadata[recNo].getField(fieldNo).getFormatStr();
        Locale locale;
        // handle field locale
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
			//get date format from locale and format string
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
            try{//parse constant string representation
            	value = ((SimpleDateFormat)format).parse(constant.toString());
            }catch(ParseException e){
            	try {//value could be formatted in method addConstantToFieldRule(String patternOut, Date value)
                	value = (DateFormat.getDateInstance()).parse(constant.toString());
                	constant.setLength(0);
                	//format constatnt with proper format
                	constant.append(((SimpleDateFormat)format).format((Date)value));
            	}catch(ParseException e1){
					errorMessage = e1.getLocalizedMessage() + " to record: " + targetMetadata[recNo].getName() 
					+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName() + 
					". Expected pattern: " + ((SimpleDateFormat)format).toPattern();
					logger.error(errorMessage);
					throw new ComponentNotReadyException(e);
            	}
            }
			break;
		case DataFieldMetadata.DECIMAL_FIELD:
			//get numeric format from locale and format string
            if ((formatString != null) && (formatString.length() != 0)) {
                if (locale != null) {
                    format = new NumericFormat(formatString, new DecimalFormatSymbols(locale));
                } else {
                    format = new NumericFormat(formatString);
                }
            } else if (locale != null) {
            	format = new NumericFormat(locale);
            }else{
            	format = new NumericFormat();
            }
            try{//parse constant string representation
            	value = DecimalFactory.getDecimal(constant.toString(), (NumericFormat)format);
            }catch(NullPointerException e){//Can't get BigDecimal from string, try get Number
               	try {
                	value = (DecimalFormat.getInstance()).parse(constant.toString());
                	constant.setLength(0);
                	//format constatnt with proper format
                	constant.append(((NumericFormat)format).format(value));
               	}catch(ParseException e1){
					errorMessage = e1.getLocalizedMessage() + " to record: " + targetMetadata[recNo].getName() 
					+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName() +
					". Expected pattern: " + ((NumericFormat)format).toPattern();
					logger.error(errorMessage);
					throw new ComponentNotReadyException(e);
               	}
            }
			break;
		case DataFieldMetadata.INTEGER_FIELD:
		case DataFieldMetadata.LONG_FIELD:
		case DataFieldMetadata.NUMERIC_FIELD:
			//get decimal format from locale and format string
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
            try{//parse constant string representation
            	value = ((DecimalFormat)format).parse(constant.toString());
            }catch(ParseException e){
				try{//value could be formatted in one of method addConstantToFieldRule
					value = new Long(constant.toString());
				}catch(NumberFormatException eL){
					try{
						value = new Double(constant.toString());
					}catch(NumberFormatException eD){
						errorMessage = eD.getLocalizedMessage() + " to record: " + targetMetadata[recNo].getName() 
						+ " , field: " + targetMetadata[recNo].getField(fieldNo).getName() +
						". Expected pattern: " + ((DecimalFormat)format).toPattern();
						logger.error(errorMessage);
						throw new ComponentNotReadyException(e);
					}
				}
				constant.setLength(0);
               	constant.append(((DecimalFormat)format).format((Number)value));
            }
            if (type == DataFieldMetadata.LONG_FIELD || 
            		type == DataFieldMetadata.INTEGER_FIELD && 
            		!(value instanceof Long)){
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
	 * @return number of mappings put to transform map
	 */
	protected int putMappingByNames(Map<String, Rule> transformMap, 
			String[] outFields, String[] inFields, String rule){
		int count = 0;
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
						if (putMapping(i, j, i, index, rule, transformMap)){
							count++;
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
							if (putMapping(i, j, i, index, rule, transformMap)){
								count++;
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
						if (putMapping(i, j, i, index, rule, transformMap)){
							count++;
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
							if (putMapping(i, j, i, index, rule, transformMap)){
								count++;
								outFieldsName[i][j] = null;
								inFieldsName[k][index] = null;
								break;
							}
						}
					}					
				}
			}
		}
		return count;
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
			String ruleString, Map<String, Rule> transformMap){
		Rule rule;
		if (!checkTypes(outRecNo, outFieldNo, inRecNo, inFieldNo)){
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
						+ DOT + inFieldNo, ruleString);
			} else {
				rule.setType(Rule.FIELD);
				rule.setValue(String.valueOf(inRecNo) + DOT + inFieldNo, ruleString);
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
				try {
						target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(
								transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue(sources));
					} catch (BadDataFormatException e) {
						errorMessage = "Can't set value from field " + 
							sourceMetadata[getRecNo(transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue())].getName() + 
							DOT + targetMetadata[getRecNo(transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue())].getField(getFieldNo(transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue())).getName() + 
							" to field " + targetMetadata[order[i][REC_NO]].getName() + 
							DOT + targetMetadata[order[i][REC_NO]].getField(order[i][FIELD_NO]).getName() +
							"\n Genarated by " + transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getSource();
						logger.error(errorMessage);
						throw new TransformException(errorMessage, e, order[i][REC_NO],order[i][FIELD_NO]);
					}
				break;
			case Rule.SEQUENCE:
				//ruleString can be only sequence ID or with method eg. sequenceID.getNextLongValue()
				sequenceID = ruleString.indexOf(DOT) == -1 ? ruleString
						: ruleString.substring(0, ruleString.indexOf(DOT));
				try {
						target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(
								transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]]
										.getValue(getGraph().getSequence(sequenceID)));
					} catch (BadDataFormatException e) {
						errorMessage = "Can't set value from sequence " + sequenceID + 
							" to field " + targetMetadata[order[i][REC_NO]].getName() + 
							DOT + targetMetadata[order[i][REC_NO]].getField(order[i][FIELD_NO]).getName() +
							"\n Genarated by " + transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getSource();
						logger.error(errorMessage);
						throw new TransformException(errorMessage, e, order[i][REC_NO],order[i][FIELD_NO]);
					}
				break;
			case Rule.PARAMETER://in method init changed to constant
				break;
			default:// constant
					try {
						target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(ruleString);
					} catch (BadDataFormatException e) {
						errorMessage = "Can't set value " + ruleString + 
						" to field " + targetMetadata[order[i][REC_NO]].getName() + 
						DOT + targetMetadata[order[i][REC_NO]].getField(order[i][FIELD_NO]).getName() +
						"\n Genarated by " + transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getSource();
					logger.error(errorMessage);
					throw new TransformException(errorMessage, e, order[i][REC_NO],order[i][FIELD_NO]);
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

	/**
	 * Gets rule in form they were set by user
	 * 
	 * @return rules
	 */
	public ArrayList<String> getRules() {
		ArrayList<String> list = new ArrayList<String>();
		Entry<String, String> entry;
		for (Iterator<Entry<String, String>> iterator = rules.entrySet().iterator();iterator.hasNext();) {
			entry = iterator.next();
			list.add(getRuleTypeAsString(Integer.valueOf(entry.getValue().substring(0, 1))) + 
					":" + entry.getKey() + "=" + entry.getValue().substring(2));
		}
		return list;
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
	
	/**
	 * Gets output fields, for which there wasn't set any rule
	 * 
	 * @return indexes (output record number, output field number) of fields without rule 
	 */
	public ArrayList<Integer[]> getFieldsWithoutRules(){
		ArrayList<Integer[]> list = new ArrayList<Integer[]>();
		for (int recNo = 0;recNo < transformMapArray.length; recNo++){
			for (int fieldNo=0;fieldNo < transformMapArray[0].length; fieldNo++){
				if (fieldNo < targetMetadata[recNo].getNumFields() && 
						transformMapArray[recNo][fieldNo] == null) {
					list.add(new Integer[]{recNo,fieldNo});
				}
			}
		}
		return list;
	}
	
	/**
	 * Gets input fields not mapped on output fields
	 * 
	 * @return indexes (output record number, output field number) of not used input fields
	 */
	public ArrayList<Integer[]> getNotUsedFields(){
		String[] inFields = findFields("*.*", sourceMetadata).toArray(new String[0]);
		Rule rule;
		int index;
		for (int recNo = 0;recNo < transformMapArray.length; recNo++){
			for (int fieldNo=0;fieldNo < transformMapArray[0].length; fieldNo++){
				rule = transformMapArray[recNo][fieldNo];
				if (rule != null && rule.getType() == Rule.FIELD) {
					index = StringUtils.findString(rule.getValue(), inFields);
					if (index != -1) {
						inFields[index] = null;
					}
				}
			}
		}
		ArrayList<Integer[]> list = new ArrayList<Integer[]>();
		for (int i = 0; i < inFields.length; i++) {
			if (inFields[i] != null){
				list.add(new Integer[]{getRecNo(inFields[i]),getFieldNo(inFields[i])});
			}
		}
		return list;
	}
	
	/**
	 * Gets rule for given output field 
	 * 
	 * @param recNo output record number
	 * @param fieldNo output record's field number
	 * @return rule for given output field 
	 */
	public String getRule(int recNo, int fieldNo){
		Rule rule = transformMapArray[recNo][fieldNo];
		if (rule == null) {
			return null;
		}
		return getRuleTypeAsString(rule.getType()) + COLON + rule.getValue();
	}
	
	/**
	 * Gets output fields, which mapped given input field
	 * 
	 * @param inRecNo input record number
	 * @param inFieldNo input record's field number
	 * @return indexes (output record number, output field number) of fields, which mapped given input field
	 */
	public ArrayList<Integer[]> getRulesWithField(int inRecNo,int inFieldNo){
		ArrayList<Integer[]> list = new ArrayList<Integer[]>();
		Rule rule;
		for (int recNo = 0;recNo < transformMapArray.length; recNo++){
			for (int fieldNo=0;fieldNo < transformMapArray[0].length; fieldNo++){
				rule = transformMapArray[recNo][fieldNo];
				if (rule != null && rule.getType() == Rule.FIELD) {
					if (getRecNo(rule.getValue()) == inRecNo && 
							getFieldNo(rule.getValue()) == inFieldNo){
						list.add(new Integer[]{recNo, fieldNo});
					}
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
	private int maxNumFields(DataRecordMetadata[] metadata){
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
	private Integer getRecNo(String recField){
		return Integer.valueOf(recField.substring(0, recField.indexOf(DOT)));
	}
	
	/**
	 * Gets part of string after . and changed it to Integer
	 * 
	 * @param recField
	 * @return field number
	 */
	private Integer getFieldNo(String recField){
		return Integer.valueOf(recField.substring(recField.indexOf(DOT) + 1));
	}
	
	/**
	 * This method gets LENGTH and SCALE from decimal data field
	 * 
	 * @param field
	 * @return string (LENGTH,SCALE)
	 */
	private String getDecimalParams(DataFieldMetadata field){
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
	
	private String getRuleTypeAsString(int type){
		switch (type) {
		case Rule.CONSTANT:
			return Rule.CONSTANT_RULE;
		case Rule.FIELD:
			return Rule.FIELD_RULE;
		case Rule.PARAMETER:
			return Rule.PARAMETER_RULE;
		case Rule.SEQUENCE:
			return Rule.SEQUENCE_RULE;
		case Rule.DELETE:
			return Rule.DELETE_RULE;
		default:
			return "UNKNOWN_RULE";
		}
	}

	/**
	 *Private class for storing transformation rules
	 */
	private class Rule {
		
		//Types of rule
		final static int FIELD = 0;
		final static int CONSTANT = 1;
		final static int SEQUENCE = 2;
		final static int PARAMETER = 3;
		final static int DELETE = 9;
		
		final static String FIELD_RULE = "FIELD_RULE";
		final static String CONSTANT_RULE = "CONSTANT_RULE";
		final static String SEQUENCE_RULE = "SEQUENCE_RULE";
		final static String PARAMETER_RULE = "PARAMETER_RULE";
		final static String DELETE_RULE = "DELETE_RULE";
		
		int type;
		String value;
		String source;
		
		Rule(int type, String value, String source){
			this.type = type;
			this.value = value;
			this.source = source;
		}
		
		String getSource() {
			return source;
		}

		String getValue(){
			return value;
		}
		
		void setValue(String value, String source){
			this.value = value;
			this.source = source;
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
		Object getValue(Sequence sequence){
			if (sequence == null){
				return null;
			}
			int dotIndex = value.indexOf(CustomizedRecordTransform.DOT);
			String method = dotIndex > -1 ? value.substring(dotIndex +1) : "nextValueInt()";
			if (method.toLowerCase().startsWith("currentvaluestring") || method.toLowerCase().startsWith("currentstring")){
				return sequence.currentValueString();
			}
			if (method.toLowerCase().startsWith("nextvaluestring") || method.toLowerCase().startsWith("nextstring")){
				return sequence.nextValueString();
			}
			if (method.toLowerCase().startsWith("currentvalueint") || method.toLowerCase().startsWith("currentint")){
				return sequence.currentValueInt();
			}
			if (method.toLowerCase().startsWith("nextvalueint") || method.toLowerCase().startsWith("nextint")){
				return sequence.nextValueInt();
			}
			if (method.toLowerCase().startsWith("currentvaluelong") || method.toLowerCase().startsWith("currentlong")){
				return sequence.currentValueLong();
			}
			if (method.toLowerCase().startsWith("nextvaluelong") || method.toLowerCase().startsWith("nextlong")){
				return sequence.nextValueLong();
			}
			//in method validateRule checked, that has to be one of method above
			return null;
		}
		
	}

}
