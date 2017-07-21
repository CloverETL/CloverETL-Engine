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
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.jetel.data.DataField;
import org.jetel.data.DataFieldFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Numeric;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.TransformException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.primitive.MultiValueMap;
import org.jetel.util.string.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * 
 * Class used for generating data transformation. It has methods for mapping input fields on output fields, assigning
 * constants, sequence methods and parameter's values to output fields.
 * 
 * <h4>Patterns for data fields can be given in three ways:</h4>
 * <ol>
 * <li> <i>record.field</i> where <i>record</i> is number, name or wild card of input or output record and <i>field</i>
 * is name, number or wild card of <i>record's</i> data field </li>
 * <li> <i>${record.field}</i> where <i>record</i> and <i>field</i> have to be as described above</li>
 * <li> <i>${in/out.record.field}</i> where <i>record</i> and <i>field</i> have to be as described above</li>
 * </ol>
 * 
 * <h4>Order of execution/methods call</h4>
 * <ol>
 * <li>setGraph()</li>
 * <li>setFieldPolicy(PolicyType) - default value is STRICT</li>
 * <li>setUseAlternativeRules(boolean) - default is <b>false</b>
 * <li>add...Rule(...)<br> .<br>
 * </li>
 * <li>deleteRule(...)<br> .<br>
 * </li>
 * <li>init()</li>
 * <li>transform() <i>for each input&amp;output records pair</i></li>
 * <li><i>optionally</i> getMessage() <i>or</i> signal() <i>or</i> getSemiResult()</li>
 * <li>finished()
 * </ol>
 * 
 * <h4>Example:</h4>
 * <b>Records:</b>
 * 
 * <pre>
 * in:
 * #0|Name|S-&gt;  HELLO 
 * #1|Age|N-&gt;135.0
 * #2|City|S-&gt;Some silly longer string.
 * #3|Born|D-&gt;
 * #4|Value|d-&gt;-999.0000000000
 * 
 * in1:
 * #0|Name|S-&gt;  My name 
 * #1|Age|N-&gt;13.25
 * #2|City|S-&gt;Prague
 * #3|Born|D-&gt;Thu Nov 30 09:54:07 CET 2006
 * #4|Value|i-&gt;
 * out:
 * #0|Name|B-&gt;
 * #1|Age|N-&gt;
 * #2|City|S-&gt;
 * #3|Born|D-&gt;
 * #4|Value|d-&gt;
 * out1:
 * #0|Name|S-&gt;  
 * #1|Age|d-&gt;
 * #2|City|S-&gt;
 * #3|Born|D-&gt;
 * #4|Value|i-&gt;
 * </pre>
 * 
 * <b>Java code:</b>
 * 
 * <pre>
 * CustomizedRecordTransform transform = new CustomizedRecordTransform(LogFactory.getLog(this.getClass()));
 * transform.setGraph(graph);
 * transform.addFieldToFieldRule(&quot;${1.?a*}&quot;, &quot;${1.*e}&quot;);
 * transform.addFieldToFieldRule(&quot;*.City&quot;, 0, 2);
 * transform.addConstantToFieldRule(1, 3, new GregorianCalendar(1973, 3, 23).getTime());
 * transform.addConstantToFieldRule(4, &quot;1.111111111&quot;);
 * transform.addSequenceToFieldRule(&quot;*.Age&quot;, graph.getSequence(&quot;ID&quot;));
 * transform.addRule(&quot;out.City&quot;, &quot;${seq.ID.nextString}&quot;);
 * transform.addParameterToFieldRule(1, 0, &quot;${WORKSPACE}&quot;);
 * transform.addParameterToFieldRule(1, &quot;City&quot;, &quot;YourCity&quot;);
 * transform.deleteRule(1, &quot;Age&quot;);
 * transform.init(properties, new DataRecordMetadata[] { metadata, metadata1 }, new DataRecordMetadata[] { metaOut,
 * 		metaOut1 });
 * List&lt;String&gt; rules = transform.getRules();
 * System.out.println(&quot;Rules:&quot;);
 * for (Iterator&lt;String&gt; i = rules.iterator(); i.hasNext();) {
 * 	System.out.println(i.next());
 * }
 * rules = transform.getResolvedRules();
 * System.out.println(&quot;Resolved rules:&quot;);
 * for (Iterator&lt;String&gt; i = rules.iterator(); i.hasNext();) {
 * 	System.out.println(i.next());
 * }
 * List&lt;Integer[]&gt; fields = transform.getFieldsWithoutRules();
 * System.out.println(&quot;Fields without rules:&quot;);
 * Integer[] index;
 * for (Iterator&lt;Integer[]&gt; i = fields.iterator(); i.hasNext();) {
 * 	index = i.next();
 * 	System.out.println(outMatedata[index[0]].getName() + CustomizedRecordTransform.DOT
 * 			+ outMatedata[index[0]].getField(index[1]).getName());
 * }
 * fields = transform.getNotUsedFields();
 * System.out.println(&quot;Not used input fields:&quot;);
 * for (Iterator&lt;Integer[]&gt; i = fields.iterator(); i.hasNext();) {
 * 	index = i.next();
 * 	System.out.println(inMetadata[index[0]].getName() + CustomizedRecordTransform.DOT
 * 			+ inMetadata[index[0]].getField(index[1]).getName());
 * }
 * transform.transform(new DataRecord[] { record, record1 }, new DataRecord[] { out, out1 });
 * System.out.println(record.getMetadata().getName() + &quot;:\n&quot; + record.toString());
 * System.out.println(record1.getMetadata().getName() + &quot;:\n&quot; + record1.toString());
 * System.out.println(out.getMetadata().getName() + &quot;:\n&quot; + out.toString());
 * System.out.println(out1.getMetadata().getName() + &quot;:\n&quot; + out1.toString());
 * </pre>
 * 
 * <b>Output:</b>
 * 
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
 *  #0|Name|S-&gt;  HELLO 
 *  #1|Age|N-&gt;135.0
 *  #2|City|S-&gt;Some silly longer string.
 *  #3|Born|D-&gt;
 *  #4|Value|d-&gt;-999.0000000000
 *  
 *  in1:
 *  #0|Name|S-&gt;  My name 
 *  #1|Age|N-&gt;13.25
 *  #2|City|S-&gt;Prague
 *  #3|Born|D-&gt;Thu Nov 30 10:40:15 CET 2006
 *  #4|Value|i-&gt;
 *  
 *  out:
 *  #0|Name|B-&gt; 
 *  #1|Age|N-&gt;2.0
 *  #2|City|S-&gt;1
 *  #3|Born|D-&gt;
 *  #4|Value|d-&gt;1.1
 *  
 *  out1:
 *  #0|Name|S-&gt;/home/user/workspace
 *  #1|Age|d-&gt;
 *  #2|City|S-&gt;London
 *  #3|Born|D-&gt;23-04-1973
 *  #4|Value|i-&gt;
 * </pre>
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; (c) JavlinConsulting s.r.o. www.javlinconsulting.cz
 * 
 * @since Nov 16, 2006
 * @see org.jetel.component.RecordTransform
 * @see org.jetel.component.DataRecordTransform
 */

@SuppressWarnings("EI2")
public class CustomizedRecordTransform implements RecordTransform {

	protected Properties parameters;
	protected DataRecordMetadata[] sourceMetadata;
	protected DataRecordMetadata[] targetMetadata;

	protected TransformationGraph graph;
	protected Node node;

	protected PolicyType fieldPolicy = PolicyType.STRICT;
	protected boolean useAlternativeRules = false;

	protected Log logger;
	protected String errorMessage;
	protected boolean[] semiResult;
	protected boolean[][] fieldResult;

	/**
	 * Map "rules" stores rules given by user in following form: key: patternOut value: proper descendant of Rule class
	 */
	protected MultiValueMap<String, Rule> rules = new MultiValueMap<String, Rule>(new LinkedHashMap<String, List<Rule>>());
	protected Rule[][] transformMapArray;// rules from "rules" map translated for concrete metadata
	protected ArrayList<Rule[][]> alternativeTransformMapArrays;
	protected int[][] order;// order for assigning output fields (important if assigning sequence values)
	protected ArrayList<Integer[][]> alternativeOrder;

	protected static final int REC_NO = 0;
	protected static final int FIELD_NO = 1;

	protected static final char DOT = '.';
	protected static final char COLON = ':';
	protected static final char PARAMETER_CHAR = '$';
	@SuppressWarnings("MS")
	protected static final String[] WILDCARDS;
	static {
		WILDCARDS = new String[WcardPattern.WCARD_CHAR.length];
		for (int i = 0; i < WILDCARDS.length; i++) {
			WILDCARDS[i] = String.valueOf(WcardPattern.WCARD_CHAR[i]);
		}
	}

	protected final static String PARAM_OPCODE_REGEX = "\\$\\{par\\.(.*)\\}";
	protected final static Pattern PARAM_PATTERN = Pattern.compile(PARAM_OPCODE_REGEX);
	protected final static String SEQ_OPCODE_REGEX = "\\$\\{seq\\.(.*)\\}";
	protected final static Pattern SEQ_PATTERN = Pattern.compile(SEQ_OPCODE_REGEX);
	protected final static String FIELD_OPCODE_REGEX = "\\$\\{in\\.(.*)\\}";
	protected final static Pattern FIELD_PATTERN = Pattern.compile(FIELD_OPCODE_REGEX);

	private Object value;

	/**
	 * @param logger
	 */
	public CustomizedRecordTransform(Log logger) {
		this.logger = logger;
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param patternOut
	 *            output field's pattern
	 * @param patternIn
	 *            input field's pattern
	 */
	public void addFieldToFieldRule(String patternOut, String patternIn) {
		rules.putValue(patternOut, new FieldRule(patternIn));
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param recNo
	 *            output record number
	 * @param fieldNo
	 *            output record's field number
	 * @param patternIn
	 *            input field's pattern
	 */
	public void addFieldToFieldRule(int recNo, int fieldNo, String patternIn) {
		addFieldToFieldRule(String.valueOf(recNo) + DOT + fieldNo, patternIn);
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param recNo
	 *            output record number
	 * @param field
	 *            output record's field name
	 * @param patternIn
	 *            input field's pattern
	 */
	public void addFieldToFieldRule(int recNo, String field, String patternIn) {
		addFieldToFieldRule(String.valueOf(recNo) + DOT + field, patternIn);
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param patternIn
	 *            input field's pattern
	 */
	public void addFieldToFieldRule(int fieldNo, String patternIn) {
		addFieldToFieldRule(0, fieldNo, patternIn);
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param recNo
	 *            input record's number
	 * @param fieldNo
	 *            input record's field number
	 */
	public void addFieldToFieldRule(String patternOut, int recNo, int fieldNo) {
		addFieldToFieldRule(patternOut, String.valueOf(recNo) + DOT + fieldNo);
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param recNo
	 *            input record's number
	 * @param field
	 *            input record's field name
	 */
	public void addFieldToFieldRule(String patternOut, int recNo, String field) {
		addFieldToFieldRule(patternOut, String.valueOf(recNo) + DOT + field);
	}

	/**
	 * Mathod for adding field mapping rule
	 * 
	 * @param outRecNo
	 *            output record's number
	 * @param outFieldNo
	 *            output record's field number
	 * @param inRecNo
	 *            input record's number
	 * @param inFieldNo
	 *            input record's field number
	 */
	public void addFieldToFieldRule(int outRecNo, int outFieldNo, int inRecNo, int inFieldNo) {
		addFieldToFieldRule(String.valueOf(outRecNo) + DOT + outFieldNo, String.valueOf(inRecNo) + DOT + inFieldNo);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param value
	 *            value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(String patternOut, String source) {
		rules.putValue(patternOut, new ConstantRule(source));
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param value
	 *            value to assign
	 */
	public void addConstantToFieldRule(String patternOut, int value) {
		rules.putValue(patternOut, new ConstantRule(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param value
	 *            value to assign
	 */
	public void addConstantToFieldRule(String patternOut, long value) {
		rules.putValue(patternOut, new ConstantRule(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param value
	 *            value to assign
	 */
	public void addConstantToFieldRule(String patternOut, double value) {
		rules.putValue(patternOut, new ConstantRule(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param value
	 *            value to assign
	 */
	public void addConstantToFieldRule(String patternOut, Date value) {
		rules.putValue(patternOut, new ConstantRule(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param value
	 *            value to assign
	 */
	public void addConstantToFieldRule(String patternOut, Numeric value) {
		rules.putValue(patternOut, new ConstantRule(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param source
	 *            value value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, String source) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, source);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, int value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, long value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, double value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, Date value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, int fieldNo, Numeric value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field name
	 * @param source
	 *            value value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(int recNo, String field, String source) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, source);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field name
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, String field, int value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field name
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, String field, long value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field name
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, String field, double value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field name
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, String field, Date value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields rule
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field name
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int recNo, String field, Numeric value) {
		addConstantToFieldRule(String.valueOf(recNo) + DOT + field, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param source
	 *            value value to assign (can be string representation of any type)
	 */
	public void addConstantToFieldRule(int fieldNo, String source) {
		addConstantToFieldRule(0, fieldNo, source);
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, int value) {
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, long value) {
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, double value) {
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, Date value) {
		addConstantToFieldRule(0, fieldNo, value);
	}

	/**
	 * Mathod for adding constant assigning to output fields from 0th output record rule
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param value
	 *            value value to assign
	 */
	public void addConstantToFieldRule(int fieldNo, Numeric value) {
		addConstantToFieldRule(0, fieldNo, String.valueOf(value));
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param sequence
	 *            sequence ID, optionally with sequence method (can be in form ${seq.seqID}, eg. "MySequence" is the
	 *            same as "MySequence.nextIntValue()" or "${seq.MySequence.nextIntValue()}"
	 */
	public void addSequenceToFieldRule(String patternOut, String sequence) {
		String sequenceString = sequence.startsWith("${") ? sequence.substring(sequence.indexOf(DOT) + 1, sequence
				.length() - 1) : sequence;
		rules.putValue(patternOut, new SequenceRule(sequenceString));
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param sequence
	 *            sequence ID, optionally with sequence method (can be in form ${seq.seqID}, eg. "MySequence" is the
	 *            same as "MySequence.nextIntValue()" or "${seq.MySequence.nextIntValue()}"
	 */
	public void addSequenceToFieldRule(int recNo, int fieldNo, String sequence) {
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + fieldNo, sequence);
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param recNo
	 *            output record's number
	 * @param field
	 *            output record's field name
	 * @param sequence
	 *            sequence ID, optionally with sequence method (can be in form ${seq.seqID}, eg. "MySequence" is the
	 *            same as "MySequence.nextIntValue()" or "${seq.MySequence.nextIntValue()}"
	 */
	public void addSequenceToFieldRule(int recNo, String field, String sequence) {
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + field, sequence);
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields in 0th record
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param sequence
	 *            sequence ID, optionally with sequence method (can be in form ${seq.seqID}, eg. "MySequence" is the
	 *            same as "MySequence.nextIntValue()" or "${seq.MySequence.nextIntValue()}"
	 */
	public void addSequenceToFieldRule(int fieldNo, String sequence) {
		addSequenceToFieldRule(0, fieldNo, sequence);
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param sequence
	 *            sequence for getting value
	 */
	public void addSequenceToFieldRule(String patternOut, Sequence sequence) {
		rules.putValue(patternOut, new SequenceRule(sequence));
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param sequence
	 *            sequence for getting value
	 */
	public void addSequenceToFieldRule(int recNo, int fieldNo, Sequence sequence) {
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + fieldNo, sequence);
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields
	 * 
	 * @param recNo
	 *            output record's number
	 * @param field
	 *            output record's field name
	 * @param sequence
	 *            sequence for getting value
	 */
	public void addSequenceToFieldRule(int recNo, String field, Sequence sequence) {
		addSequenceToFieldRule(String.valueOf(recNo) + DOT + field, sequence);
	}

	/**
	 * Mathod for adding rule: assigning value from sequence to output fields in 0th output record
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param sequence
	 *            sequence for getting value
	 */
	public void addSequenceToFieldRule(int fieldNo, Sequence sequence) {
		addSequenceToFieldRule(0, fieldNo, sequence);
	}

	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param patternOut
	 *            output fields' pattern
	 * @param parameterName
	 *            (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(String patternOut, String parameterName) {
		if (parameterName.indexOf(DOT) > -1) {
			parameterName = parameterName.substring(parameterName.indexOf(DOT) + 1, parameterName.length() - 1);
		}
		rules.putValue(patternOut, new ParameterRule(parameterName));
	}

	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param recNo
	 *            output record's number
	 * @param fieldNo
	 *            output record's field number
	 * @param parameterName
	 *            (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(int recNo, int fieldNo, String parameterName) {
		addParameterToFieldRule(String.valueOf(recNo) + DOT + fieldNo, parameterName);
	}

	/**
	 * Mathod for adding rule: assigning parameter value to output fields
	 * 
	 * @param recNo
	 *            output record's number
	 * @param field
	 *            output record's field name
	 * @param parameterName
	 *            (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(int recNo, String field, String parameterName) {
		addParameterToFieldRule(String.valueOf(recNo) + DOT + field, parameterName);
	}

	/**
	 * Mathod for adding rule: assigning parameter value to output fields in 0th output record
	 * 
	 * @param fieldNo
	 *            output record's field number
	 * @param parameterName
	 *            (can be in form ${par.parameterName})
	 */
	public void addParameterToFieldRule(int fieldNo, String parameterName) {
		addParameterToFieldRule(0, fieldNo, parameterName);
	}

	/**
	 * Method for adding rule in CloverETL syntax This method calls proper add....Rule depending on syntax of pattern
	 * 
	 * @see org.jetel.util.CodeParser
	 * @param patternOut
	 *            output field's pattern
	 * @param pattern
	 *            rule for output field as: ${par.parameterName}, ${seq.sequenceID},
	 *            ${in.inRecordPattern.inFieldPattern}. If "pattern" doesn't much to any above, it is regarded as
	 *            constant.
	 */
	public void addRule(String patternOut, String pattern) {
		Matcher matcher = PARAM_PATTERN.matcher(pattern);
		if (matcher.find()) {
			addParameterToFieldRule(patternOut, pattern);
		} else {
			matcher = SEQ_PATTERN.matcher(pattern);
			if (matcher.find()) {
				addSequenceToFieldRule(patternOut, pattern);
			} else {
				matcher = FIELD_PATTERN.matcher(pattern);
				if (matcher.find()) {
					addFieldToFieldRule(patternOut, pattern);
				} else {
					addConstantToFieldRule(patternOut, pattern);
				}
			}
		}
	}

	/**
	 * This method deletes rule for given fields, which was set before
	 * 
	 * @param patternOut
	 *            output field pattern for deleting rule
	 */
	public void deleteRule(String patternOut) {
		rules.putValue(patternOut, new DeleteRule());
	}

	/**
	 * This method deletes rule for given field, which was set before
	 * 
	 * @param outRecNo
	 *            output record number
	 * @param outFieldNo
	 *            output record's field number
	 */
	public void deleteRule(int outRecNo, int outFieldNo) {
		String patternOut = String.valueOf(outRecNo) + DOT + outFieldNo;
		rules.putValue(patternOut, new DeleteRule());
	}

	/**
	 * This method deletes rule for given field, which was set before
	 * 
	 * @param outRecNo
	 *            output record number
	 * @param outField
	 *            output record's field name
	 */
	public void deleteRule(int outRecNo, String outField) {
		String patternOut = String.valueOf(outRecNo) + DOT + outField;
		rules.putValue(patternOut, new DeleteRule());
	}

	/**
	 * This method deletes rule for given field in 0th output record, which was set before
	 * 
	 * @param outFieldNo
	 *            output record's field number
	 */
	public void deleteRule(int outFieldNo) {
		String patternOut = String.valueOf(0) + DOT + outFieldNo;
		rules.putValue(patternOut, new DeleteRule());
	}

	/**
	 * Use postExecute method.
	 */
	@Override
	@Deprecated
	public void finished() {
	}

	/**
	 * @param graph the graph to set
	 */
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	@Override
	public TransformationGraph getGraph() {
		return (node != null) ? node.getGraph() : graph;
	}

	@Override
	public String getMessage() {
		return errorMessage;
	}

	@Override
	public Object getSemiResult() {
		return Arrays.toString(semiResult);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#init(java.util.Properties, org.jetel.metadata.DataRecordMetadata[],
	 *      org.jetel.metadata.DataRecordMetadata[])
	 */
	@Override
	public boolean init(Properties parameters, DataRecordMetadata[] sourcesMetadata, DataRecordMetadata[] targetMetadata)
			throws ComponentNotReadyException {
		if (sourcesMetadata == null || targetMetadata == null) {
			return false;
		}
		this.parameters = parameters;
		this.sourceMetadata = sourcesMetadata;
		this.targetMetadata = targetMetadata;
		semiResult = new boolean[targetMetadata.length];
		fieldResult = new boolean[targetMetadata.length][];
		for (int i = 0; i < fieldResult.length; i++) {
			fieldResult[i] = new boolean[targetMetadata[i].getNumFields()];
		}
		return init();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#preExecute()
	 */
	@Override
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#postExecute(org.jetel.graph.TransactionMethod)
	 */
	@Override
	public void postExecute() throws ComponentNotReadyException {
	}
	
	/**
	 * Checks if given string contans wild cards
	 * 
	 * @see WcardPattern
	 * 
	 * @param str
	 * @return
	 */
	private boolean containsWCard(String str) {
		for (int i = 0; i < WILDCARDS.length; i++) {
			if (str.contains(WILDCARDS[i])) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Method with initialize user customized transformation with concrete metadata
	 * 
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private boolean init() throws ComponentNotReadyException {
		// map storing transformation for concrete output fields
		// key is in form: recNumber.fieldNumber
		Map<String, Rule> transformMap = new LinkedHashMap<String, Rule>();
		ArrayList<Map<String, Rule>> alternativeTransformMaps = new ArrayList<Map<String, Rule>>();
		Entry<String, List<Rule>> rulesEntry;
		String field;
		String ruleString = null;
		String[] outFields;
		String[] inFields;
		// iteration over each user given rule
		for (Iterator<Entry<String, List<Rule>>> iterator = rules.entrySet().iterator(); iterator.hasNext();) {
			rulesEntry = iterator.next();
			for (Rule rule : rulesEntry.getValue()) {
				rule.setGraph(getGraph());
				rule.setLogger(logger);
				rule.setProperties(parameters);
				// find output fields pattern
				field = resolveField(rulesEntry.getKey());
				if (field == null) {
					throwComponentNotReadyException("Wrong pattern for output fields: " + rulesEntry.getKey());
				}
				// find output fields from pattern
				final ArrayList<String> outFieldsList = findFields(field, targetMetadata);
				outFields = outFieldsList.toArray(new String[outFieldsList.size()]);
				if (outFields.length == 0) {
					throwComponentNotReadyException("There is no output field matching \"" + field + "\" pattern");
				}
				inFields = new String[0];
				if (rule instanceof DeleteRule) {
					for (int i = 0; i < outFields.length; i++) {
						rule = transformMap.remove(outFields[i]);
					}
					continue;
				}
				if (rule instanceof FieldRule) {
					// find input fields from pattern
					ruleString = resolveField(rule.getSource());
					if (ruleString == null) {
						throwComponentNotReadyException(errorMessage = "Wrong pattern for output fields: " + rule.getSource());
					}
					final ArrayList<String> inFieldsList = findFields(ruleString, sourceMetadata);
					inFields = inFieldsList.toArray(new String[inFieldsList.size()]);
					if (inFields.length == 0) {
						throwComponentNotReadyException("There is no input field matching \"" + ruleString + "\" pattern");
					}
				}
				if (rule instanceof FieldRule && inFields.length > 1) {
					// find mapping by names
					if (putMappingByNames(transformMap, alternativeTransformMaps, outFields, inFields, rule.getSource()) == 0) {
						if (!useAlternativeRules) {
							warn("Not found any field for mapping by names due to rule:\n" + field
									+ " - output fields pattern\n" + ruleString + " - input fields pattern");
						}
					}
				} else {// for each output field the same rule
					// for each output field from pattern, put rule to map
					for (int i = 0; i < outFields.length; i++) {
						if (!containsWCard(field) || !transformMap.containsKey(outFields[i])) {// check primary map
							transformMap.put(outFields[i], rule.duplicate());
						} else if (useAlternativeRules) {// rule is in primery map --> put to alternative map
							putRuleInAlternativeMap(outFields[i], rule, alternativeTransformMaps);
						}
					}
				}
			}
		}
		// changing map to array
		transformMapArray = new Rule[targetMetadata.length][maxNumFields(targetMetadata)];
		order = new int[transformMap.size()][2];
		int index = 0;
		for (Entry<String, Rule> i : transformMap.entrySet()) {
			field = i.getKey();
			order[index][REC_NO] = getRecNo(field);
			order[index][FIELD_NO] = getFieldNo(field);
			transformMapArray[order[index][REC_NO]][order[index][FIELD_NO]] = i.getValue();
			transformMapArray[order[index][REC_NO]][order[index][FIELD_NO]].init(sourceMetadata, targetMetadata,
					getRecNo(field), getFieldNo(field), fieldPolicy);
			index++;
		}
		// create and initialize alternative rules
		if (useAlternativeRules && alternativeTransformMaps.size() > 0) {
			alternativeTransformMapArrays = new ArrayList<Rule[][]>(alternativeTransformMaps.size());
			alternativeOrder = new ArrayList<Integer[][]>(alternativeTransformMaps.size());
			for (Map<String, Rule> map : alternativeTransformMaps) {
				Rule[][] ruleArray = new Rule[targetMetadata.length][maxNumFields(targetMetadata)];
				alternativeTransformMapArrays.add(ruleArray);
				Integer[][] orderArray = new Integer[map.size()][2];
				alternativeOrder.add(orderArray);
				index = 0;
				for (Entry<String, Rule> i : map.entrySet()) {
					field = i.getKey();
					order[index][REC_NO] = getRecNo(field);
					order[index][FIELD_NO] = getFieldNo(field);
					ruleArray[order[index][REC_NO]][order[index][FIELD_NO]] = i.getValue();
					ruleArray[order[index][REC_NO]][order[index][FIELD_NO]].init(sourceMetadata, targetMetadata,
							getRecNo(field), getFieldNo(field), fieldPolicy);
					index++;
				}
			}
		}
		return true;
	}

	/**
	 * This method puts rule to the alternative map, which doesn't contain rule for this field. If all alternative maps
	 * contain rule for requested field, new map is created and added to list
	 * 
	 * @param field
	 *            output field
	 * @param rule
	 *            rule to put
	 * @param alternativeTransformMaps
	 *            list of alternative maps
	 * @return
	 */
	private void putRuleInAlternativeMap(String field, Rule rule, List<Map<String, Rule>> alternativeTransformMaps) {
		for (Map<String, Rule> map : alternativeTransformMaps) {
			if (!map.containsKey(field)) {
				map.put(field, rule.duplicate());
				return;
			}
		}
		// all maps checked --> create new one
		Map<String, Rule> newMap = new LinkedHashMap<String, Rule>();
		alternativeTransformMaps.add(newMap);
		newMap.put(field, rule.duplicate());
	}

	/**
	 * Method, which puts mapping rules to map. First it tries to find fields with identical names in corresponding
	 * input and output metadata. If not all output fields were found it tries to find them in other input records. If
	 * not all output fields were found it tries to find fields with the same names ignoring case in corresponding
	 * record. If still there are some output fields without paire it tries to find fields with the same name ignoring
	 * case in other records, eg.<br>
	 * outFieldsNames:<br>
	 * <ul>
	 * <li>lname, fname, address, phone</li>
	 * <li>Lname, fname, id</li>
	 * </ul>
	 * inFieldsNames:
	 * <ul>
	 * <li>Lname, fname, id, address</li>
	 * <li>lname, fname, phone</li>
	 * </ul>
	 * Mapping:
	 * <ul>
	 * <li>0.0 <-- 1.0</li>
	 * <li>0.1 <-- 0.1</li>
	 * <li>0.2 <-- 0.3</li>
	 * <li>0.3 <-- 1.2</li>
	 * <li>1.0 <-- 0.0</li>
	 * <li>1.1 <-- 1.1</li>
	 * <li>1.2 <-- 0.2</li>
	 * </ul>
	 * 
	 * @param transformMap
	 *            map to put rules
	 * @param alternativeMaps
	 *            list of alternative maps, used if useAlternativeRules=true and primery map contains output field
	 * @param outFields
	 *            output fields to mapping
	 * @param inFields
	 *            input fields for mapping
	 * @return number of mappings put to transform map
	 */
	protected int putMappingByNames(Map<String, Rule> transformMap, List<Map<String, Rule>> alternativeMaps,
			String[] outFields, String[] inFields, String rule) throws ComponentNotReadyException {
		int count = 0;
		String[][] outFieldsName = new String[targetMetadata.length][maxNumFields(targetMetadata)];
		for (int i = 0; i < outFields.length; i++) {
			outFieldsName[getRecNo(outFields[i])][getFieldNo(outFields[i])] = targetMetadata[getRecNo(outFields[i])]
					.getField(getFieldNo(outFields[i])).getName();
		}
		String[][] inFieldsName = new String[sourceMetadata.length][maxNumFields(sourceMetadata)];
		for (int i = 0; i < inFields.length; i++) {
			inFieldsName[getRecNo(inFields[i])][getFieldNo(inFields[i])] = sourceMetadata[getRecNo(inFields[i])]
					.getField(getFieldNo(inFields[i])).getName();
		}
		int index;
		// find identical in corresponding records
		for (int i = 0; (i < outFieldsName.length) && (i < inFieldsName.length); i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				if (outFieldsName[i][j] != null) {
					index = StringUtils.findString(outFieldsName[i][j], inFieldsName[i]);
					if (index > -1) {// output field name found amoung input fields
						if (putMapping(i, j, i, index, rule, transformMap, alternativeMaps)) {
							count++;
							outFieldsName[i][j] = null;
							inFieldsName[i][index] = null;
						}
					}
				}
			}
		}
		// find identical in other records
		for (int i = 0; i < outFieldsName.length; i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				for (int k = 0; k < inFieldsName.length; k++) {
					if ((outFieldsName[i][j] != null) && (k != i)) {
						index = StringUtils.findString(outFieldsName[i][j], inFieldsName[k]);
						if (index > -1) {// output field name found amoung input fields
							if (putMapping(i, j, k, index, rule, transformMap, alternativeMaps)) {
								count++;
								outFieldsName[i][j] = null;
								inFieldsName[k][index] = null;
							}
						}
					}
				}
			}
		}
		// find ignore case in corresponding records
		for (int i = 0; (i < outFieldsName.length) && (i < inFieldsName.length); i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				if (outFieldsName[i][j] != null) {
					index = StringUtils.findStringIgnoreCase(outFieldsName[i][j], inFieldsName[i]);
					if (index > -1) {// output field name found amoung input fields
						if (putMapping(i, j, i, index, rule, transformMap, alternativeMaps)) {
							count++;
							outFieldsName[i][j] = null;
							inFieldsName[i][index] = null;
						}
					}
				}
			}
		}
		// find ignore case in other records
		for (int i = 0; i < outFieldsName.length; i++) {
			for (int j = 0; j < outFieldsName[i].length; j++) {
				for (int k = 0; k < inFieldsName.length; k++) {
					if ((outFieldsName[i][j] != null) && (k != i)) {
						index = StringUtils.findStringIgnoreCase(outFieldsName[i][j], inFieldsName[k]);
						if (index > -1) {// output field name found amoung input fields
							if (putMapping(i, j, k, index, rule, transformMap, alternativeMaps)) {
								count++;
								outFieldsName[i][j] = null;
								inFieldsName[k][index] = null;
							}
						}
					}
				}
			}
		}
		return count;
	}

	/**
	 * This method puts mapping into given output and input field to transform map if theese fields have correct types
	 * due to policy type
	 * 
	 * @param outRecNo
	 *            number of record from output metadata
	 * @param outFieldNo
	 *            number of field from output metadata
	 * @param inRecNo
	 *            number of record from input metadata
	 * @param inFieldNo
	 *            number of field from input metadata
	 * @param transformMap
	 * @param alternativeMaps
	 *            list of alternative maps, used if useAlternativeRules=true and primery map contains output field
	 * @return true if mapping was put into map, false in other case
	 */
	protected boolean putMapping(int outRecNo, int outFieldNo, int inRecNo, int inFieldNo, String ruleString,
			Map<String, Rule> transformMap, List<Map<String, Rule>> alternativeMaps) throws ComponentNotReadyException {
		if (!Rule.checkTypes(targetMetadata[outRecNo].getField(outFieldNo),
				sourceMetadata[inRecNo].getField(inFieldNo), fieldPolicy)) {
			if (fieldPolicy == PolicyType.STRICT) {
// this warning was removed due Casenet project - in future should be still present and better manageable
//				if (logger != null) {
//					logger.warn("Found fields with the same names but other types: ");
//					logger.warn(targetMetadata[outRecNo].getName() + DOT + 
//							targetMetadata[outRecNo].getField(outFieldNo).getName() + " type - " + 
//							targetMetadata[outRecNo].getFieldTypeAsString(outFieldNo) + 
//							getDecimalParams(targetMetadata[outRecNo].getField(outFieldNo)));
//					logger.warn(sourceMetadata[inRecNo].getName() + DOT + 
//							sourceMetadata[inRecNo].getField(inFieldNo).getName() + " type - " + 
//							sourceMetadata[inRecNo].getFieldTypeAsString(inFieldNo) + 
//							getDecimalParams(sourceMetadata[inRecNo].getField(inFieldNo)));
//				}
			}
			if (fieldPolicy == PolicyType.CONTROLLED) {
				if (logger !=null ) {
					logger.warn("Found fields with the same names but incompatible types: ");
					logger.warn(targetMetadata[outRecNo].getName() + DOT
							+ targetMetadata[outRecNo].getField(outFieldNo).getName() + " type - "
							+ targetMetadata[outRecNo].getFieldTypeAsString(outFieldNo)
							+ getDecimalParams(targetMetadata[outRecNo].getField(outFieldNo)));
					logger.warn(sourceMetadata[inRecNo].getName() + DOT
							+ sourceMetadata[inRecNo].getField(inFieldNo).getName() + " type - "
							+ sourceMetadata[inRecNo].getFieldTypeAsString(inFieldNo)
							+ getDecimalParams(sourceMetadata[inRecNo].getField(inFieldNo)));
				}
			}
			return false;
		} else {// map fields
			FieldRule rule = new FieldRule(ruleString);
			rule.setFieldParams(String.valueOf(inRecNo) + DOT + inFieldNo);
			if (!transformMap.containsKey(String.valueOf(outRecNo) + DOT + outFieldNo)) {
				transformMap.put(String.valueOf(outRecNo) + DOT + outFieldNo, rule);
			} else if (useAlternativeRules) {
				putRuleInAlternativeMap(String.valueOf(outRecNo) + DOT + outFieldNo, rule, alternativeMaps);
			}
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
	public static ArrayList<String> findFields(String pattern, DataRecordMetadata[] metadata) {
		ArrayList<String> list = new ArrayList<String>();
		String recordNoString = pattern.substring(0, pattern.indexOf(DOT));
		String fieldNoString = pattern.substring(pattern.indexOf(DOT) + 1);
		int recNo;
		try {// check if first part of pattern is "real" pattern or number of record
			recNo = Integer.parseInt(recordNoString);
			try {// we have one record Number
				// check if second part of pattern is "real" pattern or number of field
				Integer.parseInt(fieldNoString);
				// we have one record field number
				list.add(pattern);
			} catch (NumberFormatException e) {// second part of pattern is not a number
				// find matching fields
				for (int i = 0; i < metadata[recNo].getNumFields(); i++) {
					if (WcardPattern.checkName(fieldNoString, metadata[recNo].getField(i).getName())) {
						list.add(String.valueOf(recNo) + DOT + i);
					}
				}
			}
		} catch (NumberFormatException e) {// first part of pattern is not a number
			// check all matadata names if match pattern
			for (int i = 0; i < metadata.length; i++) {
				if (WcardPattern.checkName(recordNoString, metadata[i].getName())) {
					try {// check if second part of pattern is "real" pattern or number of field
						Integer.parseInt(fieldNoString);
						// we have matching metadata name and field number
						list.add(String.valueOf(i) + DOT + fieldNoString);
					} catch (NumberFormatException e1) {// second part of pattern is not a number
						// find matching fields
						for (int j = 0; j < metadata[i].getNumFields(); j++) {
							if (WcardPattern.checkName(fieldNoString, metadata[i].getField(j).getName())) {
								list.add(String.valueOf(i) + DOT + j);
							}
						}
					}
				}
			}
		}
		return list;
	}

	@Override
	public Node getNode() {
		return node;
	}

	@Override
	public void setNode(Node node) {
		this.node = node;
	}

	@Override
	public void signal(Object signalObject) {
		// TODO Auto-generated method stub

	}

	/**
	 * Fills error message and throws exception
	 * 
	 * @param ruleArray
	 * @param recNo
	 * @param FieldNo
	 * @param ex
	 * @throws TransformException
	 */
	private void error(Rule[][] ruleArray, int recNo, int FieldNo, Exception ex) throws TransformException {
		errorMessage = "TransformException caused by source: " + ruleArray[recNo][FieldNo].getSource();
		if (logger != null) {
			logger.error(errorMessage, ex);
		}
//		throw new TransformException(errorMessage, ex, recNo, FieldNo);
	}

	private void throwComponentNotReadyException(String message) throws ComponentNotReadyException{
		errorMessage = message;
		if (logger != null) {
			logger.error(errorMessage);
		}
		throw new ComponentNotReadyException(errorMessage);
	}
	
	private void warn(String message){
		errorMessage = message;
		if (logger != null) {
			logger.warn(errorMessage);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordTransform#transform(org.jetel.data.DataRecord[], org.jetel.data.DataRecord[])
	 */
	@Override
	public int transform(DataRecord[] sources, DataRecord[] target) throws TransformException {
		for (boolean[] index : fieldResult) {
			Arrays.fill(index, true);
		}
		// array "order" stores coordinates of output fields in order they will be assigned
		for (int i = 0; i < order.length; i++) {
			value = transformMapArray[order[i][REC_NO]][order[i][FIELD_NO]].getValue(sources);
			if (value != null || !useAlternativeRules) {
				try {
					target[order[i][REC_NO]].getField(order[i][FIELD_NO]).setValue(value);
				} catch (BadDataFormatException e) {
					// we can try to change value to String and set to output field
					if (value != null && fieldPolicy != PolicyType.STRICT) {
						try {
							target[order[i][REC_NO]].getField(order[i][FIELD_NO]).fromString(value.toString());
						} catch (BadDataFormatException e1) {
							if (!useAlternativeRules
									|| !setAlternativeValue(sources, target, order[i][REC_NO], order[i][FIELD_NO], 0, e1)) {
								error(transformMapArray, order[i][REC_NO], order[i][FIELD_NO], e1);
								fieldResult[order[i][REC_NO]][order[i][FIELD_NO]] = false;
							}
						}
					} else if (!useAlternativeRules
							|| !setAlternativeValue(sources, target, order[i][REC_NO], order[i][FIELD_NO], 0, e)) {// value is null or value can't be set to field
						error(transformMapArray, order[i][REC_NO], order[i][FIELD_NO], e);
						fieldResult[order[i][REC_NO]][order[i][FIELD_NO]] = false;
					}
				}
			} else {// value is null and useuseAlternativeRules = true
				fieldResult[order[i][REC_NO]][order[i][FIELD_NO]] = setAlternativeValue(sources, target, order[i][REC_NO], order[i][FIELD_NO], 0, null);
			}
		}
		//fill semiresult for each record  
		Arrays.fill(semiResult, true);
	semiResultLoop:
		for (int i = 0; i < semiResult.length; i++){
			for (int j = 0; j < fieldResult[i].length; j++){
				if (!fieldResult[i][j]) {
					semiResult[i] = false;
					continue semiResultLoop;
				}
			}
		}
		//find first fully transformed record, if not all records were transformed succesfully
		int result = -1;
		boolean allOk = true;
		for (int i = 0; i < semiResult.length; i++){
			if (result == -1 && semiResult[i]) {//found first succesfully transformed record
				result = i;
			}
			allOk = allOk && semiResult[i];
			if (!allOk && result != -1) {//not all records transformed succesfully, but one for sure
				return result;
			}
		}
		return allOk ? ALL : SKIP;
	}

	@Override
	public int transformOnError(Exception exception, DataRecord[] sources, DataRecord[] target)
			throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Transform failed!", exception);
	}

	/**
	 * Tries to set value due to given alternative rule
	 * 
	 * @param sources
	 *            source records
	 * @param target
	 *            target records
	 * @param trgRec
	 *            target record number
	 * @param trgField
	 *            target field number
	 * @param alternativeRuleNumber
	 * @param cause
	 *            why we call alternative rule
	 * @return true if value was set, in other case throws TransformException
	 * @throws TransformException
	 */
	private boolean setAlternativeValue(DataRecord[] sources, DataRecord[] target, int trgRec, int trgField,
			int alternativeRuleNumber, Exception cause) throws TransformException {
		Rule[][] ruleArray = alternativeTransformMapArrays.get(alternativeRuleNumber);
		if (ruleArray[trgRec][trgField] == null) {
			error(ruleArray, trgRec, trgField, cause);
			return false;
		}
		value = ruleArray[trgRec][trgField].getValue(sources);
		if (value != null) {
			try {
				target[trgRec].getField(trgField).setValue(value);
				return true;
			} catch (BadDataFormatException e) {
				if (fieldPolicy != PolicyType.STRICT) {
					try {
						target[trgRec].getField(trgField).fromString(value.toString());
						return true;
					} catch (BadDataFormatException e1) {
						if (++alternativeRuleNumber < alternativeTransformMapArrays.size()) {
							return setAlternativeValue(sources, target, trgRec, trgField, alternativeRuleNumber, e1);
						} else {
							error(ruleArray, trgRec, trgField, e1);
							return false;
						}
					}
				} else if (++alternativeRuleNumber < alternativeTransformMapArrays.size()) {
					return setAlternativeValue(sources, target, trgRec, trgField, alternativeRuleNumber, e);
				} else {
					error(ruleArray, trgRec, trgField, e);
					return false;
				}
			}
		} else if (++alternativeRuleNumber < alternativeTransformMapArrays.size()) {
			return setAlternativeValue(sources, target, trgRec, trgField, alternativeRuleNumber, cause);
		} else {// value is null
			try {
				target[trgRec].getField(trgField).setValue(value);
				return true;
			} catch (BadDataFormatException e) {
				error(ruleArray, trgRec, trgField, e);
				return false;
			}
		}
	}

	/**
	 * Changes pattern given in one of possible format to record.field
	 * 
	 * @param pattern
	 * @return pattern in format record.field of null if it is not possible
	 */
	public static String resolveField(String pattern) {
		String[] parts = pattern.split("\\.");
		switch (parts.length) {
		case 2:
			if (parts[0].startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {// ${recNo.field}
				return parts[0].substring(2) + DOT + parts[1].substring(0, parts[1].length() - 1);
			} else {// recNo.field
				return pattern;
			}
		case 3:
			if (parts[0].startsWith(Defaults.CLOVER_FIELD_INDICATOR)) {// ${in/out.recNo.field}
				return parts[1] + DOT + parts[2].substring(0, parts[2].length() - 1);
			} else {// in/out.recNo.field
				return parts[1] + DOT + parts[2];
			}
		default:
			return null;
		}
	}

	/**
	 * Gets rule in form they were set by user
	 * 
	 * @return rules
	 */
	public ArrayList<String> getRulesAsStrings() {
		ArrayList<String> list = new ArrayList<String>();
		Entry<String, List<Rule>> entry;
		List<Rule> subList;
		for (Iterator<Entry<String, List<Rule>>> iterator = rules.entrySet().iterator(); iterator.hasNext();) {
			entry = iterator.next();
			subList = entry.getValue();
			for (int i = 0; i < subList.size(); i++) {
				list.add(subList.get(i).getType() + ":" + entry.getKey() + "=" + subList.get(i).getSource());
			}
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
		String value;
		for (int recNo = 0; recNo < transformMapArray.length; recNo++) {
			for (int fieldNo = 0; fieldNo < transformMapArray[0].length; fieldNo++) {
				if (transformMapArray[recNo][fieldNo] != null) {
					value = transformMapArray[recNo][fieldNo].getCanonicalSource() != null ? transformMapArray[recNo][fieldNo]
							.getCanonicalSource().toString()
							: "null";
					list.add(targetMetadata[recNo].getName() + DOT + targetMetadata[recNo].getField(fieldNo).getName()
							+ "=" + value);
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
	public ArrayList<Integer[]> getFieldsWithoutRules() {
		ArrayList<Integer[]> list = new ArrayList<Integer[]>();
		for (int recNo = 0; recNo < transformMapArray.length; recNo++) {
			for (int fieldNo = 0; fieldNo < transformMapArray[0].length; fieldNo++) {
				if (fieldNo < targetMetadata[recNo].getNumFields() && transformMapArray[recNo][fieldNo] == null) {
					list.add(new Integer[] { recNo, fieldNo });
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
	public ArrayList<Integer[]> getNotUsedFields() {
		ArrayList<String> inFieldsList = findFields("*.*", sourceMetadata);
		String[] inFields = inFieldsList.toArray(new String[inFieldsList.size()]);
		Rule rule;
		int index;
		String field;
		for (int recNo = 0; recNo < transformMapArray.length; recNo++) {
			for (int fieldNo = 0; fieldNo < transformMapArray[0].length; fieldNo++) {
				rule = transformMapArray[recNo][fieldNo];
				if (rule != null && rule instanceof FieldRule) {
					field = (String) rule.getCanonicalSource();
					index = StringUtils.findString(field, inFields);
					if (index != -1) {
						inFields[index] = null;
					}
				}
			}
		}
		ArrayList<Integer[]> list = new ArrayList<Integer[]>();
		for (int i = 0; i < inFields.length; i++) {
			if (inFields[i] != null) {
				list.add(new Integer[] { getRecNo(inFields[i]), getFieldNo(inFields[i]) });
			}
		}
		return list;
	}

	/**
	 * Gets rule for given output field
	 * 
	 * @param recNo
	 *            output record number
	 * @param fieldNo
	 *            output record's field number
	 * @return rule for given output field
	 */
	public String getRule(int recNo, int fieldNo) {
		Rule rule = transformMapArray[recNo][fieldNo];
		if (rule == null) {
			return null;
		}
		return (rule.getType()) + COLON + rule.getCanonicalSource();
	}

	/**
	 * Gets output fields, which mapped given input field
	 * 
	 * @param inRecNo
	 *            input record number
	 * @param inFieldNo
	 *            input record's field number
	 * @return indexes (output record number, output field number) of fields, which mapped given input field
	 */
	public ArrayList<Integer[]> getRulesWithField(int inRecNo, int inFieldNo) {
		ArrayList<Integer[]> list = new ArrayList<Integer[]>();
		Rule rule;
		for (int recNo = 0; recNo < transformMapArray.length; recNo++) {
			for (int fieldNo = 0; fieldNo < transformMapArray[0].length; fieldNo++) {
				rule = transformMapArray[recNo][fieldNo];
				if (rule != null && rule instanceof FieldRule) {
					if (getRecNo((String) rule.getCanonicalSource()) == inRecNo
							&& getFieldNo((String) rule.getCanonicalSource()) == inFieldNo) {
						list.add(new Integer[] { recNo, fieldNo });
					}
				}
			}
		}
		return list;
	}

	public PolicyType getFieldPolicy() {
		return fieldPolicy;
	}

	/**
	 * Sets the field policy:
	 * <ul>
	 * <li>PolicyType.STRICT - mapped output and input fields have to be of the same types
	 * <li>PolicyType.CONTROLLED - mapped input fields have to be subtypes of output fields<br>
	 * <li>PolicyType.LENIENT - field's types are not checked during initialization
	 * </ul>
	 * For PolicyType CONTROLLED and LENIENT method transform can work slower as for not identical types for is called
	 * method fromString, when method setValue has failed.
	 * 
	 * @param fieldPolicy
	 */
	public void setFieldPolicy(PolicyType fieldPolicy) {
		this.fieldPolicy = fieldPolicy;
	}

	/**
	 * Finds maximal length of metadata
	 * 
	 * @param metadata
	 * @return maximal length of metadatas
	 */
	private int maxNumFields(DataRecordMetadata[] metadata) {
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
	 * @param recField
	 *            recNo.FieldNo
	 * @return record number
	 */
	public static Integer getRecNo(String recField) {
		return Integer.valueOf(recField.substring(0, recField.indexOf(DOT)));
	}

	/**
	 * Gets part of string after . and changed it to Integer
	 * 
	 * @param recField
	 * @return field number
	 */
	public static Integer getFieldNo(String recField) {
		return Integer.valueOf(recField.substring(recField.indexOf(DOT) + 1));
	}

	/**
	 * This method gets LENGTH and SCALE from decimal data field
	 * 
	 * @param field
	 * @return string (LENGTH,SCALE)
	 */
	public static String getDecimalParams(DataFieldMetadata field) {
		if (field.getDataType() != DataFieldType.DECIMAL) {
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
	 * Use preExecute method.
	 */
	@Override
	@Deprecated
	public void reset() {
		errorMessage = null;
	}

	public void setLogger(Log logger) {
		this.logger = logger;
	}

	/**
	 * @return if transformation uses alternative rules
	 * 
	 * @see setUseAlternativeRules
	 */
	public boolean isUseAlternativeRules() {
		return useAlternativeRules;
	}

	/**
	 * Switchs on/off alternative rules. When alternative rules are switched on, more rules can be used for one field:
	 * if value from primery rule is null or transformations failed trying to set it, there are used lternative rules.
	 * Tranasformation fails if value of noone rule can be set to requested field.
	 * 
	 * @param useAlternativeRules
	 */
	public void setUseAlternativeRules(boolean useAlternativeRules) {
		this.useAlternativeRules = useAlternativeRules;
	}

}// class CustomizedRecordTransform

/**
 * Private class for storing transformation rules
 */
abstract class Rule {

	Object value;
	String source;
	String errorMessage;
	Log logger;
	TransformationGraph graph;
	Properties parameters;

	Rule(String source) {
		this.source = source;
	}

	Rule(Object value) {
		this.value = value;
	}

	String getSource() {
		return source;
	}

	public void setLogger(Log logger) {
		this.logger = logger;
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public void setProperties(Properties parameters) {
		this.parameters = parameters;
	}

	protected void warn(String message){
		errorMessage = message;
		if (logger != null) {
			logger.warn(message);
		}
	}
	
	protected void error(String message) throws ComponentNotReadyException{
		errorMessage = message;
		if (logger != null) {
			logger.error(message);
		}
		throw new ComponentNotReadyException(message);
	}
	
	abstract Rule duplicate();

	abstract String getType();

	abstract Object getCanonicalSource();

	/**
	 * Gets value for setting to data field
	 * 
	 * @param sources
	 *            source data record (used only in Field rule)
	 * @return value to be set to data field
	 */
	abstract Object getValue(DataRecord[] sources);

	/**
	 * Prepares rule (source, value and check if value can be got) for getting values in transform method of
	 * CustomizedRecordTransform class
	 * 
	 * @param sourceMetadata
	 * @param targetMetadata
	 * @param recNo
	 *            output metadata number (from targetMetadata)
	 * @param fieldNo
	 *            output field number
	 * @param policy
	 *            field policy
	 * @throws ComponentNotReadyException
	 */
	abstract void init(DataRecordMetadata[] sourceMetadata, DataRecordMetadata[] targetMetadata, int recNo,
			int fieldNo, PolicyType policy) throws ComponentNotReadyException;

	/**
	 * This method checks if input field is subtype of output type
	 * 
	 * @param outRecNo
	 *            output record number
	 * @param outFieldNo
	 *            output record's field number
	 * @param inRecNo
	 *            input record number
	 * @param inFieldNo
	 *            input record's field number
	 * @return "true" if input field is subtype of output field, "false" in other cases
	 */
	public static boolean checkTypes(DataFieldMetadata outField, DataFieldMetadata inField, PolicyType policy) {
		boolean checkTypes;
		// check if both fields are of type DECIMAL, if yes inField must be subtype of outField
		if (outField.getDataType() == inField.getDataType()) {
			if (outField.getDataType() == DataFieldType.DECIMAL) {
				checkTypes = inField.isSubtype(outField);
			} else {
				checkTypes = true;
			}
		} else {
			checkTypes = false;
		}
		if (policy == PolicyType.STRICT && !checkTypes) {
			return false;
		} else if (policy == PolicyType.CONTROLLED && !inField.isSubtype(outField)) {
			return false;
		}
		return true;
	}
}

/**
 * Descendent of Rule class for storing field's mapping rule
 */
class FieldRule extends Rule {

	String fieldParams;// "recNo.fieldNo" = "resolved source" - it have to be set by setFieldParams method

	FieldRule(String source) {
		super(source);
	}

	@Override
	void init(DataRecordMetadata[] sourceMetadata, DataRecordMetadata[] targetMetadata, int recNo, int fieldNo,
			PolicyType policy) throws ComponentNotReadyException {
		if (fieldParams == null) {
			// try find ONE field in source metadata matching source
			fieldParams = CustomizedRecordTransform.resolveField(source);
			ArrayList<String> tmp = CustomizedRecordTransform.findFields(fieldParams, sourceMetadata);
			if (tmp.size() != 1) {
				throw new ComponentNotReadyException("Field parameters are "
						+ "not set and can't be resolved from source: " + source);
			}
			fieldParams = tmp.get(0);
		}
		// check input and output fields types
		if (!checkTypes(targetMetadata[recNo].getField(fieldNo), sourceMetadata[CustomizedRecordTransform
				.getRecNo(fieldParams)].getField(CustomizedRecordTransform.getFieldNo(fieldParams)), policy)) {
			if (policy == PolicyType.STRICT) {
				errorMessage = "Output field type does not match input field "
						+ "type:\n"
						+ targetMetadata[recNo].getName()
						+ CustomizedRecordTransform.DOT
						+ targetMetadata[recNo].getField(fieldNo).getName()
						+ " type - "
						+ targetMetadata[recNo].getField(fieldNo).getDataType().getName()
						+ CustomizedRecordTransform.getDecimalParams(targetMetadata[recNo].getField(fieldNo))
						+ "\n"
						+ sourceMetadata[CustomizedRecordTransform.getRecNo(fieldParams)].getName()
						+ CustomizedRecordTransform.DOT
						+ sourceMetadata[CustomizedRecordTransform.getRecNo(fieldParams)].getField(
								CustomizedRecordTransform.getFieldNo(fieldParams)).getName()
						+ " type - "
						+ sourceMetadata[CustomizedRecordTransform.getRecNo(fieldParams)].getField(
								CustomizedRecordTransform.getFieldNo(fieldParams)).getDataType().getName()
						+ CustomizedRecordTransform.getDecimalParams(sourceMetadata[CustomizedRecordTransform
								.getRecNo(fieldParams)].getField(CustomizedRecordTransform.getFieldNo(fieldParams)));
				error(errorMessage);
			}
			if (policy == PolicyType.CONTROLLED) {
				errorMessage = "Output field type is not compatible with input field "
						+ "type:\n"
						+ targetMetadata[recNo].getName()
						+ CustomizedRecordTransform.DOT
						+ targetMetadata[recNo].getField(fieldNo).getName()
						+ " type - "
						+ targetMetadata[recNo].getField(fieldNo).getDataType().getName()
						+ CustomizedRecordTransform.getDecimalParams(targetMetadata[recNo].getField(fieldNo))
						+ "\n"
						+ sourceMetadata[CustomizedRecordTransform.getRecNo(fieldParams)].getName()
						+ CustomizedRecordTransform.DOT
						+ sourceMetadata[CustomizedRecordTransform.getRecNo(fieldParams)].getField(
								CustomizedRecordTransform.getFieldNo(fieldParams)).getName()
						+ " type - "
						+ sourceMetadata[CustomizedRecordTransform.getRecNo(fieldParams)].getField(
								CustomizedRecordTransform.getFieldNo(fieldParams)).getDataType().getName()
						+ CustomizedRecordTransform.getDecimalParams(sourceMetadata[CustomizedRecordTransform
								.getRecNo(fieldParams)].getField(CustomizedRecordTransform.getFieldNo(fieldParams)));
				error(errorMessage);
			}
		}
	}

	public void setFieldParams(String fieldParams) {
		this.fieldParams = fieldParams;
	}

	@Override
	Object getCanonicalSource() {
		return fieldParams;
	}

	@Override
	String getType() {
		return "FIELD_RULE";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.Rule#getValue(org.jetel.data.DataRecord[])
	 * 
	 */
	@Override
	Object getValue(DataRecord[] sources) {
		int dotIndex = fieldParams.indexOf(CustomizedRecordTransform.DOT);
		int recNo = dotIndex > -1 ? Integer.parseInt(fieldParams.substring(0, dotIndex)) : 0;
		int fieldNo = dotIndex > -1 ? Integer.parseInt(fieldParams.substring(dotIndex + 1)) : Integer
				.parseInt(fieldParams);
		return sources[recNo].getField(fieldNo).getValue();
	}

	@Override
	Rule duplicate() {
		FieldRule duplicate = new FieldRule(source);
		duplicate.setFieldParams(fieldParams);
		duplicate.setGraph(graph);
		duplicate.setLogger(logger);
		duplicate.setProperties(parameters);
		return duplicate;
	}
}

/**
 * Descendent of Rule class for storing sequence rule
 */
class SequenceRule extends Rule {

	String method;// sequence Id with one of squence method used for

	// getting value from sequence eg.:
	// seq1.nextValueString()

	SequenceRule(String source) {
		super(source);
	}

	SequenceRule(Object value) {
		super(value);
		if (!(value instanceof Sequence)) {
			throw new IllegalArgumentException("Sequence rule doesn't accept " + value.getClass().getName()
					+ " argument");
		}
		source = ((Sequence) value).getId();
	}

	@Override
	Rule duplicate() {
		SequenceRule duplicate;
		if (value != null) {
			duplicate = new SequenceRule(value);
		} else {
			duplicate = new SequenceRule(source);
		}
		duplicate.setGraph(graph);
		duplicate.setLogger(logger);
		duplicate.setProperties(parameters);
		return duplicate;
	}

	@Override
	String getType() {
		return "SEQUENCE_RULE";
	}

	@Override
	Object getCanonicalSource() {
		return method;
	}

	@Override
	void init(DataRecordMetadata[] sourceMetadata, DataRecordMetadata[] targetMetadata, int recNo, int fieldNo,
			PolicyType policy) throws ComponentNotReadyException {
		// prepare sequence and method
		String sequenceID = source.indexOf(CustomizedRecordTransform.DOT) == -1 ? source : source.substring(0, source
				.indexOf(CustomizedRecordTransform.DOT));
		if (value == null) {
			value = graph.getSequence(sequenceID);
		}
		if (value == null) {
			warn("There is no sequence \"" + sequenceID + "\" in graph");
			if (!(targetMetadata[recNo].getField(fieldNo).isNullable() || targetMetadata[recNo].getField(fieldNo)
					.isDefaultValueSet())) {
				error("Null value not allowed to record: " + targetMetadata[recNo].getName() + " , field: "
						+ targetMetadata[recNo].getField(fieldNo).getName());
			} else {
				method = "null";
				return;
			}
		}
		// check sequence method
		String method = source.indexOf(CustomizedRecordTransform.DOT) > -1 ? source.substring(source
				.indexOf(CustomizedRecordTransform.DOT) + 1) : null;
		DataFieldType methodType = DataFieldType.UNKNOWN;
		if (method != null) {
			this.method = method;
			if (method.toLowerCase().startsWith("currentvaluestring")
					|| method.toLowerCase().startsWith("currentstring")
					|| method.toLowerCase().startsWith("nextvaluestring")
					|| method.toLowerCase().startsWith("nextstring")) {
				methodType = DataFieldType.STRING;
			}
			if (method.toLowerCase().startsWith("currentvalueint") || method.toLowerCase().startsWith("currentint")
					|| method.toLowerCase().startsWith("nextvalueint") || method.toLowerCase().startsWith("nextint")) {
				methodType = DataFieldType.INTEGER;
			}
			if (method.toLowerCase().startsWith("currentvaluelong") || method.toLowerCase().startsWith("currentlong")
					|| method.toLowerCase().startsWith("nextvaluelong") || method.toLowerCase().startsWith("nextlong")) {
				methodType = DataFieldType.LONG;
			}
		} else {// method is not given, prepare the best
			switch (targetMetadata[recNo].getField(fieldNo).getDataType()) {
			case BYTE:
			case CBYTE:
			case STRING:
				this.method = sequenceID + CustomizedRecordTransform.DOT + "nextValueString()";
				methodType = DataFieldType.STRING;
				break;
			case DECIMAL:
			case LONG:
			case NUMBER:
				this.method = sequenceID + CustomizedRecordTransform.DOT + "nextValueLong()";
				methodType = DataFieldType.LONG;
				break;
			case INTEGER:
				this.method = sequenceID + CustomizedRecordTransform.DOT + "nextValueInt()";
				methodType = DataFieldType.INTEGER;
				break;
			default:
				errorMessage = "Can't set sequence to data field of type: "
						+ targetMetadata[recNo].getField(fieldNo).getDataType().getName() + " ("
						+ targetMetadata[recNo].getName() + CustomizedRecordTransform.DOT
						+ targetMetadata[recNo].getField(fieldNo).getName() + ")";
				error(errorMessage);
			}
			DataFieldMetadata tmp = null;
			if (methodType == DataFieldType.UNKNOWN) {
				error("Unknown sequence method");
			} else {
				tmp = new DataFieldMetadata("tmp", methodType, ";");
			}
			// check if value from sequence can be set to given field
			if (!checkTypes(targetMetadata[recNo].getField(fieldNo), tmp, policy)) {
				if (policy == PolicyType.STRICT) {
					errorMessage = "Sequence method:" + this.method + " does not " + "match field type:\n"
							+ targetMetadata[recNo].getName() + CustomizedRecordTransform.DOT
							+ targetMetadata[recNo].getField(fieldNo).getName() + " type - "
							+ targetMetadata[recNo].getField(fieldNo).getDataType().getName()
							+ CustomizedRecordTransform.getDecimalParams(targetMetadata[recNo].getField(fieldNo));
					error(errorMessage);
				}
				if (policy == PolicyType.CONTROLLED) {
					errorMessage = "Sequence method:" + this.method + " does not " + "match field type:\n"
							+ targetMetadata[recNo].getName() + CustomizedRecordTransform.DOT
							+ targetMetadata[recNo].getField(fieldNo).getName() + " type - "
							+ targetMetadata[recNo].getField(fieldNo).getDataType().getName()
							+ CustomizedRecordTransform.getDecimalParams(targetMetadata[recNo].getField(fieldNo));
					error(errorMessage);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.Rule#getValue(org.jetel.data.DataRecord[])
	 */
	@Override
	Object getValue(DataRecord[] sources) {
		if (value == null) {
			return null;
		}
		int dotIndex = method.indexOf(CustomizedRecordTransform.DOT);
		String method = this.method.substring(dotIndex + 1);
		if (method.toLowerCase().startsWith("currentvaluestring") || method.toLowerCase().startsWith("currentstring")) {
			return ((Sequence) value).currentValueString();
		}
		if (method.toLowerCase().startsWith("nextvaluestring") || method.toLowerCase().startsWith("nextstring")) {
			return ((Sequence) value).nextValueString();
		}
		if (method.toLowerCase().startsWith("currentvalueint") || method.toLowerCase().startsWith("currentint")) {
			return ((Sequence) value).currentValueInt();
		}
		if (method.toLowerCase().startsWith("nextvalueint") || method.toLowerCase().startsWith("nextint")) {
			return ((Sequence) value).nextValueInt();
		}
		if (method.toLowerCase().startsWith("currentvaluelong") || method.toLowerCase().startsWith("currentlong")) {
			return ((Sequence) value).currentValueLong();
		}
		if (method.toLowerCase().startsWith("nextvaluelong") || method.toLowerCase().startsWith("nextlong")) {
			return ((Sequence) value).nextValueLong();
		}
		// in method validateRule checked, that has to be one of method above
		return null;
	}

}

/**
 * Descendent of Rule class for storing constant rule
 */
class ConstantRule extends Rule {

	/**
	 * Constructor for setting constant as string
	 * 
	 * @param source
	 */
	ConstantRule(String source) {
		super(source);
	}

	/**
	 * Constructor for setting constant as expected Object (due to data field type)
	 * 
	 * @param value
	 */
	ConstantRule(Object value) {
		super(value);
	}

	@Override
	Rule duplicate() {
		ConstantRule duplicate;
		if (value != null) {
			duplicate = new ConstantRule(value);
		} else {
			duplicate = new ConstantRule(source);
		}
		duplicate.setGraph(graph);
		duplicate.setLogger(logger);
		duplicate.setProperties(parameters);
		return duplicate;
	}

	@Override
	String getType() {
		return "CONSTANT_RULE";
	}

	@Override
	Object getCanonicalSource() {
		return source != null ? source : value;
	}

	@Override
	Object getValue(DataRecord[] sources) {
		return value;
	}

	@Override
	void init(DataRecordMetadata[] sourceMetadata, DataRecordMetadata[] targetMetadata, int recNo, int fieldNo,
			PolicyType policy) throws ComponentNotReadyException {
		// used temporary data field for checking constant
		DataField tmp = DataFieldFactory.createDataField(targetMetadata[recNo].getField(fieldNo), false);
		if (source != null) {
			try {
				tmp.fromString(source);
				value = tmp.getValue();
			} catch (BadDataFormatException e) {
				error(ExceptionUtils.exceptionChainToMessage(e));
			}
		} else {
			try {
				tmp.setValue(value);
				source = tmp.toString();
			} catch (BadDataFormatException e) {
				error(ExceptionUtils.exceptionChainToMessage(e));
			}
		}
	}

}

/**
 * Descendent of Rule class for storing parameter rule
 */
class ParameterRule extends Rule {

	ParameterRule(String source) {
		super(source);
	}

	@Override
	Rule duplicate() {
		ParameterRule duplicate = new ParameterRule(source);
		duplicate.setGraph(graph);
		duplicate.setLogger(logger);
		duplicate.setProperties(parameters);
		return duplicate;
	}

	@Override
	String getType() {
		return "PARAMETER_RULE";
	}

	@Override
	Object getCanonicalSource() {
		return source;
	}

	@Override
	Object getValue(DataRecord[] sources) {
		return value;
	}

	@Override
	void init(DataRecordMetadata[] sourceMetadata, DataRecordMetadata[] targetMetadata, int recNo, int fieldNo,
			PolicyType policy) throws ComponentNotReadyException {
		// get parameter value
		String paramValue;
		if (source.startsWith("${")) {// get graph parameter
			paramValue = graph.getGraphProperties().getProperty(source.substring(2, source.lastIndexOf('}')));
		} else if (source.startsWith(String.valueOf(CustomizedRecordTransform.PARAMETER_CHAR))) {
			// get parameter from node properties
			paramValue = parameters.getProperty((source));
		} else {
			// try to find parameter with given name in node properties
			paramValue = parameters.getProperty(CustomizedRecordTransform.PARAMETER_CHAR + source);
			if (paramValue == null) {
				// try to find parameter with given name among graph parameters
				paramValue = graph.getGraphProperties().getProperty(source);
			}
			if (paramValue == null) {
				if (!(targetMetadata[recNo].getField(fieldNo).isNullable() || targetMetadata[recNo].getField(fieldNo)
						.isDefaultValueSet())) {
					error("Not found parameter: " + source);
				} else {
					warn("Not found parameter: " + source);
				}
			}
		}
		// use temporary field to check if the value can be set to given data field
		DataField tmp = DataFieldFactory.createDataField(targetMetadata[recNo].getField(fieldNo), false);
		try {
			tmp.fromString(paramValue);
			value = tmp.getValue();
		} catch (BadDataFormatException e) {
			error(ExceptionUtils.exceptionChainToMessage(e));
		}
	}

}

/**
 * Degenerated descendent of Rule class for marking fields for deleting previous rule
 */
class DeleteRule extends Rule {

	DeleteRule() {
		super(null);
	}

	@Override
	Rule duplicate() {
		DeleteRule duplicate = new DeleteRule();
		duplicate.setGraph(graph);
		duplicate.setLogger(logger);
		duplicate.setProperties(parameters);
		return duplicate;
	}

	@Override
	String getType() {
		return "DELETE_RULE";
	}

	@Override
	Object getCanonicalSource() {
		return null;
	}

	@Override
	Object getValue(DataRecord[] sources) {
		return null;
	}

	@Override
	void init(DataRecordMetadata[] sourceMetadata, DataRecordMetadata[] targetMetadata, int recNo, int fieldNo,
			PolicyType policy) throws ComponentNotReadyException {
		// do nothing
	}

}
