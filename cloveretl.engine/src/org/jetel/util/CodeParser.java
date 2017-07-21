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
package org.jetel.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
/**
 * The purpose for this class is to handle parsing java code enhanced with
 * cloverETL syntax.  Initially cloverETL syntax will support references
 * to field values and record &amp; field objects.<br>
 * The future enhancement will be to add support for aggregate
 * functions (similar to SQL agregation). <br>
 * <h3>Syntax used</h3>
 * <table>
 * <tr>
 * <td valign="top"><tt>${<b>in.</b><i>record_ordinal_num</i><b>.</b><i>field_name</i>}</tt></td>
 * <td valign="top"><p>References <emp>intput record</emp> and field within record for reading values from it.</p>
 * <p>Record ordinal number corresponds to port number from which the record is/was read.</p>
 * <p>Field names are names of individual fields within respective records.</p>
 * <p>The actual Java code generated depends on field's data type. <br>
 * Data field object is casted to proper subclass (e.g. StringDataField,
 * NumericDataField,etc.) and then appropriate "getter" method is called
 * This table shows
 * what "getter" method is generated for individual Clover field types:</p>
 * <table border="1">
 * <tr><td>STRING_FIELD</td><td>toString()</td></tr>
 * <tr><td>DATE_FIELD<br>DATETIME_FIELD</td><td>getDate()</td></tr>
 * <tr><td>NUMERIC_FIELD</td><td>getDouble()</td></tr>
 * <tr><td>INTEGER_FIELD</td><td>getInt()</td></tr>
 * <tr><td>DECIMAL_FIELD</td><td>getDecimal()</td></tr>
 * <tr><td>BYTE_FIELD</td><td>getValue()</td></tr>
 * </table>
 * <i>Note: field names are not case sensitive</i></td>
 * </tr>
 * <tr>
 * <td valign="top"><tt>@{<b>in.</b><i>record_ordinal_num</i><b>.</b><i>field_name</i>}</tt></td>
 * <td valign="top"><p>References <emp>intput record</emp> and field within record - the field object.</p>
 * <p>Record ordinal number corresponds to port number from which the record is/was read.</p>
 * <p>Field names are names of individual fields within respective records.</p>
 * <p>It is translated to object access - e.g. <tt>@&lt;in.1.id&gt;</tt> is translated to
 * to something like: <tt>(inputRecords[0].getField(1))</tt></p>
 * </tr>
 * <tr>
 * <td valign="top"><tt>${<b>out.</b><i>record_ordinal_num</i><b>.</b><i>field_name</i>}</tt></td>
 * <td valign="top"><p>References <emp>output record</emp> and field within record for assigning values to it.</p>
 * <p>The generated Java code takes care of casting the DataField to proper subclass (e.g. StringDataField,
 * NumericDataField,etc.) and calling <tt>setValue()</tt> on the object. 
 * </td>
 * </tr>
 * <tr>
 * <td valign="top"><tt>@{<b>out.</b><i>record_ordinal_num</i><b>.</b><i>field_name</i>}</tt></td>
 * <td valign="top"><i>similar to</i> <tt>@{<b>in.</b></tt> </td>
 * </tr>
 * <tr>
 * <td valign="top"><tt>${<b>par.</b><i>parameter_name</i>}</tt></td>
 * <td valign="top"><p>References <emp>graph's paramater</emp> for reading values from it.</p>
 * <p>Parameter's value in form of <tt>String</tt> object is returned.</p>
 * </td>
 * </tr>
 * <td valign="top"><tt>${<b>seq.</b><i>sequence_name</i>}</tt></td>
 * <td valign="top"><p>References <emp>graph's sequence</emp> for reading values from it.</p>
 * <p>Sequence must be registered within graph. The generated Java code takes
 * care of initializing the object and calling <tt>nextValueInt()</tt>.</p>
 * </td>
 * </tr>
 * </table>
 * @author                          David Pavlis, Martin Zatopek
 * @since
 * @revision                        $Revision$
 * <h4>Example:</h4>
 * <pre>
 * String variable1 = Integer.parseInt(${in.0.OrderID});
 * 
 * ${out.0.OrderID}=variable1;
 * ${out.0.UniqueID}=${in.0.OrderID}+${in.0.CustomerID}; </pre>
 * <h4>Example:</h4>
 * <h5>Mixed Java code:</h5>
 * <pre>
 * ${out.0.PSSO_ID} = ${seq.Sequence0};
 * ${out.0.PROJECT_ID} = ${in.0.PROJECT_ID};
 * ${out.0.PSFLAG} = ${in.0.PSFLAG};
 * ${out.0.NAME} = ${in.0.NAME}+${in.1.NAME}.toUpperCase();
 * ${out.0.PSSOUSERNUM} = ${in.0.PSSOUSERNUM};
 * ${out.0.SUPPLIER} = ${in.1.SUPPLIER};
 * ${out.0.PROJECTNUM} = ${in.1.PROJECTNUM};
 * ${out.0.RESPONSIBLEPERSON} = "Person"+${in.1.RESPONSIBLEPERSON};
 * ${out.0.HZS_IND_M} = ${in.1.HZS_IND_M};
 * ${out.0.HZS_IND} = ${in.1.HZS_IND};
 * ${out.0.STARTDATE} = ${in.1.STARTDATE};
 * ${out.0.ENDDATE} = ${in.1.ENDDATE};
 * ${out.0.DESCRIPTION} = ${par.dbURL};	</pre>
 *<h5>Output Java source code (excerpt):</h5>
 * <pre>
 * ((IntegerDataField)outputRecords[0].getField(OUT0_PSSO_ID)).setValue(  Sequence0.nextValueInt());
 * ((IntegerDataField)outputRecords[0].getField(OUT0_PROJECT_ID)).setValue(  (((IntegerDataField)inputRecords[0].getField(IN0_PROJECT_ID)).getInt()));
 * ((IntegerDataField)outputRecords[0].getField(OUT0_PSFLAG)).setValue(  (((IntegerDataField)inputRecords[0].getField(IN0_PSFLAG)).getInt()));
 * (outputRecords[0].getField(OUT0_NAME)).setValue(  (inputRecords[0].getField(IN0_NAME).toString())+(inputRecords[1].getField(IN1_NAME).toString()).toUpperCase());
 * ((IntegerDataField)outputRecords[0].getField(OUT0_PSSOUSERNUM)).setValue(  (((IntegerDataField)inputRecords[0].getField(IN0_PSSOUSERNUM)).getInt()));
 * ((IntegerDataField)outputRecords[0].getField(OUT0_SUPPLIER)).setValue(  (((IntegerDataField)inputRecords[1].getField(IN1_SUPPLIER)).getInt()));
 * ((IntegerDataField)outputRecords[0].getField(OUT0_PROJECTNUM)).setValue(  (((IntegerDataField)inputRecords[1].getField(IN1_PROJECTNUM)).getInt()));
 * ((NumericDataField)outputRecords[0].getField(OUT0_HZS_IND_M)).setValue(  (((NumericDataField)inputRecords[1].getField(IN1_HZS_IND_M)).getDouble()));
 * ((NumericDataField)outputRecords[0].getField(OUT0_HZS_IND)).setValue(  (((NumericDataField)inputRecords[1].getField(IN1_HZS_IND)).getDouble()));
 * ((DateDataField)outputRecords[0].getField(OUT0_STARTDATE)).setValue(  (((DateDataField)inputRecords[1].getField(IN1_STARTDATE)).getDate()));
 * ((DateDataField)outputRecords[0].getField(OUT0_ENDDATE)).setValue(  (((DateDataField)inputRecords[1].getField(IN1_ENDDATE)).getDate()));
 * (outputRecords[0].getField(OUT0_DESCRIPTION)).setValue(  param_dbURL);
 *</pre>
 * 
 *  
 */
public class CodeParser {

	private Map inputRecordsNames;
	private Map outputRecordsNames;
	private Map[] inputFieldsNames;
	private Map[] outputFieldsNames;
	private StringBuffer sourceCode;
	private DataRecordMetadata[] inputRecordsMeta;
	private DataRecordMetadata[] outputRecordsMeta;
	private String[] classImports;
	private Map sequences = new HashMap();
	private Map parameters = new HashMap();
	private Set refInputFieldNames = new LinkedHashSet();
	private Set refOutputFieldNames = new LinkedHashSet();
   
	
	private boolean useSymbolicNames=true;
    
	private final static int SOURCE_CODE_BUFFER_INITIAL_SIZE = 512;
	private final static String DEFAULT_OUTPUT_FILE_CHARSET = "UTF-8";
    
    private final static String GET_OPCODE_STR = "${in.";
	private final static String GET_OPCODE_REGEX = "\\$\\{in.";
	
	private final static String OBJ_IN_ACCESS_OPCODE_STR= "@{in.";
	private final static String OBJ_IN_ACCESS_OPCODE_REGEX= "@\\{in.";
	
	private final static String SET_OPCODE_STR = "${out.";
	private final static String SET_OPCODE_REGEX = "\\$\\{out.";
	
	private final static String OBJ_OUT_ACCESS_OPCODE_STR= "@{out.";
	private final static String OBJ_OUT_ACCESS_OPCODE_REGEX= "@\\{out.";
	
	private final static String PARAM_OPCODE_STR = "${par.";
	private final static String PARAM_OPCODE_REGEX = "\\$\\{par.";
	private final static String PARAM_CODE_PREFIX = "param_";
	
	private final static String SEQ_OPCODE_STR = "${seq.";
	private final static String SEQ_OPCODE_REGEX = "\\$\\{seq.";

	private final static String OBJ_SEQ_OPCODE_STR = "@{seq.";
	private final static String OBJ_SEQ_OPCODE_REGEX = "@\\{seq.";

	private final static String OPCODE_END_STR = "}";

	private final static String IN_RECORDS_ARRAY_NAME_STR = "inputRecords";
	private final static String OUT_RECORDS_ARRAY_NAME_STR = "outputRecords";

	private final static char GET_OPCODE = 'G';
	private final static char SET_OPCODE = 'S';
//unused	private final static char UNKNOWN_OPCODE = (char) -1;

	static Log logger = LogFactory.getLog(CodeParser.class);

	/**
	 * @param  inputRecords   Description of the Parameter
	 * @param  outputRecords  Description of the Parameter
	 */
	@SuppressWarnings("EI2")
	public CodeParser(DataRecordMetadata[] inputMetadatas, DataRecordMetadata[] outputMetadatas) {
	    
	    //initialization metadata arrays based on given ports
        this.inputRecordsMeta = inputMetadatas != null ? inputMetadatas : new DataRecordMetadata[0];
        this.outputRecordsMeta = outputMetadatas != null ? outputMetadatas : new DataRecordMetadata[0];

        inputRecordsNames = new HashMap(inputRecordsMeta.length);
        outputRecordsNames = new HashMap(outputRecordsMeta.length);
        inputFieldsNames = new HashMap[inputRecordsMeta.length];
        outputFieldsNames = new HashMap[outputRecordsMeta.length];
        // initialize map for input records & fields
        for (int i = 0; i < inputRecordsMeta.length; i++) {
            inputRecordsNames.put(String.valueOf(i), Integer.valueOf(i));
            inputFieldsNames[i] = new HashMap(inputRecordsMeta[i]
                    .getNumFields());
            for (int j = 0; j < inputRecordsMeta[i].getNumFields(); j++) {
                inputFieldsNames[i].put(inputRecordsMeta[i].getField(j)
                        .getName(), Integer.valueOf(j)
                /*
                 * inputRecords.getField(j)
                 */
                );
            }
        }
        // initialize map for output records & fields
        for (int i = 0; i < outputRecordsMeta.length; i++) {
            outputRecordsNames.put(String.valueOf(i), Integer.valueOf(i));
            outputFieldsNames[i] = new HashMap(outputRecordsMeta[i]
                    .getNumFields());
            for (int j = 0; j < outputRecordsMeta[i].getNumFields(); j++) {
                outputFieldsNames[i].put(outputRecordsMeta[i].getField(j)
                        .getName(), Integer.valueOf(j)
                /*
                 * outputRecords.getField(j)
                 */
                );
            }
        }
        sourceCode = new StringBuffer(SOURCE_CODE_BUFFER_INITIAL_SIZE);

    }

    /**
     * Gets the sourceCode attribute of the CodeParser object
     * 
     * @return The sourceCode value
     */
    public String getSourceCode() {
        return sourceCode.toString();
    }

    
    public String getClassName() {
        Pattern pattern = Pattern.compile("class\\s+(\\w+)");
        Matcher matcher =  pattern.matcher(sourceCode);
        
        if (matcher.find()){
            return matcher.group(1);
        }

        return null;
    }
    
    public boolean setClassName(String newName) {
        Pattern pattern = Pattern.compile("class\\s+(\\w+)");
        Matcher matcher =  pattern.matcher(sourceCode);
        
        if (matcher.find()){
            matcher.replaceFirst("class "+newName);
            return true;
        }
        return false;
    }
    
    /**
     * Description of the Method
     * 
     * @param filename
     *            Description of the Parameter
     * @exception IOException
     *                Description of the Exception
     */
    public void saveSourceCode(String filename) throws IOException {
        saveSourceCode(new File(filename));

    }

    public void saveSourceCode(File file) throws IOException {
        Writer out = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file), DEFAULT_OUTPUT_FILE_CHARSET));
        try{
        	out.append(sourceCode);
        } finally {
        	out.close();
        }

    }

    /**
     * Sets the sourceCode attribute of the CodeParser object
     * 
     * @param charSeq
     *            The new sourceCode value
     */
    public void setSourceCode(CharSequence charSeq) {
        sourceCode.setLength(0);
        sourceCode.ensureCapacity(charSeq.length());
        sourceCode.append("\t\t\t");
        for (int i = 0; i < charSeq.length(); i++) {
            sourceCode.append(charSeq.charAt(i));
            if (charSeq.charAt(i) == '\n') {
                sourceCode.append("\t\t\t");
            }
        }
    }

    public boolean isUseSymbolicNames() {
        return useSymbolicNames;
    }

    /**
     * Specifies whether output Java source code should use symbolic names when
     * accessing fields instead of their ordinal numbers. <br>
     * Default is yes(true).
     * 
     * @param useSymbolicNames
     *            true/false
     */
    public void setUseSymbolicNames(boolean useSymbolicNames) {
        this.useSymbolicNames = useSymbolicNames;
    }

    /** */
    public void parse() {
        // find all CloverETL tokens first
        String[] fieldRefStr;
        String refStr;
        Token token;
        int index;
        // firstly all get opcodes
        do {
            token = findEnclosedString(0, GET_OPCODE_STR, OPCODE_END_STR);
            if (token != null) {
                fieldRefStr = parseFieldReference(GET_OPCODE, token.getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1,
                        translateToFieldGetMethod(fieldRefStr));
            }
        } while (token != null);
        // next all parameters
        do {
            token = findEnclosedString(0, PARAM_OPCODE_STR, OPCODE_END_STR);
            if (token != null) {
                refStr = parseParamReference(token.getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1, translateToParam(refStr));
            }
        } while (token != null);
        // next all sequences' values
        do {
            token = findEnclosedString(0, SEQ_OPCODE_STR, OPCODE_END_STR);
            if (token != null) {
                refStr = parseSeqReference(token.getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1, translateToSeq(refStr));
            }
        } while (token != null);
        // next all sequence objects
        do {
            token = findEnclosedString(0, OBJ_SEQ_OPCODE_STR, OPCODE_END_STR);
            if (token != null) {
                refStr = parseSeqReferenceObj(token.getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1, translateToSeqObj(refStr));
            }
        } while (token != null);
        // next all input field & record objects
        do {
            token = findEnclosedString(0, OBJ_IN_ACCESS_OPCODE_STR,
                    OPCODE_END_STR);
            if (token != null) {
                fieldRefStr = parseObjectReference(GET_OPCODE, token
                        .getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1,
                        translateToFieldRecordObjectReference(fieldRefStr));
            }
        } while (token != null);

        // next all output field & record objects
        do {
            token = findEnclosedString(0, OBJ_OUT_ACCESS_OPCODE_STR,
                    OPCODE_END_STR);
            if (token != null) {
                fieldRefStr = parseObjectReference(SET_OPCODE, token
                        .getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1,
                        translateToFieldRecordObjectReference(fieldRefStr));
            }
        } while (token != null);

        // finally all set opcodes (assignments)
        do {
            token = findEnclosedString(0, SET_OPCODE_STR, OPCODE_END_STR);
            if (token != null) {
                fieldRefStr = parseFieldReference(SET_OPCODE, token.getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1,
                        translateToFieldSetMethod(fieldRefStr));
                //we have to remove [=]
                index = sourceCode.indexOf("=", token.getEndOffset());
                if (index != -1) {
                    sourceCode.deleteCharAt(index);
                } else {
                    throw new RuntimeException(
                            "No [=] found when parsing field reference: "
                                    + token);
                }
                // we have to find [;] and insert parenthesis [)] in front of it
                // [;] must be outside of quotes - counting quotes before first [;] - right is even number of quotes
                index = sourceCode.indexOf(";", token.getEndOffset());

                if (index != -1) {
                    int numQuotes = 0;
                    int indexQuotes = token.getEndOffset();
                    boolean parenthesisNotAdded = true;
                    while(parenthesisNotAdded) {
                        indexQuotes = sourceCode.indexOf("\"", indexQuotes + 1);
                    
                        if(indexQuotes != -1 && indexQuotes < index) numQuotes++; 
                        else {
                            if(numQuotes % 2 == 0) {
                                sourceCode.insert(index, ")");
                                parenthesisNotAdded = false;
                            } else {
                                if(indexQuotes != -1) numQuotes++;
                                index = sourceCode.indexOf(";", indexQuotes);
                            }
                        }
                    }
                } else {
                    throw new RuntimeException(
                            "No [;] found around when parsing field reference: "
                                    + token);
                }
            }
        } while (token != null);
    }

    /**
     * Description of the Method
     * 
     * @param fieldRef
     *            Description of the Parameter
     * @return Description of the Return Value
     */
    private String translateToParam(String fieldRef) {
        if (!parameters.containsKey(fieldRef)) {
            parameters.put(fieldRef, fieldRef);
        }
        return PARAM_CODE_PREFIX + fieldRef;
    }

    /**
     * Description of the Method
     * 
     * @param fieldRef
     *            Description of the Parameter
     * @return Description of the Return Value
     */
    private String translateToSeq(String fieldRef) {
        if (!sequences.containsKey(fieldRef)) {
            sequences.put(fieldRef, fieldRef);
        }
        return fieldRef + ".nextValueInt()";
    }

    private String translateToSeqObj(String fieldRef) {
        if (!sequences.containsKey(fieldRef)) {
            sequences.put(fieldRef, fieldRef);
        }
        return fieldRef;
    }

    /**
     * Description of the Method
     * 
     * @param fieldRef
     *            Description of the Parameter
     * @return Description of the Return Value
     */
    private String translateToFieldGetMethod(String[] fieldRef) {
        Integer recordNum;
        Integer fieldNum;
        char fieldType;
        FieldReference fieldRefObj = null;
        StringBuffer code = new StringBuffer(40);

        //if (logger.isDebugEnabled()) {
        //	logger.debug(fieldRef[0]+" : "+fieldRef[1]);
        //}

        recordNum = (Integer) inputRecordsNames.get(fieldRef[0]);
        if (recordNum == null) {
            throw new RuntimeException("Input record does not exist: " + fieldRef[0]);
        }
        try {
            fieldNum = (Integer) inputFieldsNames[recordNum.intValue()].get(fieldRef[1]);
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new RuntimeException("Nonexisting index to array containing input records", ex);
        }
        if (fieldNum == null) {
            throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in input record: " + fieldRef[0]);
        }
        
        //	register that field has been referenced
        if (useSymbolicNames) {
            DataRecordMetadata recMeta = inputRecordsMeta[recordNum.intValue()];
            int fieldNo = fieldNum.intValue();
            fieldRefObj = new FieldReference(recMeta.getName(), recordNum
                    .intValue(), fieldRef[1], FieldReference.IN_DIRECTION,
                    fieldNo);
            refInputFieldNames.add(fieldRefObj);
        }

        // code for accessing record
        code.append(IN_RECORDS_ARRAY_NAME_STR).append("[").append(recordNum).append("]");
        // code for accessing field
        code.append(".getField(");
        if (useSymbolicNames) {
            code.append(formatFieldSymbolicName(fieldRefObj));
        } else {
            code.append(fieldNum);
        }
        code.append(")");

        // apply proper get method for field type
        try {
            fieldType = inputRecordsMeta[recordNum.intValue()].getFieldType(fieldNum.intValue());
        } catch (NullPointerException ex) {
            throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in input record: " + fieldRef[0]);
        }
        switch (fieldType) {
        case DataFieldMetadata.STRING_FIELD:
            code.append(".toString()");
            break;
        case DataFieldMetadata.DATETIME_FIELD:
        case DataFieldMetadata.DATE_FIELD:
            code.insert(0, "((DateDataField)");
            code.append(")");
            code.append(".getDate()");
            break;
        case DataFieldMetadata.NUMERIC_FIELD:
            code.insert(0, "((NumericDataField)");
            code.append(")");
            code.append(".getDouble()");
            break;
        case DataFieldMetadata.INTEGER_FIELD:
            code.insert(0, "((IntegerDataField)");
            code.append(")");
            code.append(".getInt()");
            break;
        case DataFieldMetadata.LONG_FIELD:
            code.insert(0, "((LongDataField)");
            code.append(")");
            code.append(".getLong()");
            break;
        case DataFieldMetadata.DECIMAL_FIELD:
            code.insert(0, "((DecimalDataField)");
            code.append(")");
            code.append(".getDecimal()");
            break;
        case DataFieldMetadata.BYTE_FIELD:
        case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
            code.insert(0, "((ByteDataField)");
            code.append(")");
            code.append(".getValue()");
            break;
        case DataFieldMetadata.BOOLEAN_FIELD:
            code.insert(0, "((BooleanDataField)");
            code.append(")");
            code.append(".getValue()");
            break;
        default:
            throw new RuntimeException("Can't translate field type !");
        }
        // finally, enclose everything into parenthesis
        code.insert(0, "(").append(")");

        return code.toString();
    }

    /**
     *  Description of the Method
     *
     * @param  fieldRef  Description of the Parameter
     * @return           Description of the Return Value
     */
    private String translateToFieldRecordObjectReference(String[] fieldRef) {
        Integer recordNum = null;
        Integer fieldNum = null;
        FieldReference fieldRefObj = null;
        StringBuffer code = new StringBuffer(40);

        //if (logger.isDebugEnabled()) {
        //	logger.debug(fieldRef[0]+" : "+fieldRef[1]);
        //}

        recordNum = (Integer) inputRecordsNames.get(fieldRef[0]);
        if (recordNum == null) {
            throw new RuntimeException("Input record does not exist: " + fieldRef[0]);
        }
        if (fieldRef.length > 1) { // we reference field as well
            try {
                fieldNum = (Integer) inputFieldsNames[recordNum.intValue()].get(fieldRef[1]);
            } catch (ArrayIndexOutOfBoundsException ex) {
                throw new RuntimeException("Nonexisting index to array containing input records", ex);
            }
            if (fieldNum == null) {
                throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in input record: " + fieldRef[0]);
            }
        }

        //	register that field has been referenced
        if (useSymbolicNames && fieldNum != null) {
            DataRecordMetadata recMeta = inputRecordsMeta[recordNum.intValue()];
            int fieldNo = fieldNum.intValue();
            fieldRefObj = new FieldReference(recMeta.getName(), recordNum
                    .intValue(), fieldRef[1], FieldReference.IN_DIRECTION,
                    fieldNo);
            refInputFieldNames.add(fieldRefObj);
        }

        // code for accessing record
        code.append(IN_RECORDS_ARRAY_NAME_STR).append("[").append(recordNum).append("]");
        // code for accessing field
        if (fieldNum != null) {
            code.append(".getField(");
            if (useSymbolicNames) {
                code.append(formatFieldSymbolicName(fieldRefObj));
            } else {
                code.append(fieldNum);
            }
            code.append(")");
        }

        // finally, enclose everything into parenthesis
        code.insert(0, "(").append(")");

        return code.toString();
    }

  


	/**
	 *  Description of the Method
	 *
	 * @param  fieldRef  Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private String translateToFieldSetMethod(String[] fieldRef) {
		Integer recordNum;
		Integer fieldNum;
		char fieldType;
		FieldReference fieldRefObj=null;
		StringBuffer code = new StringBuffer(40);

		//if (logger.isDebugEnabled()) {
		//	logger.debug(fieldRef[0]+" : "+fieldRef[1]);
		//}

		recordNum = (Integer) outputRecordsNames.get(fieldRef[0]);
		if (recordNum == null) {
			throw new RuntimeException("Output record does not exist: " + fieldRef[0]);
		}
		try {
			fieldNum = (Integer) outputFieldsNames[recordNum.intValue()].get(fieldRef[1]);
		} catch (ArrayIndexOutOfBoundsException ex) {
			throw new RuntimeException("Nonexisting index to array containing output records", ex);
		}
		if (fieldNum == null) {
			throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in output record: " + fieldRef[0]);
		}

		//	register that field has been referenced
		if (useSymbolicNames){
		    DataRecordMetadata recMeta=outputRecordsMeta[recordNum.intValue()];
		    int fieldNo=fieldNum.intValue();
		    fieldRefObj=new FieldReference(recMeta.getName(),
		            recordNum.intValue(),
		            fieldRef[1],
		            FieldReference.OUT_DIRECTION,fieldNo);
		    refOutputFieldNames.add(fieldRefObj);
		}
		
		// code for accessing record
		code.append(OUT_RECORDS_ARRAY_NAME_STR).append("[").append(recordNum).append("]");
		
		// code for accessing field
		code.append(".getField(");
		if (useSymbolicNames){
		    code.append(formatFieldSymbolicName(fieldRefObj));
		}else{
		    code.append(fieldNum);
		}
		code.append(")");
		
		// type cast according to Field's type
		// apply proper get method for field type
		try {
			fieldType = outputRecordsMeta[recordNum.intValue()].getFieldType(fieldNum.intValue());
		} catch (NullPointerException ex) {
			throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in output record: " + fieldRef[0]);
		}
		switch (fieldType) {
			case DataFieldMetadata.STRING_FIELD:
				code.insert(0,"(");
				break;
			case DataFieldMetadata.DATETIME_FIELD:
			case DataFieldMetadata.DATE_FIELD:
				code.insert(0,"((DateDataField)");
				break;
			case DataFieldMetadata.NUMERIC_FIELD:
				code.insert(0,"((NumericDataField)");
				break;
			case DataFieldMetadata.INTEGER_FIELD:
				code.insert(0,"((IntegerDataField)");
				break;
			case DataFieldMetadata.LONG_FIELD:
				code.insert(0,"((LongDataField)");
				break;
			case DataFieldMetadata.DECIMAL_FIELD:
				code.insert(0,"((DecimalDataField)");
				break;
			case DataFieldMetadata.BYTE_FIELD:
			case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			    code.insert(0,"((ByteDataField)");
			    break;
			case DataFieldMetadata.BOOLEAN_FIELD:
			    code.insert(0,"((BooleanDataField)");
			    break;
			default:
				throw new RuntimeException("Can't translate field type !");
		}
		code.append(")");
		
		//set method
		code.append(".setValue(");
					
		return code.toString();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  refType   Description of the Parameter
	 * @param  fieldRef  Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private String[] parseFieldReference(char refType, String fieldRef) {
		if (refType == GET_OPCODE) {
			fieldRef = fieldRef.replaceFirst(GET_OPCODE_REGEX, "");
		} else {
			fieldRef = fieldRef.replaceFirst(SET_OPCODE_REGEX, "");
		}
		fieldRef = fieldRef.replaceFirst("\\}", "");
		//fieldRef = fieldRef.toUpperCase();

		return fieldRef.split("\\.", 2);
	}

  /**
	 *  Description of the Method
	 *
	 * @param  refType   Description of the Parameter
	 * @param  fieldRef  Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private String[] parseObjectReference(char refType, String fieldRef) {
		if (refType == GET_OPCODE) {
			fieldRef = fieldRef.replaceFirst(OBJ_IN_ACCESS_OPCODE_REGEX, "");
		} else {
			fieldRef = fieldRef.replaceFirst(OBJ_OUT_ACCESS_OPCODE_REGEX, "");
		}
		fieldRef = fieldRef.replaceFirst("\\}", "");
		//fieldRef = fieldRef.toUpperCase();

		return fieldRef.split("\\.", 2);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  refType   Description of the Parameter
	 * @param  fieldRef  Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private String parseParamReference(String fieldRef) {
		fieldRef = fieldRef.replaceFirst(PARAM_OPCODE_REGEX, "");
		fieldRef = fieldRef.replaceFirst("\\}", "");
		//fieldRef = fieldRef.toUpperCase();

		return fieldRef;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  refType   Description of the Parameter
	 * @param  fieldRef  Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private String parseSeqReference(String fieldRef) {
		fieldRef = fieldRef.replaceFirst(SEQ_OPCODE_REGEX, "");
		fieldRef = fieldRef.replaceFirst("\\}", "");
		//fieldRef = fieldRef.toUpperCase();

		return fieldRef;
	}

  private String parseSeqReferenceObj(String fieldRef) {
		fieldRef = fieldRef.replaceFirst(OBJ_SEQ_OPCODE_REGEX, "");
		fieldRef = fieldRef.replaceFirst("\\}", "");
		//fieldRef = fieldRef.toUpperCase();

		return fieldRef;
	}

	/**
	 *  Description of the Method
	 *
	 * @param  offset      Description of the Parameter
	 * @param  openingStr  Description of the Parameter
	 * @param  closingStr  Description of the Parameter
	 * @return             Description of the Return Value
	 */
	private Token findEnclosedString(int offset, String openingStr, String closingStr) {
		int startOffset;
		int endOffset;

		if ((startOffset = sourceCode.indexOf(openingStr, offset)) != -1) {
			if ((endOffset = sourceCode.indexOf(closingStr, startOffset)) != -1) {
				return new Token(startOffset, endOffset, sourceCode.substring(startOffset, endOffset + 1));
			}
		}
		return null;
	}


	/**
	 *  Adds Java source code needed to act as DataRecordTransform 
	 *
	 * @param  className  The feature to be added to the TransformCodeStub attribute
	 */
	public void addTransformCodeStub(String className) {
		StringBuffer transCode = new StringBuffer(SOURCE_CODE_BUFFER_INITIAL_SIZE);

		//imports
		transCode.append("// automatically generated on ");
		transCode.append(java.util.Calendar.getInstance().getTime()).append("\n");
        transCode.append("import java.util.*;\n");
		transCode.append("import org.jetel.data.*;\n");
		transCode.append("import org.jetel.graph.*;\n");
		transCode.append("import org.jetel.metadata.*;\n");
        transCode.append("import org.jetel.component.*;\n");
        transCode.append("import org.jetel.exception.*;\n");
		transCode.append("import org.jetel.data.sequence.*;\n");
		
		// add any user specified imports
		if (classImports!=null){
			for(int i=0;i<classImports.length;i++){
				transCode.append("import ").append(classImports[i]).append("; \n");
			}
		}
		
		transCode.append("\n");
		//class start definition
		transCode.append("public class ").append(className).append(" extends DataRecordTransform { \n\n");
		
		
		// shall we use symbolic names when referencing fields ?
		if (useSymbolicNames){
		    // list input field references -
		    transCode.append("\n");
		    transCode.append("\t// CONSTANTS definition of input fields\n");
		    for (Iterator it=refInputFieldNames.iterator();it.hasNext();){
		        final FieldReference ref=(FieldReference)it.next();
		        transCode.append("\tprivate final static int ").append(formatFieldSymbolicName(ref)).append(" = ").
		        append(ref.fieldNum).append(";\n");
		    }
		    transCode.append("\n");
		    // list input field references -
		    transCode.append("\t// CONSTANTS definition of output fields\n");
		    for (Iterator it=refOutputFieldNames.iterator();it.hasNext();){
		        final FieldReference ref=(FieldReference)it.next();
		        transCode.append("\tprivate final static int ").append(formatFieldSymbolicName(ref)).append(" = ").
		        append(ref.fieldNum).append(";\n");
		    }
		    transCode.append("\n");
		}
		//definition sequences
		for(Iterator it = sequences.values().iterator(); it.hasNext();) {
		    final String seq = (String) it.next();
		    transCode.append("\tSequence ").append(seq).append(";\n"); 
		}
		//definition parameters
		for(Iterator it = parameters.values().iterator(); it.hasNext();) {
		    final String param = (String) it.next();
		    transCode.append("\tString ").append(PARAM_CODE_PREFIX).append(param).append(";\n"); 
		}
		//init method
		transCode.append("\n\t/**\n"
		        		+ "\t * Initializes reformat class/function. This method is called only once at then\n"
		   	 			+ "\t * beginning of transformation process. Any object allocation/initialization should\n"
		        		+ "\t * happen here.\n"
        				+ "\t */\n");
		transCode.append("\tpublic boolean init() throws ComponentNotReadyException {\n");
		//initialization sequeneces
		for(Iterator it = sequences.values().iterator(); it.hasNext();) {
		    final String seq = (String) it.next();
		    transCode.append("\t\t").append(seq).append(" = graph.getSequence(\"").append(seq).append("\");\n");
            transCode.append("\t\t" + "if(" + seq + " == null) {\n");
            transCode.append("\t\t\tthrow new ComponentNotReadyException(\"Sequence id='" + seq + "' does not exist.\");\n");
            transCode.append("\t\t}\n\n");
		}
        //initialization parameters
        for(Iterator it = parameters.values().iterator(); it.hasNext();) {
            final String param = (String) it.next();
            transCode.append("\t\t").append(PARAM_CODE_PREFIX).append(param).append(" = graph.getGraphProperties().getProperty(\"").append(param).append("\");\n"); 
            transCode.append("\t\t" + "if(" + PARAM_CODE_PREFIX + param + " == null) {\n");
            transCode.append("\t\t\tthrow new ComponentNotReadyException(\"Graph parameter '" + param + "' does not exist.\");\n");
            transCode.append("\t\t}\n\n");
        }
		transCode.append("\t\treturn true;\n");
		transCode.append("\t}\n\n");
		//transformation
		transCode.append("\t/**\n"
        		+ "\t * Performs reformat of source records to target records.\n"
   	 			+ "\t * This method is called as one step in transforming flow of\n"
        		+ "\t * records.\n"
				+ "\t */\n");
		transCode.append("\tpublic int transform(DataRecord[] " + IN_RECORDS_ARRAY_NAME_STR + ", DataRecord[] " + OUT_RECORDS_ARRAY_NAME_STR + ") throws TransformException {\n");
        transCode.append("\t\ttry {\n");

		//add triple tab before all lines of code
        transCode.append("\t\t\t// user's code STARTs from here !\n\n");
		sourceCode.insert(0, transCode.toString());
        sourceCode.append("\n\t\t\t// user's code ENDs here !\n");
        
		sourceCode.append("\t\t} catch(Exception e) {\n");
        sourceCode.append("\t\t\tthrow new TransformException(\"Error in transformation class \" + " + className + ".class.getName() + \": \" + e.getMessage(), e);\n");
        sourceCode.append("\t\t}\n");
		sourceCode.append("\t\treturn ALL;\n");
		sourceCode.append("\t}\n\n");
		
		sourceCode.append("\t/**\n"
        		+ "\t * Method called at the end of transformation process. No more\n"
   	 			+ "\t * records will be processed. The implementing class should release\n"
        		+ "\t * any resource reserved during init() or runtime at this point.\n"
				+ "\t */\n");
		sourceCode.append("\tpublic void finished() {\n");
		sourceCode.append("\t\t\n");
		sourceCode.append("\t}\n");
		
		sourceCode.append("}\n");
		sourceCode.append("//end of transform class \n");

	}

	
	@SuppressWarnings("EI2")
	public void setClassImports(String classNames[]){
		this.classImports=classNames;
	}

	
	public String formatFieldSymbolicName(FieldReference ref){
	    return ref.direction+ref.recNum+"_"+ref.fieldName.toUpperCase();
	}
	
	/**
	 *  The main program for the CodeParser class - for testing/debugging only
	 *
	 * @author       dpavlis
	 * @since
	 * @revision     $Revision$
	 * @param  argv  The command line arguments
	 */
//	public static void main(String[] argv) {
//		String string = "@{TestInput.Name} = ${TestInput.Name};\n    String name_city = ${TestInput.Name} + ${TestInput.City};\n"+
//		"int age = (int) ${IN0.Age} + 10; ";
//		DataRecord[] inRecords=new DataRecord[1];
//		DataRecord[] outRecords=new DataRecord[1];
//
//		DataRecordMetadata metadata=new DataRecordMetadata("TestInput",DataRecordMetadata.DELIMITED_RECORD);
//		metadata.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
//		metadata.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
//		metadata.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
//		inRecords[0] = new DataRecord(metadata);
//		outRecords[0] = new DataRecord(metadata);
//		inRecords[0].init();
//		outRecords[0].init();
//		CodeParser parser=new CodeParser(new DataRecordMetadata[] {metadata}, new DataRecordMetadata[] {metadata});
//		parser.setSourceCode(string);
//		parser.parse();
//		parser.addTransformCodeStub("Transform");
//		System.out.println(parser.getSourceCode());
//
//		try{
//			parser.saveSourceCode("codeOutput.java");
//		}catch(IOException ex){
//			System.out.println(ex.getMessage());
//		}
//
//	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since
	 * @revision    $Revision$
	 */
	static class Token {
		String token;
		int startOffset;
		int endOffset;


		/**
		 *Constructor for the Token object
		 *
		 * @param  startOffset  Description of the Parameter
		 * @param  endOffset    Description of the Parameter
		 * @param  token        Description of the Parameter
		 */
		Token(int startOffset, int endOffset, String token) {
			this.token = token;
			this.startOffset = startOffset;
			this.endOffset = endOffset;
		}


		/**
		 *  Gets the string attribute of the Token object
		 *
		 * @return    The string value
		 */
		String getString() {
			return token;
		}


		/**
		 *  Description of the Method
		 *
		 * @return    Description of the Return Value
		 */
		int length() {
			return endOffset - startOffset;
		}


		/**
		 *  Gets the startOffset attribute of the Token object
		 *
		 * @return    The startOffset value
		 */
		int getStartOffset() {
			return startOffset;
		}


		/**
		 *  Gets the endOffset attribute of the Token object
		 *
		 * @return    The endOffset value
		 */
		int getEndOffset() {
			return endOffset;
		}


		/**
		 *  Description of the Method
		 *
		 * @return    Description of the Return Value
		 */
		@Override
		public String toString() {
			return "" + token + ":" + startOffset + ":" + endOffset;
		}
	}

	static class FieldReference{
	    static final String OUT_DIRECTION="OUT";
	    static final String IN_DIRECTION="IN";
	    
	    String fieldName;
	    String direction;
	    int		fieldNum;
	    int		recNum;
	    
	    FieldReference(String recName,int recNum,String name,String direction,int num){
	        this.fieldName=name;
	        this.fieldNum=num;
	        this.recNum=recNum;
	        this.direction=direction;
	    }
	    
	   @Override
	public boolean equals(Object obj) {
	        if (obj instanceof FieldReference){
	            final FieldReference cmpTo=(FieldReference)obj;
	            return (cmpTo.fieldNum==this.fieldNum && cmpTo.recNum==this.recNum && 
	                    cmpTo.direction.equalsIgnoreCase(this.direction));
	        }
	        return false;
	    }
	   
	   @Override
	public int hashCode(){
	       return this.fieldNum+this.recNum+direction.hashCode()+fieldName.hashCode();
	   }
	}
}

