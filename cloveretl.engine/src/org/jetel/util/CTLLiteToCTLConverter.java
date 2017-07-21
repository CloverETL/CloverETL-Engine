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

/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CodeParser.Token;

/**
 * Based on CodeParser.java
 * 
 * TODO: enter class description here
 * 
 * @author Jakub Lehotsky (jakub.lehotsky@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Jun 11, 2008
 */
public class CTLLiteToCTLConverter {

	private Map<String, Integer> inputRecordsNames;
	private Map<String, Integer> outputRecordsNames;
	private Map<String, Integer>[] inputFieldsNames;
	private Map<String, Integer>[] outputFieldsNames;
	private StringBuffer sourceCode;
	private DataRecordMetadata[] inputRecordsMeta;
	private DataRecordMetadata[] outputRecordsMeta;
	private Map<String, String> sequences = new HashMap<String, String>();
	private Map<String, String> parameters = new HashMap<String, String>();
    
	private final static int SOURCE_CODE_BUFFER_INITIAL_SIZE = 512;
    
    private final static String GET_OPCODE_STR = "${in.";
	private final static String GET_OPCODE_REGEX = "\\$\\{in.";
	
	private final static String SET_OPCODE_STR = "${out.";
	private final static String SET_OPCODE_REGEX = "\\$\\{out.";
	
	private static final String CTL_FIELD_PREFIX = "$";
	private static final Object CTL_PORT_FIELD_DELIMITER = ".";
	
	private final static String PARAM_OPCODE_STR = "${par.";
	private final static String PARAM_OPCODE_REGEX = "\\$\\{par.";
	private final static String CTL_PARAM_PREFIX = "\"${";
	private final static String CTL_PARAM_SUFFIX = "}\"";
	
	private final static String SEQ_OPCODE_STR = "${seq.";
	private final static String SEQ_OPCODE_REGEX = "\\$\\{seq.";
	private final static String CTL_SEQUENCE_PREFIX = "sequence(";
	private final static String CTL_SEQUENCE_SUFFIX = ").next";
	
	private final static String OPCODE_END_STR = "}";

	private final static char GET_OPCODE = 'G';
	private final static char SET_OPCODE = 'S';




	static Log logger = LogFactory.getLog(CTLLiteToCTLConverter.class);

	/**
	 * @param  inputRecords   Description of the Parameter
	 * @param  outputRecords  Description of the Parameter
	 */
	public CTLLiteToCTLConverter(DataRecordMetadata[] inputMetadatas, DataRecordMetadata[] outputMetadatas) {
	    
	    //initialization metadata arrays based on given ports
        this.inputRecordsMeta = inputMetadatas;
        this.outputRecordsMeta = outputMetadatas;

        inputRecordsNames = new HashMap<String, Integer>(inputRecordsMeta.length);
        outputRecordsNames = new HashMap<String, Integer>(outputRecordsMeta.length);
        inputFieldsNames = new HashMap[inputRecordsMeta.length];
        outputFieldsNames = new HashMap[outputRecordsMeta.length];
        // initialize map for input records & fields
        for (int i = 0; i < inputRecordsMeta.length; i++) {
            inputRecordsNames.put(String.valueOf(i), Integer.valueOf(i));
            inputFieldsNames[i] = new HashMap<String,Integer>(inputRecordsMeta[i]
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
            outputFieldsNames[i] = new HashMap<String, Integer>(outputRecordsMeta[i]
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

    /**
     * Sets the sourceCode attribute of the CodeParser object
     * 
     * @param charSeq
     *            The new sourceCode value
     */
    public void setSourceCode(CharSequence charSeq) {
        sourceCode.setLength(0);
        sourceCode.ensureCapacity(charSeq.length());
        sourceCode.append("\t");
        for (int i = 0; i < charSeq.length(); i++) {
            sourceCode.append(charSeq.charAt(i));
            if (charSeq.charAt(i) == '\n') {
                sourceCode.append("\t");
            }
        }
    }

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
                        translateToField(fieldRefStr, false));
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
        // finally all set opcodes (assignments)
        do {
            token = findEnclosedString(0, SET_OPCODE_STR, OPCODE_END_STR);
            if (token != null) {
                fieldRefStr = parseFieldReference(SET_OPCODE, token.getString());
                sourceCode.replace(token.getStartOffset(),
                        token.getEndOffset() + 1,
                        translateToField(fieldRefStr, true));
                //we have to remove [=]
                index = sourceCode.indexOf("=", token.getEndOffset() - 5);
                if (index != -1) {
                	sourceCode.insert(index, ":");
                } else {
                    throw new RuntimeException(
                            "No [=] found when parsing field reference: "
                                    + token);
                }
            }
        } while (token != null);
    }

    private String translateToParam(String fieldRef) {
        if (!parameters.containsKey(fieldRef)) {
            parameters.put(fieldRef, fieldRef);
        }
        return CTL_PARAM_PREFIX + fieldRef + CTL_PARAM_SUFFIX;
    }

    private String translateToSeq(String fieldRef) {
        if (!sequences.containsKey(fieldRef)) {
            sequences.put(fieldRef, fieldRef);
        }
        return CTL_SEQUENCE_PREFIX + fieldRef + CTL_SEQUENCE_SUFFIX;
    }

    private String translateToField(String[] fieldRef, boolean output) {
        Integer recordNum;
        Integer fieldNum;
        StringBuffer code = new StringBuffer(40);

        //if (logger.isDebugEnabled()) {
        //	logger.debug(fieldRef[0]+" : "+fieldRef[1]);
        //}

        recordNum = output ? outputRecordsNames.get(fieldRef[0]) : inputRecordsNames.get(fieldRef[0]);
        if (recordNum == null) {
            throw new RuntimeException((output ? "Output" : "Input") + "record does not exist: " + fieldRef[0]);
        }
        try {
        	if (output) {
        		fieldNum = (Integer) outputFieldsNames[recordNum.intValue()].get(fieldRef[1]);
        	} else {
        		fieldNum = (Integer) inputFieldsNames[recordNum.intValue()].get(fieldRef[1]);
        	}
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new RuntimeException("Nonexisting index to array containing " +
            		(output ? "output" : "input") + " records :" + ex.getMessage());
        }
        if (fieldNum == null) {
            throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in " +
            		(output ? "output" : "input") + " record: " + fieldRef[0]);
        }

        code.append(CTL_FIELD_PREFIX);
		code.append(fieldRef[0]);
		code.append(CTL_PORT_FIELD_DELIMITER);
		code.append(fieldRef[1]);
		
        if ((output && outputRecordsMeta[recordNum.intValue()] == null) ||
        	(!output && inputRecordsMeta[recordNum.intValue()] == null)) {
            throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in "
            		+ (output ? "output" : "input") + " record: " + fieldRef[0]);
        }

        return code.toString();
    }

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

	private String parseParamReference(String fieldRef) {
		fieldRef = fieldRef.replaceFirst(PARAM_OPCODE_REGEX, "");
		fieldRef = fieldRef.replaceFirst("\\}", "");
		//fieldRef = fieldRef.toUpperCase();

		return fieldRef;
	}

	private String parseSeqReference(String fieldRef) {
		fieldRef = fieldRef.replaceFirst(SEQ_OPCODE_REGEX, "");
		fieldRef = fieldRef.replaceFirst("\\}", "");
		//fieldRef = fieldRef.toUpperCase();

		return fieldRef;
	}

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

	public void addTransformCodeStub() {
		
		StringBuffer transCode = new StringBuffer(SOURCE_CODE_BUFFER_INITIAL_SIZE);

		transCode.append("// automatically generated on ");
		transCode.append(new Date()).append("\n");
		transCode.append("function transform() {\n"); 

		sourceCode.insert(0, transCode.toString());
		sourceCode.append("\n}\n");

	}
	
}
