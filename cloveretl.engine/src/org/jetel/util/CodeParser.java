/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
package org.jetel.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.InputPort;
import org.jetel.graph.OutputPort;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
/**
 * The purpose for this class is to handle parsing java code enhanced with
 * cloverETL syntax.  Initially cloverETL syntax will support references
 * to field values. The future enhancement will be to add support for aggregate
 * functions (similar to SQL agregation). <br>
 * <h3>Syntax used</h3>
 * <table>
 * <tr>
 * <td><tt>${<i>record_name</i><b>.</b><i>field_name</i>}</tt></td>
 * <td><p>References record and field within record for reading values from it.</p>
 * <p>Record names can be names assigned to record through record's metadata or
 * <tt>INx</tt> where x runs from 0 to #of input records -1 (e.g. IN0, IN1)</p>
 * <p>Field names are names of individual fields within respective records or
 * numbers denoting index of field within record can be used</p>
 * <i>Note: names are not case sensitive</i></td>
 * </tr>
 * <tr>
 * <td><tt>
 * \@{<i>record_name</i><b>.</b><i>field_name</i>}</tt></td>
 * <td><p>References record and field within record for assigning values to it.</p>
 * <p>Record names can be names assigned to record through record's metadata or
 * <tt>OUTx</tt> where x runs from 0 to #of output records -1 (e.g. OUT0, OUT1)</p>
 * </td>
 *
 * </tr>
 * </table>
 * @author                          Wes Maciorowski, David Pavlis
 * @since
 * @revision                        $Revision$
 * <h4>Example:</h4>
 * <pre> \@{OUT.OrderID}=${IN.OrderID};
 * String= ${IN.OrderID}+${IN.CustomerID}; </pre>
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

	private final static int SOURCE_CODE_BUFFER_INITIAL_SIZE = 512;
	private final static String GET_OPCODE_STR = "${in.";
	private final static String SET_OPCODE_STR = "${out.";
	private final static String GET_OPCODE_REGEX = "\\$\\{in.";
	private final static String SET_OPCODE_REGEX = "\\$\\{out.";
	private final static String GET_REFERENCE_OPCODE_STR = "%{";
	private final static String OPCODE_END_STR = "}";

	private final static String IN_RECORDS_ARRAY_NAME_STR = "inputRecords";
	private final static String OUT_RECORDS_ARRAY_NAME_STR = "outputRecords";

	private final static char GET_OPCODE = 'G';
	private final static char SET_OPCODE = 'S';
	private final static char GET_REFERENCE_OPCODE = 'R';
	private final static char UNKNOWN_OPCODE = (char) -1;

	static Log logger = LogFactory.getLog(CodeParser.class);

	/**
	 * @param  inputRecords   Description of the Parameter
	 * @param  outputRecords  Description of the Parameter
	 */
	public CodeParser(InputPort[] inputPorts, OutputPort[] outputPorts) {
	    
	    //initialization metadata arrays based on given ports
		this.inputRecordsMeta = new DataRecordMetadata[inputPorts.length];
		this.outputRecordsMeta = new DataRecordMetadata[outputPorts.length];
		for(int i = 0; i < inputPorts.length; i++) {
		    inputRecordsMeta[i] = inputPorts[i].getMetadata();
		}
		for(int i = 0; i < outputPorts.length; i++) {
		    outputRecordsMeta[i] = outputPorts[i].getMetadata();
		}
		
		inputRecordsNames = new HashMap(inputRecordsMeta.length);
		outputRecordsNames = new HashMap(outputRecordsMeta.length);
		inputFieldsNames = new HashMap[inputRecordsMeta.length];
		outputFieldsNames = new HashMap[outputRecordsMeta.length];
		// initialize map for input records & fields
		for (int i = 0; i < inputRecordsMeta.length; i++) {
			inputRecordsNames.put(String.valueOf(i), new Integer(i));
			inputFieldsNames[i] = new HashMap(inputRecordsMeta[i].getNumFields());
			for (int j = 0; j < inputRecordsMeta[i].getNumFields(); j++) {
				inputFieldsNames[i].put(inputRecordsMeta[i].getField(j).getName().toUpperCase(),
						new Integer(j)
				/*
				 *  inputRecords.getField(j)
				 */
						);
			}
		}
		// initialize map for output records & fields
		for (int i = 0; i < outputRecordsMeta.length; i++) {
			outputRecordsNames.put(String.valueOf(i), new Integer(i));
			outputFieldsNames[i] = new HashMap(outputRecordsMeta[i].getNumFields());
			for (int j = 0; j < outputRecordsMeta[i].getNumFields(); j++) {
				outputFieldsNames[i].put(outputRecordsMeta[i].getField(j).getName().toUpperCase(),
						new Integer(j)
				/*
				 *  outputRecords.getField(j)
				 */
						);
			}
		}
		sourceCode = new StringBuffer(SOURCE_CODE_BUFFER_INITIAL_SIZE);

	}


	/**
	 *  Gets the sourceCode attribute of the CodeParser object
	 *
	 * @return    The sourceCode value
	 */
	public String getSourceCode() {
		return sourceCode.toString();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  filename         Description of the Parameter
	 * @exception  IOException  Description of the Exception
	 */
	public void saveSourceCode(String filename) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
		for (int i = 0; i < sourceCode.length(); i++) {
			out.write(sourceCode.charAt(i));
		}
		out.close();

	}


	/**
	 *  Sets the sourceCode attribute of the CodeParser object
	 *
	 * @param  charSeq  The new sourceCode value
	 */
	public void setSourceCode(CharSequence charSeq) {
		sourceCode.setLength(0);
		sourceCode.ensureCapacity(charSeq.length());
		for (int i = 0; i < charSeq.length(); i++) {
			sourceCode.append(charSeq.charAt(i));
		}
	}


	/** */
	public void parse() {
		// find all CloverETL tokens first
		String[] fieldRefStr;
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
		// secondly all set opcodes
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
					throw new RuntimeException("No [=] found when parsing field reference: " + token);
				}
				// we have to find [;] and insert parenthesis [)] in front of it
				index = sourceCode.indexOf(";", token.getEndOffset());
				if (index != -1) {
					sourceCode.insert(index, ")");
				} else {
					throw new RuntimeException("No [;] found around when parsing field reference: " + token);
				}
			}
		} while (token != null);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  fieldRef  Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private String translateToFieldGetMethod(String[] fieldRef) {
		Integer recordNum;
		Integer fieldNum;
		char fieldType;
		StringBuffer code = new StringBuffer(40);

		if (logger.isDebugEnabled()) {
			logger.debug(fieldRef[0]+" : "+fieldRef[1]);
		}

		recordNum = (Integer) inputRecordsNames.get(fieldRef[0]);
		try {
			fieldNum = (Integer) inputFieldsNames[recordNum.intValue()].get(fieldRef[1]);
		} catch (ArrayIndexOutOfBoundsException ex) {
			throw new RuntimeException("Nonexisting index to array containing input records :" + ex.getMessage());
		}
		if (recordNum == null) {
			throw new RuntimeException("Input record does not exist: " + fieldRef[0]);
		}
		// code for accessing record
		code.append(IN_RECORDS_ARRAY_NAME_STR).append("[").append(recordNum).append("]");
		// code for accessing field
		code.append(".getField(").append(fieldNum).append(")");

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
				code.insert(0,"((DateDataField)");
				code.append(")");
				code.append(".getDate()");
				break;
			case DataFieldMetadata.NUMERIC_FIELD:
				code.insert(0,"((NumericDataField)");
				code.append(")");
				code.append(".getDouble()");
				break;
			case DataFieldMetadata.INTEGER_FIELD:
				code.insert(0,"((IntegerDataField)");
				code.append(")");
				code.append(".getInt()");
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
	private String translateToFieldSetMethod(String[] fieldRef) {
		Integer recordNum;
		Integer fieldNum;
		char fieldType;
		StringBuffer code = new StringBuffer(40);

		if (logger.isDebugEnabled()) {
			logger.debug(fieldRef[0]+" : "+fieldRef[1]);
		}

		recordNum = (Integer) outputRecordsNames.get(fieldRef[0]);
		if (recordNum == null) {
			throw new RuntimeException("Input record does not exist: " + fieldRef[0]);
		}
		try {
			fieldNum = (Integer) outputFieldsNames[recordNum.intValue()].get(fieldRef[1]);
		} catch (ArrayIndexOutOfBoundsException ex) {
			throw new RuntimeException("Nonexisting index to array containing output records :" + ex.getMessage());
		}
		if (fieldNum == null) {
			throw new RuntimeException("Field does not exist: " + fieldRef[1] + " in output record: " + fieldRef[0]);
		}

		// code for accessing record
		code.append(OUT_RECORDS_ARRAY_NAME_STR).append("[").append(recordNum).append("]");
		// code for accessing field
		code.append(".getField(").append(fieldNum).append(")");

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
		fieldRef = fieldRef.toUpperCase();

		return fieldRef.split("\\.", 2);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  fieldRef  Description of the Parameter
	 * @return           Description of the Return Value
	 */
	private char analyzeFieldReference(String fieldRef) {
		if (fieldRef.startsWith(GET_OPCODE_STR)) {
			return GET_OPCODE;
		} else if (fieldRef.startsWith(SET_OPCODE_STR)) {
			return SET_OPCODE;
		} else {
			return UNKNOWN_OPCODE;
		}

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
		Token token;

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
		StringBuffer transCode = new StringBuffer(40);

		transCode.append("// automatically generated on ");
		transCode.append(java.util.Calendar.getInstance().getTime()).append("\n");
		transCode.append("import org.jetel.data.*; \n");
		transCode.append("import org.jetel.graph.*; \n");
		transCode.append("import org.jetel.metadata.*; \n");
		transCode.append("import org.jetel.component.*; \n");
		
		// add any user specified imports
		if (classImports!=null){
			for(int i=0;i<classImports.length;i++){
				transCode.append("import ").append(classImports[i]).append("; \n");
			}
		}
		
		transCode.append("\n");
		transCode.append("public class ").append(className).append(" extends DataRecordTransform { \n");
		transCode.append("\tpublic ").append(className).append("() {super(\"").append(className).append("\"); }; \n");
		transCode.append("\tpublic boolean transform(DataRecord[] " + IN_RECORDS_ARRAY_NAME_STR + ", DataRecord[] " + OUT_RECORDS_ARRAY_NAME_STR + "){\n");
		transCode.append("\t// user's code STARTs from here !\n");

		sourceCode.insert(0, transCode);

		sourceCode.append("\n\t// user's code ENDs here !\n");
		sourceCode.append("\treturn true;\n");
		sourceCode.append("\t}\n");
		sourceCode.append("}\n");
		sourceCode.append("//end of transform class \n");

	}

	
	public void setClassImports(String classNames[]){
		this.classImports=classNames;
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
	class Token {
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
		public String toString() {
			return "" + token + ":" + startOffset + ":" + endOffset;
		}
	}

}

