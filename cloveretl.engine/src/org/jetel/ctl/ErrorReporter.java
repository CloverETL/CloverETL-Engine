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
package org.jetel.ctl;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import org.jetel.ctl.ASTnode.*;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.util.ExceptionUtils;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Sep 24, 2013
 */
public class ErrorReporter {
	
	final static int DEFAULT_TAB_WIDTH = 4;
	final static int DEFAULT_BUFFER_SIZE = 8192;
	final static int MAX_VALUE_LENGTH = 512; // CLO-2658: decreased from 2048
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");
	
	TransformLangExecutorRuntimeException runtimeException;
	String sourceCode;
	Stack stack;
	DataRecord[] inputRecords, outputRecords;
	int tabWidth = DEFAULT_TAB_WIDTH;
	PrintWriter err;
	StringWriter wr;
	
	// already printed variables
	private Set<String> identifiers;
	
	public ErrorReporter(TransformLangExecutorRuntimeException ex,Stack stack,DataRecord[] inputRecords,DataRecord[] outputRecords,String source) {
		runtimeException = ex;
		this.sourceCode = source;
		this.stack = stack;
		this.inputRecords = inputRecords;
		this.outputRecords = outputRecords;
		wr = new StringWriter(DEFAULT_BUFFER_SIZE);
		err = new PrintWriter(wr);
	}

	public void setTabWidth(int width) {
		tabWidth = width;
	}
	
	/**
	 * Replaces tabs with a varying number of spaces
	 * (at least one). The number of inserted spaces depends 
	 * on the tab position.
	 * 
	 * @param line input text
	 * @return input text where tabs are replaced with the right amount of spaces 
	 * 
	 * @see #tabWidth
	 * @see #setTabWidth(int)
	 */
	private String replaceTabs(String line) {
		StringBuilder sb = new StringBuilder(line.length() + 10);
		int length = line.length();
		for (int i = 0; i < length; i++) {
			char c = line.charAt(i);
			if (c == '\t') {
				do {
					sb.append(' '); // align to tabWidth, always append at least one space
				} while (sb.length() % tabWidth != 0);
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	public ErrorReporter createReport() {
		//find the core exception
		Throwable exc=runtimeException;
		while (exc.getCause()!=null) exc=exc.getCause();
		SimpleNode nodeInError = runtimeException.getNode();
		err.println(runtimeException.getSimpleMessage());
		err.format("Caused by: %s -> %s%n",exc.getClass().getName(),exc.getMessage());
//		err.println("In operation: "+nodeInError.toString()); // CLO-2658
		if (sourceCode != null) {
			err.println("----------------------- CTL2 snippet -----------------------");
			err.println(getSnippet(sourceCode, nodeInError.getBegin().getLine(), nodeInError.getBegin().getColumn(), nodeInError.getEnd().getLine(),nodeInError.getEnd().getColumn()));
			err.println("----------------------- CTL2 snippet -----------------------");
		} else {
			err.println("(source not available)");
		}
		identifiers = new HashSet<String>();
		for(int i=0;i<nodeInError.jjtGetNumChildren();i++){
			process(nodeInError.jjtGetChild(i));
		}
		identifiers = null;
		// error message should not end with a line break, delete it
		StringBuffer sb = wr.getBuffer();
		if (sb.toString().endsWith(LINE_SEPARATOR)) {
			sb.delete(sb.length() - LINE_SEPARATOR.length(), sb.length());
		}
		return this;
	}
	
	public String getReport() {
		return wr.toString();
	}
	
	public String getSnippet(String source, int fromLine, int fromCol, int toLine, int toCol) {
		int width = String.valueOf(toLine).length();
		String lineNumberFormat = "%" + width + "d: ";
		String lineNumberPadding = String.format("%" + width + "s  ", "");
		LineNumberReader reader = new LineNumberReader(new StringReader(source));
		try {
			StringBuilder strbuf = new StringBuilder(256);
			String lineS;
			while ((lineS = reader.readLine()) != null) {
				int lineNumber = reader.getLineNumber();
				if (lineNumber >= fromLine) {
					if (lineNumber <= toLine) {
						// process formatting chars (mostly tabs)
						String outputLine = replaceTabs(lineS);
						strbuf.append(String.format(lineNumberFormat, lineNumber)).append(outputLine).append(LINE_SEPARATOR);
						String pointerStr;
						if (fromLine == toLine) {
							pointerStr = getPointerStr(fromCol, toCol);
						} else if (lineNumber == fromLine) {
							pointerStr = getPointerStr(fromCol, outputLine.length());
						} else if (lineNumber == toLine) {
							pointerStr = getPointerStr(1, toCol);
						} else {
							pointerStr = getPointerStr(1, outputLine.length());
						}
						strbuf.append(lineNumberPadding).append(pointerStr).append(LINE_SEPARATOR);
					} else {
						break;
					}
				}
			}
			
			if (strbuf.length() > 0) {
				strbuf.delete(strbuf.length() - LINE_SEPARATOR.length(), strbuf.length());
			}
			return strbuf.toString();
		} catch (IOException e) {
			return ExceptionUtils.getMessage("(source not available)", e);
		} catch (IndexOutOfBoundsException e) {
			return ExceptionUtils.getMessage("(source not available)", e);
		}
	}
	
	public String getPointerStr(int from, int to) {
		StringBuilder buf = new StringBuilder(180);
		for (int i = 1; i <= to; i++) {
			if (i < from) {
				buf.append(' ');
			} else {
				buf.append('^');
			}
		}
		return buf.toString();
	}

	final String maxstr(String value) {
		if (value.length() > MAX_VALUE_LENGTH) {
			return value.substring(0, MAX_VALUE_LENGTH) + "...";
		} else {
			return value;
		}
	}
	
	
	/*----------------
	 * processing of individual node types starts here
	 */

	String process(Node node) {
		if (node instanceof CLVFIdentifier) {
			return process((CLVFIdentifier) node);
		} else if (node instanceof CastNode) {
			return process((CastNode) node);
		} else if (node instanceof CLVFArguments) {
			return process((CLVFArguments) node);
		} else if (node instanceof CLVFLiteral) {
			return process((CLVFLiteral) node);
		} else if (node instanceof CLVFListOfLiterals) {
			return process((CLVFListOfLiterals) node);
		} else if (node instanceof CLVFFieldAccessExpression) {
			return process((CLVFFieldAccessExpression) node);
		} else if (node instanceof CLVFArrayAccessExpression) {
			return process((CLVFArrayAccessExpression) node);
		}
		
		return process((SimpleNode) node);
	}
	
	
	String process(SimpleNode anynode) {
//		err.println(anynode.toString()); // CLO-2658
		return null;
	}
	
	
	String process(CLVFIdentifier node) {
		if (identifiers.add(node.getName())) {
			Object value = (stack != null) ? stack.getVariable(node.getBlockOffset(), node.getVariableOffset()) : "(value not available)";
			if (node.getType().isPrimitive() || value==null){
				err.format("variable \"%s\" (%s) -> %s%n",node.getName(),node.getType(),maxstr(value==null ? "null" : value.toString()));
			} else {
				err.format("variable \"%s\" (%s) :%n",node.getName(),node.getType());
				err.println(maxstr(value.toString()));
			}
		}
		return null;
	}
	
	String process(CastNode node) {
		process(node.jjtGetChild(0));
		return null;
	}
	
	String process(CLVFArguments node) {
		err.println("Call arguments: ");
		for(int j=0;j<node.jjtGetNumChildren();j++){
			err.format("[%d] ",(j+1));
			process(node.jjtGetChild(j));
		}
		
		return null;
	}
	
	String process(CLVFLiteral node) {
//		err.format("literal -> \"%s\"%n",node.getValue()); // CLO-2658
		return null;
	}
	
	String process(CLVFListOfLiterals node) {
//		err.println("Literals: "); // CLO-2658
		for(int j=0;j<node.jjtGetNumChildren();j++){
			process(node.jjtGetChild(j));
		}
		
		return null;
	}
	
	String process(CLVFFieldAccessExpression node) {
		DataRecord record =	node.isOutput() 
				? outputRecords[node.getRecordId()] : inputRecords[node.getRecordId()];
		if (node.getFieldId() != null) {
			final DataField field = record.getField(node.getFieldId());
			err.format("field: \"%s\" -> %s%n",node.getFieldName(),field.toString());
		} else { //accessing the whole record
			err.format("record: \"%s\"%n",node.getRecordName());
			err.println(record.toString());
		}
		return null;
	}
	
	String process(CLVFArrayAccessExpression node) {
		//err.print("array: ");
		process(node.jjtGetChild(0));
		process(node.jjtGetChild(1));
		return null;
	}
	

}
