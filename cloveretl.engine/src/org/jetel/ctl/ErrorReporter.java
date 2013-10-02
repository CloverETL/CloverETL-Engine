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

import org.jetel.ctl.ASTnode.*;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.util.string.StringUtils;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Sep 24, 2013
 */
public class ErrorReporter {
	
	final static int DEFAULT_TAB_SIZE=6;
	final static int DEFAULT_BUFFER_SIZE = 8192;
	final static int MAX_VALUE_LENGHT = 2048;
	
	TransformLangExecutorRuntimeException runtimeException;
	String sourceCode;
	Stack stack;
	DataRecord[] inputRecords,outputRecords;
	String tabPadding;
	PrintWriter err;
	StringWriter wr;
	
	
	public ErrorReporter(TransformLangExecutorRuntimeException ex,Stack stack,DataRecord[] inputRecords,DataRecord[] outputRecords,String source){
		runtimeException=ex;
		this.sourceCode=source;
		this.stack=stack;
		this.inputRecords=inputRecords;
		this.outputRecords=outputRecords;
		setTabSize(DEFAULT_TAB_SIZE);
		wr=new StringWriter(DEFAULT_BUFFER_SIZE);
		err=new PrintWriter(wr);
	}

	public void setTabSize(int size){
		tabPadding=StringUtils.formatString(new Object[]{""}, new int[]{size});
	}
	
	public ErrorReporter createReport(){
		//find the core exception
		Throwable exc=runtimeException;
		while(exc.getCause()!=null) exc=exc.getCause();
		SimpleNode nodeInError = runtimeException.getNode();
		err.println(runtimeException.getMessage());
		err.format("Caused by: %s -> %s\n",exc.getClass(),exc.getMessage());
		err.println("In operation: "+nodeInError.toString());
		err.println("--------------------vvv code snippet vvv--------------------");
		err.println(getLine(sourceCode, nodeInError.getBegin().getLine(), nodeInError.getBegin().getColumn(), nodeInError.getEnd().getLine(),nodeInError.getEnd().getColumn()));
		err.println(getPointerStr(nodeInError.getBegin().getColumn(), nodeInError.getEnd().getColumn()));
		err.println("--------------------^^^ code snippet ^^^--------------------");
		for(int i=0;i<nodeInError.jjtGetNumChildren();i++){
			process(nodeInError.jjtGetChild(i));
		}
		return this;
	}
	
	public String getReport(){
		return wr.toString();
	}
	
	
	public String getLine(String source, int fromLine, int fromCol,int toLine, int toCol){
		LineNumberReader reader = new LineNumberReader(new StringReader(source));
		try {
			StringBuilder strbuf=new StringBuilder(256);
			String lineS;
			while((lineS=reader.readLine())!=null){
				if (reader.getLineNumber()>=fromLine){
					if (reader.getLineNumber()<=toLine){
					//process formatting chars (mostly tabs)
					if (strbuf.length()>0) strbuf.append("\n");
					strbuf.append(lineS.replaceAll("\t", tabPadding));
					}else{
						break;
					}
				}
		}
			
			return strbuf.toString();
		} catch (IOException e) {
		} catch (IndexOutOfBoundsException e){
		}
		return "";
	}
	
	public String getPointerStr(int from, int to){
		StringBuilder buf=new StringBuilder(180);
		for(int i=0;i<to;i++){
			if (i<from)
				buf.append(' ');
			else
				buf.append('^');
		}
		return buf.toString();
	}

	final String maxstr(String value){
		if(value.length()>MAX_VALUE_LENGHT)
			return value.substring(0,MAX_VALUE_LENGHT)+"...";
		else
			return value;
	}
	
	
	/*----------------
	 * processing of individual node types starts here
	 */

	String process(Node node){
		if (node instanceof CLVFIdentifier){
			return process((CLVFIdentifier)node);
		}else if (node instanceof CastNode){
			return process((CastNode)node);
		}else if (node instanceof CLVFArguments){
			return process((CLVFArguments)node);
		}else if (node instanceof CLVFLiteral){
			return process((CLVFLiteral)node);
		}else if (node instanceof CLVFListOfLiterals){
			return process((CLVFListOfLiterals)node);
		}else if (node instanceof CLVFFieldAccessExpression){
			return process((CLVFFieldAccessExpression)node);
		}else if(node instanceof CLVFArrayAccessExpression){
			return process((CLVFArrayAccessExpression)node);
		}
			return process((SimpleNode)node);
		
	}
	
	
	String process(SimpleNode anynode){
		err.println(anynode.toString());
		return null;
	}
	
	
	String process(CLVFIdentifier node){
		Object value=stack.getVariable(node.getBlockOffset(), node.getVariableOffset());
		if (node.getType().isPrimitive() || value==null){
			err.format("variable \"%s\" (%s) -> %s\n",node.getName(),node.getType(),maxstr(value==null ? "null" : value.toString()));
		}else{
			err.format("variable \"%s\" (%s) :\n",node.getName(),node.getType());
			err.println(maxstr(value.toString()));
		}
		return null;
	}
	
	String process(CastNode node){
		process(node.jjtGetChild(0));
		return null;
	}
	
	String process(CLVFArguments node){
		err.println("Call arguments: ");
		for(int j=0;j<node.jjtGetNumChildren();j++){
			err.format("[%d]",(j+1));
			process(node.jjtGetChild(j));
		}
		
		return null;
	}
	
	String process(CLVFLiteral node){
		err.format("literal -> \"%s\"\n",node.getValue());
		return null;
	}
	
	String process(CLVFListOfLiterals node){
		err.println("Literals: ");
		for(int j=0;j<node.jjtGetNumChildren();j++){
			process(node.jjtGetChild(j));
		}
		
		return null;
	}
	
	String process(CLVFFieldAccessExpression node){
		DataRecord record =	node.isOutput() 
				? outputRecords[node.getRecordId()] : inputRecords[node.getRecordId()];
		if (node.getFieldId()!=null){
			final DataField field = record.getField(node.getFieldId());
			err.format("field: \"%s\" -> %s\n",node.getFieldName(),field.toString());
		}else{ //accessing the whole record
			err.format("record: \"%s\"\n",node.getRecordName());
			err.println(record.toString());
		}
		return null;
	}
	
	String process(CLVFArrayAccessExpression node){
		//err.print("array: ");
		process(node.jjtGetChild(0));
		process(node.jjtGetChild(1));
		return null;
	}
	

}
