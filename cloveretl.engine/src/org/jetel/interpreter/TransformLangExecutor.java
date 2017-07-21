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
package org.jetel.interpreter;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.NullRecord;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.StringDictionaryType;
import org.jetel.interpreter.ASTnode.CLVFAddNode;
import org.jetel.interpreter.ASTnode.CLVFAnd;
import org.jetel.interpreter.ASTnode.CLVFAssignment;
import org.jetel.interpreter.ASTnode.CLVFBlock;
import org.jetel.interpreter.ASTnode.CLVFBreakStatement;
import org.jetel.interpreter.ASTnode.CLVFBreakpointNode;
import org.jetel.interpreter.ASTnode.CLVFCaseExpression;
import org.jetel.interpreter.ASTnode.CLVFComparison;
import org.jetel.interpreter.ASTnode.CLVFContinueStatement;
import org.jetel.interpreter.ASTnode.CLVFDictionaryNode;
import org.jetel.interpreter.ASTnode.CLVFDirectMapping;
import org.jetel.interpreter.ASTnode.CLVFDivNode;
import org.jetel.interpreter.ASTnode.CLVFDoStatement;
import org.jetel.interpreter.ASTnode.CLVFEvalNode;
import org.jetel.interpreter.ASTnode.CLVFForStatement;
import org.jetel.interpreter.ASTnode.CLVFForeachStatement;
import org.jetel.interpreter.ASTnode.CLVFFunctionCallStatement;
import org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration;
import org.jetel.interpreter.ASTnode.CLVFIfStatement;
import org.jetel.interpreter.ASTnode.CLVFIffNode;
import org.jetel.interpreter.ASTnode.CLVFImportSource;
import org.jetel.interpreter.ASTnode.CLVFIncrDecrStatement;
import org.jetel.interpreter.ASTnode.CLVFInputFieldLiteral;
import org.jetel.interpreter.ASTnode.CLVFIsNullNode;
import org.jetel.interpreter.ASTnode.CLVFListOfLiterals;
import org.jetel.interpreter.ASTnode.CLVFLiteral;
import org.jetel.interpreter.ASTnode.CLVFLookupNode;
import org.jetel.interpreter.ASTnode.CLVFMinusNode;
import org.jetel.interpreter.ASTnode.CLVFModNode;
import org.jetel.interpreter.ASTnode.CLVFMulNode;
import org.jetel.interpreter.ASTnode.CLVFNVL2Node;
import org.jetel.interpreter.ASTnode.CLVFNVLNode;
import org.jetel.interpreter.ASTnode.CLVFOperator;
import org.jetel.interpreter.ASTnode.CLVFOr;
import org.jetel.interpreter.ASTnode.CLVFOutputFieldLiteral;
import org.jetel.interpreter.ASTnode.CLVFPostfixExpression;
import org.jetel.interpreter.ASTnode.CLVFPrintErrNode;
import org.jetel.interpreter.ASTnode.CLVFPrintLogNode;
import org.jetel.interpreter.ASTnode.CLVFPrintStackNode;
import org.jetel.interpreter.ASTnode.CLVFRaiseErrorNode;
import org.jetel.interpreter.ASTnode.CLVFRegexLiteral;
import org.jetel.interpreter.ASTnode.CLVFReturnStatement;
import org.jetel.interpreter.ASTnode.CLVFSequenceNode;
import org.jetel.interpreter.ASTnode.CLVFStart;
import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.interpreter.ASTnode.CLVFStatementExpression;
import org.jetel.interpreter.ASTnode.CLVFSubNode;
import org.jetel.interpreter.ASTnode.CLVFSwitchStatement;
import org.jetel.interpreter.ASTnode.CLVFSymbolNameExp;
import org.jetel.interpreter.ASTnode.CLVFTryCatchStatement;
import org.jetel.interpreter.ASTnode.CLVFUnaryExpression;
import org.jetel.interpreter.ASTnode.CLVFVarDeclaration;
import org.jetel.interpreter.ASTnode.CLVFVariableLiteral;
import org.jetel.interpreter.ASTnode.CLVFWhileStatement;
import org.jetel.interpreter.ASTnode.CLVFWildCardMapping;
import org.jetel.interpreter.ASTnode.Node;
import org.jetel.interpreter.ASTnode.SimpleNode;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLByteArrayValue;
import org.jetel.interpreter.data.TLContainerValue;
import org.jetel.interpreter.data.TLDateValue;
import org.jetel.interpreter.data.TLListValue;
import org.jetel.interpreter.data.TLMapValue;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLObjectValue;
import org.jetel.interpreter.data.TLRecordValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.interpreter.data.TLVariable;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.CharSequenceReader;
import org.jetel.util.string.CloverString;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * Executor of FilterExpression parse tree.
 * 
 * @author dpavlis
 * @since 16.9.2004
 * 
 * Executor of FilterExpression parse tree
 */
public class TransformLangExecutor implements TransformLangParserVisitor,
        TransformLangParserConstants{

    public static final int BREAK_BREAK=1;
    public static final int BREAK_CONTINUE=2;
    public static final int BREAK_RETURN=3;
    
    protected Stack stack;

    protected boolean breakFlag;
    protected int breakType;
    protected Properties globalParameters;
    
    protected DataRecord[] inputRecords;
    protected DataRecord[] outputRecords;
    
    protected Node emptyNode; // used as replacement for empty statements
    
    protected TransformationGraph graph;
    protected Log runtimeLogger;
    protected ExpParser parser;
    
    static Log logger = LogFactory.getLog(TransformLangExecutor.class);
    
    Map<String, Lookup> lookups = new HashMap<String, Lookup>();

    /**
     * Constructor
     */
    public TransformLangExecutor(Properties globalParameters) {
        stack = new Stack();
        breakFlag = false;
        this.globalParameters=globalParameters;
        emptyNode = new SimpleNode(Integer.MAX_VALUE);
    }
    
    public TransformLangExecutor() {
        this(null);
    }
    
    public TransformationGraph getGraph() {
        return graph;
    }

    public void setGraph(TransformationGraph graph) {
        this.graph = graph;
    }

    public Log getRuntimeLogger() {
        return runtimeLogger;
    }

    public void setRuntimeLogger(Log runtimeLogger) {
        this.runtimeLogger = runtimeLogger;
    }

    /**
     * Set input data records for processing.<br>
     * Referenced input data fields will be resolved from
     * these data records.
     * 
     * @param inputRecords array of input data records carrying values
     */
   	@SuppressWarnings(value="EI2")
    public void setInputRecords(DataRecord[] inputRecords){
        this.inputRecords=inputRecords;
        for (DataRecord record : this.inputRecords) {
			if (record == null) record = NullRecord.NULL_RECORD;
		}
    }
    
    /**
     * Set output data records for processing.<br>
     * Referenced output data fields will be resolved from
     * these data records - assignment (in code) to output data field
     * will result in assignment to one of these data records.
     * 
     * @param outputRecords array of output data records for setting values
     */
    @SuppressWarnings(value="EI2")
    public void setOutputRecords(DataRecord[] outputRecords){
        this.outputRecords=outputRecords;
    }
    
    /**
     * Set global parameters which may be reference from within the
     * transformation source code
     * 
     * @param parameters
     */
    public void setGlobalParameters(Properties parameters){
        this.globalParameters=parameters;
    }

    
    /**
     * Allows to store parameter/value on stack from
     * where it can be read by executed script/function.
     * @param obj   Object/value to be stored
     * @since 10.12.2006
     */
    public void setParameter(String obj){
        stack.push(new TLStringValue(obj));
    }
    
    /**
     * Method which returns result of executing parse tree.<br>
     * Basically, it returns whatever object was left on top of executor's
     * stack (usually as a result of last executed expression/operation).<br>
     * It can be called repetitively in order to read all objects from stack.
     * 
     * @return  Object saved on stack or NULL if no more objects are available
     */
    public TLValue getResult() {
        return stack.pop();
    }
    
    /**
     * Return value of globally defined variable determined by slot number.
     * Slot can be obtained by calling <code>TransformLangParser.getGlobalVariableSlot(<i>varname</i>)</code>
     * 
     * @param varSlot
     * @return  Object - depending of Global variable type
     * @since 6.12.2006
     */
    public TLVariable getGlobalVariable(int varSlot){
        return stack.getGlobalVar(varSlot);
    }
    
    
    /**
     * Allows to set value of defined global variable.
     * 
     * @param varSlot
     * @param value
     * @since 6.12.2006
     */
    
    public void setGlobalVariable(int varSlot,TLVariable value){
        stack.storeGlobalVar(varSlot,value);
    }
    
    
    /**
     * Allows to set parser which may be used in "evaL" 
     * 
     * @param parser
     */
    public void setParser(ExpParser parser){
    	this.parser=parser;
    }

    

    /* *********************************************************** */

    /* implementation of visit methods for each class of AST node */

    /* *********************************************************** */
    /* it seems to be necessary to define a visit() method for SimpleNode */

    @Override
	public Object visit(SimpleNode node, Object data) {
//        throw new TransformLangExecutorRuntimeException(node,
//                "Error: Call to visit for SimpleNode");
        return data;
    }

    @Override
	public Object visit(CLVFStart node, Object data) {

        int i, k = node.jjtGetNumChildren();

        for (i = 0; i < k; i++)
            node.jjtGetChild(i).jjtAccept(this, data);

        return data; // this value is ignored in this example
    }
    
    @Override
	public Object visit(CLVFStartExpression node, Object data) {

        int i, k = node.jjtGetNumChildren();

        for (i = 0; i < k; i++)
            node.jjtGetChild(i).jjtAccept(this, data);

        return data; // this value is ignored in this example
    }


    @Override
	public Object visit(CLVFOr node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a=stack.pop();
        
        if (a.type!=TLValueType.BOOLEAN){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }else if (a==TLBooleanValue.TRUE){
        		stack.push(TLBooleanValue.TRUE);
        		return data;
        }
        
        node.jjtGetChild(1).jjtAccept(this, data);
        a=stack.pop();

        if (a.type!=TLValueType.BOOLEAN){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }
        
        stack.push( a==TLBooleanValue.TRUE ? TLBooleanValue.TRUE : TLBooleanValue.FALSE);
        
        return data;
    }

    @Override
	public Object visit(CLVFAnd node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a=stack.pop();

        if (a.type!=TLValueType.BOOLEAN){
            Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }else if (a==TLBooleanValue.FALSE){
            stack.push(TLBooleanValue.FALSE);
            return data;
        }

        node.jjtGetChild(1).jjtAccept(this, data);
        a=stack.pop();

        if (a.type!=TLValueType.BOOLEAN){
            Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }

        stack.push(a==TLBooleanValue.TRUE ? TLBooleanValue.TRUE : TLBooleanValue.FALSE);
        return data;
    }

    @Override
	public Object visit(CLVFComparison node, Object data) {
        int cmpResult = 2;
        boolean lValue = false;
        TLValue a;
        TLValue b;
        
        switch(node.cmpType){
        case REGEX_EQUAL:
        // special handling for Regular expression
            node.jjtGetChild(0).jjtAccept(this, data);
            TLValue field1 = stack.pop();
            node.jjtGetChild(1).jjtAccept(this, data);
            TLValue field2 = stack.pop();
            if (field1.type == TLValueType.STRING
                    && field2.getValue() instanceof Matcher) {
                Matcher regex = (Matcher) field2.getValue();
                regex.reset(((TLStringValue)field1).getCharSequence());
                if (regex.matches()) {
                    lValue = true;
                } else {
                    lValue = false;
                }
            } else {
                Object[] arguments = { field1, field2 };
                throw new TransformLangExecutorRuntimeException(node,
                        arguments, "regex equal - wrong type of literal(s)");
            }

            break;
        case IN_OPER:
        	// other types of comparison
        	TLContainerValue list=null;
            node.jjtGetChild(0).jjtAccept(this, data);
            a = stack.pop();
            node.jjtGetChild(1).jjtAccept(this, data);
            b = stack.pop();
            try{
            	list = (TLContainerValue)b;
            }catch(Exception ex){
            	Object[] arguments = { a, b};
                throw new TransformLangExecutorRuntimeException(node,
                        arguments, "in - wrong type of literal(s)", ex);
            	
            }     
        	// SPECIAL hanadling of IN in case a is NULL
        	if (a==TLNullValue.getInstance()){
        		stack.push(TLBooleanValue.FALSE);
                return data;
        	}
        	try{
       			lValue=list.contains(a);
       		}catch(Exception ex){
       			Object[] arguments = { a, b };
                   throw new TransformLangExecutorRuntimeException(node,
                            arguments, "in - incompatible literals/expressions", ex);
       		}
        	break;
   		default:
            // other types of comparison
            node.jjtGetChild(0).jjtAccept(this, data);
            a = stack.pop();
            node.jjtGetChild(1).jjtAccept(this, data);
            b = stack.pop();
            
            if (!a.type.isCompatible(b.type)) {
            	// SPECIAL handling of EQUAL/NON_EQUAL in case a is NULL
            	if (a==TLNullValue.getInstance()) {
            		if (node.cmpType==EQUAL) {
	            		stack.push(TLBooleanValue.FALSE);
	                    return data;
	            	}
            		if (node.cmpType==NON_EQUAL) {
	            		stack.push(TLBooleanValue.TRUE);
	                    return data;
	            	}
            	}
            	
                Object arguments[] = { a, b };
                throw new TransformLangExecutorRuntimeException(node,
                        arguments,
                        "compare - incompatible literals/expressions");
            }
            switch (a.type) {
            case INTEGER:
            case LONG:
            case NUMBER:
            case DECIMAL:
            case DATE:
            case STRING:
            case LIST:
            case MAP:
            case RECORD:
            	try{
                cmpResult = a.compareTo(b);
            	}catch(Exception ex){
            		Object arguments[] = { a, b };
                    throw new TransformLangExecutorRuntimeException(node,
                            arguments,
                            "compare - error during comparison of literals/expressions", ex);
            	}
                break;
            case BOOLEAN:
                if (node.cmpType == EQUAL || node.cmpType == NON_EQUAL) {
                    cmpResult = a.equals(b) ? 0 : -1;
                } else {
                    Object arguments[] = { a, b };
                    throw new TransformLangExecutorRuntimeException(node,
                            arguments,
                            "compare - unsupported comparison operator ["
                                    + tokenImage[node.cmpType]
                                    + "] for literals/expressions");
                }
                break;
            default:
                Object arguments[] = { a, b };
                throw new TransformLangExecutorRuntimeException(node,
                        arguments,
                        "compare - don't know how to compare literals/expressions");
            }

            switch (node.cmpType) {
            case EQUAL:
                if (cmpResult == 0) {
                    lValue = true;
                }
                break;// equal
            case LESS_THAN:
                if (cmpResult == -1) {
                    lValue = true;
                }
                break;// less than
            case GREATER_THAN:
                if (cmpResult == 1) {
                    lValue = true;
                }
                break;// grater than
            case LESS_THAN_EQUAL:
                if (cmpResult <= 0) {
                    lValue = true;
                }
                break;// less than equal
            case GREATER_THAN_EQUAL:
                if (cmpResult >= 0) {
                    lValue = true;
                }
                break;// greater than equal
            case NON_EQUAL:
                if (cmpResult != 0) {
                    lValue = true;
                }
                break;
            default:
                // this should never happen !!!
                logger
                        .fatal("Internal error: Unsupported comparison operator !");
                throw new RuntimeException(
                        "Internal error - Unsupported comparison operator !");
            }
        }
        stack.push(lValue ? TLBooleanValue.TRUE : TLBooleanValue.FALSE);
        return data;
    }

    @Override
	public Object visit(CLVFAddNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        
        if (node.nodeVal==null) {
        	if (a!=TLNullValue.getInstance()){
        		node.nodeVal=a.duplicate();
        	}else if (b!=TLNullValue.getInstance()){
        		node.nodeVal=b.duplicate();
        	}else{
        		throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                "add - NULL values not allowed");
        	}
        }

        try {
            if (a.type.isNumeric() && b.type.isNumeric()) {
                node.nodeVal.setValue(a);
                ((TLNumericValue)node.nodeVal).add(((TLNumericValue)b).getNumeric());
                stack.push(node.nodeVal);
            } else if (a.type==TLValueType.DATE && b.type.isNumeric()) {
                Calendar result = Calendar.getInstance();
                result.setTime(((TLDateValue)a).getDate());
                result.add(Calendar.DATE, ((TLNumericValue)b).getInt());
                ((TLDateValue)node.nodeVal).getDate().setTime(result.getTimeInMillis());
                stack.push(node.nodeVal);
            } else if (a.type==TLValueType.STRING) {
                //CharSequence a1 = ((TLStringValue)a).getCharSequence();
                CloverString buf = (CloverString)node.nodeVal.getValue();
                buf.setLength(0);
                buf.append(a.getValue());
                if (b.type==TLValueType.STRING) {
                    buf.append(b.getValue());
                } else {
                    buf.append(b);
                }
                stack.push(node.nodeVal);
            } else {
                Object[] arguments = { a, b };
                throw new TransformLangExecutorRuntimeException(node,arguments,
                        "add - wrong type of literal(s)");
            }
        } catch (ClassCastException ex) {
            Object arguments[] = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "add - wrong type of literal(s)", ex);
        }

        return data;
    }

    @Override
	public Object visit(CLVFSubNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
       

        if (a==TLNullValue.getInstance() || b==TLNullValue.getInstance()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "sub - NULL value not allowed");
        }

        if (!b.type.isNumeric()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { b },
                    "sub - wrong type of literal");
        }

        if (node.nodeVal==null) {
        		node.nodeVal=a.duplicate();
        }
        
        if(a.type.isNumeric()) {
            node.nodeVal.setValue(a);
            ((TLNumericValue)node.nodeVal).sub(((TLNumericValue)b).getNumeric());
            stack.push(node.nodeVal);
        } else if (a.type==TLValueType.DATE) {
            Calendar result = Calendar.getInstance();
            result.setTime(((TLDateValue)a).getDate());
            result.add(Calendar.DATE, ((TLNumericValue)b).getInt() * -1);
            ((TLDateValue)node.nodeVal).getDate().setTime(result.getTimeInMillis());
            stack.push(node.nodeVal);
        } else {
            Object[] arguments = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "sub - wrong type of literal(s)");
        }

        return data;
    }

    @Override
	public Object visit(CLVFMulNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        
        if (a==TLNullValue.getInstance()|| b==TLNullValue.getInstance()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mul - NULL value not allowed");
        }

        if (!a.type.isNumeric() && !b.type.isNumeric()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a,b },
                    "mul - wrong type of literals");
        }

        if (node.nodeVal==null) {
            node.nodeVal=a.duplicate();
        }else{
        	node.nodeVal.setValue(a);
        }
        ((TLNumericValue)node.nodeVal).mul(((TLNumericValue)b).getNumeric()); //TODO: hack due to IntegerDecimal problem..
        stack.push(node.nodeVal);
        return data;
        
    }

    @Override
	public Object visit(CLVFDivNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        

        if (a==TLNullValue.getInstance()|| b==TLNullValue.getInstance()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "div - NULL value not allowed");
        }

        if (!a.type.isNumeric() && !b.type.isNumeric()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a,b },
                    "div - wrong type of literals");
        }

        if (node.nodeVal==null || node.nodeVal.type!=a.type) {
            node.nodeVal=a.duplicate();
        }else{
        	node.nodeVal.setValue(a);
        }
        
        try {
            ((TLNumericValue)node.nodeVal).div(((TLNumericValue)b).getNumeric()); //TODO: hack due to IntegerDecimal problem.
        }catch(ArithmeticException ex){
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a,b },
                    "div - arithmetic exception",ex);
        }catch (Exception ex) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a,b },
            "div - error during operation",ex);
        }
        stack.push(node.nodeVal);
        return data;
    }

    @Override
	public Object visit(CLVFModNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        

        if (a==TLNullValue.getInstance()|| b==TLNullValue.getInstance()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mod - NULL value not allowed");
        }

        if (!a.type.isNumeric() && !b.type.isNumeric()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mod - wrong type of literals");
        }
        
        if (node.nodeVal==null) {
            node.nodeVal=a.duplicate();
        }else{
        	node.nodeVal.setValue(a);
        }
      
        ((TLNumericValue)node.nodeVal).mod(((TLNumericValue)b).getNumeric()); //TODO: hack due to IntegerDecimal problem.
        stack.push(node.nodeVal);
        return data;
    }

     @Override
	public Object visit(CLVFIsNullNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value==TLNullValue.getInstance()) {
            stack.push(TLBooleanValue.TRUE);
        } else {
            if (value.type==TLValueType.STRING) {
                stack.push( ((TLStringValue)value).getCharSequence().length()==0 ? TLBooleanValue.TRUE : TLBooleanValue.FALSE);
            }else {
                stack.push(TLBooleanValue.FALSE);
            }
        }

        return data;
    }

    @Override
	public Object visit(CLVFNVLNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value==TLNullValue.getInstance()) {
            node.jjtGetChild(1).jjtAccept(this, data);
        } else {
            if (value.type==TLValueType.STRING && ((TLStringValue)value).length()==0) {
                node.jjtGetChild(1).jjtAccept(this, data);
            }else {
                stack.push(value);
            }
        }

        return data;
    }
    
    @Override
	public Object visit(CLVFNVL2Node node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value==TLNullValue.getInstance() || (value.type==TLValueType.STRING && ((TLStringValue)value).length()==0)) {
            node.jjtGetChild(2).jjtAccept(this, data);
        } else {
            node.jjtGetChild(1).jjtAccept(this, data);
        }

        return data;
    }

    @Override
	public Object visit(CLVFLiteral node, Object data) {
        stack.push(node.value);
        return data;
    }

    @Override
	public Object visit(CLVFInputFieldLiteral node, Object data) {
    	if (inputRecords == null) {
			throw new TransformLangExecutorRuntimeException(node, "Cannot access input fields within this scope!");
    	}

    	DataRecord record = inputRecords[node.recordNo];
    	int fieldNo=-1;
		if (record == NullRecord.NULL_RECORD || record == null) {
			stack.push(TLNullValue.getInstance());
			return null;
		}
		
		if (node.indexSet){ 
			node.childrenAccept(this, data);
			TLValue val=stack.pop();
			try{
				fieldNo=val.getNumeric().getInt();
			}catch(Exception ex){
				throw new TransformLangExecutorRuntimeException(node,new Object[] {val},"invalid field index", ex);
			}
		}

		if (node.fieldNo < 0) { // record context
			if (node.value == null) {
				if (node.indexSet){
					try{
						node.value = TLValue.convertValue(record.getField(fieldNo));
					}catch(Exception ex){
						throw new TransformLangExecutorRuntimeException(node, "field index ("+fieldNo +") out of bounds", ex);
					}
				}else{
					node.value = new TLRecordValue(record);
				}
			} else {
				if (node.indexSet){
					try{
						node.value = TLValue.convertValue(record.getField(fieldNo));
					}catch(Exception ex){
						throw new TransformLangExecutorRuntimeException(node, "field index ("+fieldNo +") out of bounds", ex);
					}
				}else{
					node.value.setValue(record);
				}
			}

			stack.push(node.value);

			// we return reference to DataRecord so we can
			// perform extra checking in special cases
			return record;

		} else {
			node.field = record.getField(node.fieldNo);
			if (node.field.isNull()) {
				stack.push(TLNullValue.getInstance());
				return null;
			}

			if (node.value == null  || node.field.getMetadata().getType() == DataFieldMetadata.BOOLEAN_FIELD) {
				// since TLBooleanValue is immutable, we have to pass correct reference
				node.value = TLValue.convertValue(node.field);
			} else {
				node.value.setValue(node.field);
			}
			stack.push(node.value);

			// we return reference to DataField so we can
			// perform extra checking in special cases
			return node.field;
		}
        
    }

    public Object visit(CLVFOutputFieldLiteral node, Object data) {
        //stack.push(inputRecords[node.recordNo].getField(node.fieldNo));
        // we return reference to DataField so we can
        // perform extra checking in special cases
        return data;
    }
    
 
    
    @Override
	public Object visit(CLVFRegexLiteral node, Object data) {
        stack.push(new TLObjectValue(node.matcher));
        return data;
    }
   
 
    public Object visit(CLVFMinusNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value.type.isNumeric()) {
            TLNumericValue newVal=(TLNumericValue)value.duplicate();
            newVal.getNumeric().mul(Stack.NUM_MINUS_ONE_P);
            stack.push(newVal);
        } else {
            Object arguments[] = { value };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "minus - not a number");
        }

        return data;
    }

    
    @Override
	public Object visit(CLVFIffNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue condition = stack.pop();

        if (condition.type==TLValueType.BOOLEAN) {
            if (condition==TLBooleanValue.TRUE) {
                node.jjtGetChild(1).jjtAccept(this, data);
            } else {
                node.jjtGetChild(2).jjtAccept(this, data);
            }
            stack.push(stack.pop());
        } else {
            Object[] arguments = { condition };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "iif - condition does not evaluate to BOOLEAN value");
        }

        return data;
    }

    @Override
	public Object visit(CLVFPrintErrNode node, Object data) {
        node.childrenAccept(this, data);
        boolean printLocationFlag = false;
        
        // interpret optional parameter
        if (node.jjtGetNumChildren() > 1) {
	        TLValue printLocation = stack.pop();
	        
	        if (printLocation.type != TLValueType.BOOLEAN) {
	        	throw new TransformLangExecutorRuntimeException(node,new Object[]{printLocation},
	            "print_err - the second argument does not evaluate to a BOOLEAN value");
	        }
	        
	        printLocationFlag = (Boolean)printLocation.getValue();
        }
        
        TLValue message = stack.pop();
        if (printLocationFlag) {
            StringBuilder buf=new StringBuilder((message != null ? message.toString() : "<null>"));
            buf.append(" (on line: ").append(node.getLineNumber());
            buf.append(" col: ").append(node.getColumnNumber()).append(")");
            System.err.println(buf);
        }else{
            System.err.println(message != null ? message : "<null>");
        }

        return data;
    }

    @Override
	public Object visit(CLVFPrintStackNode node, Object data) {
        for (int i=stack.top;i>=0;i--){
            System.err.println("["+i+"] : "+stack.stack[i]);
        }
        
    	System.out.println("** list of local variables ***");
    	for (int i=0;i<stack.localVarCounter;i++)
    		System.out.println(stack.localVarSlot[stack.localVarSlotOffset+i]);

        return data;
    }

    
    /***************************************************************************
     * Transformation Language executor starts here.
     **************************************************************************/

    @Override
	public Object visit(CLVFForStatement node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data); // set up of the loop
        boolean condition = false;
        Node loopCondition = node.jjtGetChild(1);
        Node increment = node.jjtGetChild(2);
        Node body;
        
        try{
            body=node.jjtGetChild(3); 
        }catch(ArrayIndexOutOfBoundsException ex){
            body=emptyNode;
        }

        loopCondition.jjtAccept(this, data); // evaluate the condition
        TLValue conVal=stack.pop();
        try{
            if (conVal.type!=TLValueType.BOOLEAN)
            	throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
            condition = (conVal==TLBooleanValue.TRUE);
        }catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition", ex);
        }

        // loop execution
        while (condition) {
            body.jjtAccept(this, data);
            stack.pop(); // in case there is anything on top of stack
            // check for break or continue statements
            if (breakFlag){ 
                breakFlag=false;
                if (breakType==BREAK_BREAK || breakType==BREAK_RETURN) {
                    return data;
                }
            }
            increment.jjtAccept(this, data);
            stack.pop(); // in case there is anything on top of stack
            // evaluate the condition
            loopCondition.jjtAccept(this, data);
            condition = (stack.pop()==TLBooleanValue.TRUE);
        }

        return data;
    }

    @Override
	public Object visit(CLVFForeachStatement node, Object data) {
        CLVFVariableLiteral varNode=(CLVFVariableLiteral)node.jjtGetChild(0);
        CLVFVariableLiteral arrayNode=(CLVFVariableLiteral)node.jjtGetChild(1);
        TLVariable variableToAssign = stack.getVar(varNode.localVar,
                varNode.varSlot);
        TLVariable arrayVariable=stack.getVar(arrayNode.localVar,
                arrayNode.varSlot);
        Node body;
        try{
            body=node.jjtGetChild(2); 
        }catch(ArrayIndexOutOfBoundsException ex){
            body=emptyNode;
        }
        switch(arrayVariable.getType()) {
        case LIST:
        case RECORD:
        case BYTE:
        	TLContainerValue container=(TLContainerValue)arrayVariable.getTLValue();
            for(int i=0; i<container.getLength();i++) {
                variableToAssign.setTLValue(container.getStoredValue(i));
                body.jjtAccept(this, data);
                stack.pop(); // in case there is anything on top of stack
                // check for break or continue statements
                if (breakFlag){ 
                    breakFlag=false;
                    if (breakType==BREAK_BREAK || breakType==BREAK_RETURN) {
                        return data;
                    }
                }
            }
            break;
        case MAP:
        	Iterator<TLValue> iter = ((TLContainerValue)arrayVariable.getTLValue()).getCollection().iterator();
            while(iter.hasNext()) {
                variableToAssign.setTLValue(iter.next());
                body.jjtAccept(this, data);
                stack.pop(); // in case there is anything on top of stack
                // check for break or continue statements
                if (breakFlag){ 
                    breakFlag=false;
                    if (breakType==BREAK_BREAK || breakType==BREAK_RETURN) {
                        return data;
                    }
                }
            }
            break;
        default:
                throw new TransformLangExecutorRuntimeException(node,"not a Map/List/Record/ByteArray variable");
        }
        return data;
    }

    
    @Override
	public Object visit(CLVFWhileStatement node, Object data) {
        boolean condition = false;
        Node loopCondition = node.jjtGetChild(0);
        Node body;
        try{
            body=node.jjtGetChild(1);
        }catch(ArrayIndexOutOfBoundsException ex){
            body=emptyNode;
        }

       loopCondition.jjtAccept(this, data); // evaluate the condition
       TLValue conVal=stack.pop();
       try{
            if (conVal.type!=TLValueType.BOOLEAN)
            	throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
            condition = (conVal==TLBooleanValue.TRUE);
        }catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition", ex);
        }

        // loop execution
        while (condition) {
            body.jjtAccept(this, data);
            stack.pop(); // in case there is anything on top of stack
            // check for break or continue statements
            if (breakFlag){ 
                breakFlag=false;
                if (breakType==BREAK_BREAK || breakType==BREAK_RETURN) return data;
            }
            // evaluate the condition
            loopCondition.jjtAccept(this, data);
            condition = (stack.pop()==TLBooleanValue.TRUE);
        }

        return data;
    }

    @Override
	public Object visit(CLVFIfStatement node, Object data) {
        boolean condition = false;
        node.jjtGetChild(0).jjtAccept(this, data); // evaluate the
        TLValue conVal=stack.pop();
        try{
            if (conVal.type!=TLValueType.BOOLEAN)
            	throw new TransformLangExecutorRuntimeException(node,"if condition does not evaluate to BOOLEAN value");
            condition = (conVal==TLBooleanValue.TRUE);
        } catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition", ex);
        }

        // first if
        if (condition) {
            node.jjtGetChild(1).jjtAccept(this, data);
        } else { // if else part exists
            if (node.jjtGetNumChildren() > 2) {
                node.jjtGetChild(2).jjtAccept(this, data);
            }
        }

        return data;
    }

    @Override
	public Object visit(CLVFDoStatement node, Object data) {
        boolean condition = false;
        Node loopCondition = node.jjtGetChild(1);
        Node body = node.jjtGetChild(0);

        // loop execution
        do {
            body.jjtAccept(this, data);
            stack.pop(); // in case there is anything on top of stack
            // check for break or continue statements
            if (breakFlag){ 
                breakFlag=false;
                if (breakType==BREAK_BREAK || breakType==BREAK_RETURN) return data;
            }
            // evaluate the condition
            loopCondition.jjtAccept(this, data);
            TLValue conVal=stack.pop();
            try{
            	if (conVal.type!=TLValueType.BOOLEAN)
            		throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
            	condition = (conVal==TLBooleanValue.TRUE);
            }catch (NullPointerException ex){
                throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition", ex);
            }
        } while (condition);

        return data;
    }

    @Override
	public Object visit(CLVFSwitchStatement node, Object data) {
        // get value of switch && push/leave it on stack
        boolean match=false;
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue switchVal=stack.pop();
        int numChildren = node.jjtGetNumChildren();
        int numCases = node.hasDefaultClause ? numChildren-1 : numChildren;
        // loop over remaining case statements
        for (int i = 1; i < numCases; i++) {
            stack.push(switchVal);
            if (node.jjtGetChild(i).jjtAccept(this, data)==TLBooleanValue.TRUE){
                match=true;
            }
            if (breakFlag) {
                if (breakType == BREAK_BREAK) {
                    breakFlag = false;
                }
                break;
            }
           
        }
        // test whether execute default branch
        if (node.hasDefaultClause && !match){
            node.jjtGetChild(numChildren-1).jjtAccept(this, data);
        }
        return data;
    }

    @Override
	public Object visit(CLVFCaseExpression node, Object data) {
        // test if literal (as child 0) is equal to data on stack
        // if so, execute block (child 1)
        boolean match = false;
        TLValue switchVal = stack.pop();
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();
        try {
        	match=(switchVal.compareTo(value)==0);
        } catch (ClassCastException ex) {
            Object[] args=new Object[] {switchVal,value};
            throw new TransformLangExecutorRuntimeException(node,args,"incompatible literals in case clause", ex);
        }catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid case value", ex);
        }catch (IllegalArgumentException ex){
        	Object[] args=new Object[] {switchVal,value};
            throw new TransformLangExecutorRuntimeException(node,args,"incompatible literals in case clause", ex);
        }
        if (match){
            node.jjtGetChild(1).jjtAccept(this, data);
            return TLBooleanValue.TRUE;
        }
        return TLBooleanValue.FALSE;
    }

	@Override
	public Object visit(CLVFTryCatchStatement node, Object data) {
		try {
			node.jjtGetChild(0).jjtAccept(this, data); // evaluate the
		} catch (Exception ex) {
			if (node.jjtGetNumChildren() > 2) {
				// populate chosen variable with exception name

				CLVFVariableLiteral varLit = (CLVFVariableLiteral) node.jjtGetChild(1);
				TLVariable var = stack.getVar(varLit.localVar, varLit.varSlot);
				if (var.getType() != TLValueType.STRING) {
					throw new TransformLangExecutorRuntimeException(node, "variable \"" + var.getName() + "\" is not of type string in catch() block");
				}
				
				var.getTLValue().setValue(ex.getCause()== null ? ex.getClass().getName() : ex.getCause().getClass().getName());

				// call the catch block when variable is present
				node.jjtGetChild(2).jjtAccept(this, data);
			} else {
				// call the catch block - simple variant
				node.jjtGetChild(1).jjtAccept(this, data);
			}
		}

		return data;
	}

    

    @Override
	public Object visit(CLVFIncrDecrStatement node, Object data) {
        Node childNode = node.jjtGetChild(0);
            CLVFVariableLiteral varNode=(CLVFVariableLiteral) childNode;
            TLVariable var=stack.getVar(varNode.localVar, varNode.varSlot);
            
            if (var.getType().isNumeric()) {
                ((TLNumericValue)var.getTLValue()).getNumeric().add( node.kind==INCR ? Stack.NUM_ONE_P : Stack.NUM_MINUS_ONE_P);
            }else if (var.getType()==TLValueType.DATE) {
                stack.calendar.setTime(((TLDateValue)var.getTLValue()).getDate());
                stack.calendar.add(Calendar.DATE, node.kind == INCR ? 1 : -1);
                var.getTLValue().setValue(stack.calendar.getTime());
            }else {
                throw new TransformLangExecutorRuntimeException(node,"variable is not of numeric or date type");
            }
        
        return data;
    }

    @Override
	public Object visit(CLVFBlock node, Object data) {
        int childern = node.jjtGetNumChildren();
        for (int i = 0; i < childern; i++) {
            node.jjtGetChild(i).jjtAccept(this, data);
            // have we seen contiue/break/return statement ??
            if (breakFlag){ 
            	if (breakType!=BREAK_RETURN)
            		stack.pop();
                return data;
            }
            stack.pop();
        }
        return data;
    }

    /*
     * Loop & block & function control nodes
     */

    @Override
	public Object visit(CLVFBreakStatement node, Object data) {
        breakFlag = true; // we encountered break statement;
        breakType=BREAK_BREAK;
        return data;
    }

    @Override
	public Object visit(CLVFContinueStatement node, Object data) {
        breakFlag = true; // we encountered continue statement;
        breakType= BREAK_CONTINUE;
        return data;
    }

    @Override
	public Object visit(CLVFReturnStatement node, Object data) {
        if (node.jjtHasChildren()){
            node.jjtGetChild(0).jjtAccept(this, data);
        }
        breakFlag = true;
        breakType = BREAK_RETURN;
        return data;
    }

    @Override
	public Object visit(CLVFBreakpointNode node, Object data) {
        // list all variables
    	System.err.println("** list of global variables ***");
    	for (int i=0;i<stack.globalVarSlot.length;System.out.println(stack.globalVarSlot[i++]));
    	System.err.println("** list of local variables ***");
    	for (int i=0;i<stack.localVarCounter;i++)
    		System.out.println(stack.localVarSlot[stack.localVarSlotOffset+i]);
    	
    	
        return data;
    }
    
    /*
     * Variable declarations
     */
    @Override
	public Object visit(CLVFVarDeclaration node, Object data) {
        TLValue value=null;
        // create global/local variable
        switch (node.type) {
        case INT_VAR:
            value = new TLNumericValue(TLValueType.INTEGER);
            break;
        case LONG_VAR:
            value = new TLNumericValue(TLValueType.LONG);
            break;
        case DOUBLE_VAR:
            value = new TLNumericValue(TLValueType.NUMBER);
            break;
        case DECIMAL_VAR:
        {
            if (node.length > 0) {
                if (node.precision > 0) {
                    value = new TLNumericValue(TLValueType.DECIMAL, DecimalFactory.getDecimal(node.length,
                            node.precision));
                } else {
                    value = new TLNumericValue(TLValueType.DECIMAL,DecimalFactory.getDecimal(node.length, 0));
                }
            } else {
                value = new TLNumericValue(TLValueType.DECIMAL,DecimalFactory.getDecimal());
            }
            ((TLNumericValue)value).getValue().setValue(0.0d);
        }
            break;
        case STRING_VAR:
            value = new TLStringValue();
            break;
        case DATE_VAR:
            value = new TLDateValue();
            break;
        case BOOLEAN_VAR:
            value = TLBooleanValue.getInstance(false);
            break;
        case BYTE_VAR:
        {
            if (node.length>0) {
                value = new TLByteArrayValue(node.length);
            }else {
                value = new TLByteArrayValue();
            }
        }   
        	break;
        case LIST_VAR:
        {
            if (node.length>0) {
                value = new TLListValue(node.length);
                ((TLListValue)value).fill(TLNullValue.getInstance(), node.length);
            }else {
                value = new TLListValue();
            }
        }   
            break;
        case MAP_VAR:
        {
            if (node.length>0){
                value = new TLMapValue(node.length);
            }else {
                value = new TLMapValue();
            }
        }
            break;
        case RECORD_VAR:
			DataRecordMetadata metadata = null;
			if (node.recordNo >= 0) {
				metadata = parser.getInRecordMeta(node.recordNo);
			} else {
				try {
					metadata = graph.getDataRecordMetadata(node.metadataId, true);
				} catch (Exception ex) {
					throw new TransformLangExecutorRuntimeException(node, "error in Record declaration", ex);
				}
			}
			if (metadata == null) {
				throw new TransformLangExecutorRuntimeException(node, "record variable declaration - " + "can't find metadata ID \"" + (node.metadataId != null ? node.metadataId : "<unknown ID>") + "\"");
			}
			value = new TLRecordValue(metadata);
			break;
        default:
            throw new TransformLangExecutorRuntimeException(node,
                    "variable declaration - "
                            + "unknown type for variable \""
                            + node.name + "\"");

        }
        
        TLVariable variable=new TLVariable(node.name,value);
        stack.storeVar(node.localVar, node.varSlot,variable );

        if (node.hasInitValue) {
        	// can have spec node & initialization
            node.jjtGetChild(node.jjtGetNumChildren()>1 ?  1 : 0).jjtAccept(this, data);
            TLValue initValue = stack.pop();
            TLValueType type =variable.getType();
            if (type.isCompatible(initValue.type)) {
                  variable.setTLValue(initValue);
            }else {
                throw new TransformLangExecutorRuntimeException(node,
                        "invalid assignment of \"" + initValue
                                + "\" ("+initValue.type +")to variable \"" + node.name
                                + "\" ("+type +")- incompatible data types");
            }
        }
        return data;
    }

    @Override
	public Object visit(CLVFVariableLiteral node, Object data) {
        TLVariable var = stack.getVar(node.localVar, node.varSlot);
        TLValue index = null;
        if (node.indexSet) {
            try {
                switch (var.getType()) {
                case LIST:
                    node.jjtGetChild(0).jjtAccept(this, data);
                    index = stack.pop();
                    stack.push(((TLContainerValue)var.getTLValue()).getStoredValue(((TLNumericValue)index).getInt()));
                    break;
                case MAP:
                    node.jjtGetChild(0).jjtAccept(this, data);
                    index = stack.pop();
                    stack.push(((TLContainerValue)var.getTLValue()).getStoredValue(index));
                    break;
                case RECORD:
					if (node.fieldID != null) {
						if (node.arrayIndex == -1) {
								node.arrayIndex = ((DataRecord) var
										.getTLValue().getValue()).getMetadata()
										.getFieldPosition(node.fieldID);
							if (node.arrayIndex==-1) {
								throw new TransformLangExecutorRuntimeException(
										node, "invalid field ID \""
												+ node.fieldID
												+ "\" of variable \""
												+ var.getName() + "\" - type "
												+ var.getType().toString());
							}
						}
						stack.push(((TLContainerValue) var.getTLValue())
								.getStoredValue(node.arrayIndex));
					} else {
						node.jjtGetChild(0).jjtAccept(this, data);
						index = stack.pop();
						stack.push(((TLContainerValue) var.getTLValue())
								.getStoredValue(index));
					}
					break;
                case BYTE:
                	node.jjtGetChild(0).jjtAccept(this, data);
                    index = stack.pop();
                    stack.push(((TLContainerValue)var.getTLValue()).getStoredValue(((TLNumericValue)index).getInt()));
                    break;
                default:
                		throw new TransformLangExecutorRuntimeException(node,"invalid usage if index for variable \""
												+ var.getName() + "\" - type "
												+ var.getType().toString());
				}
            }catch (TransformLangExecutorRuntimeException ex1){
            	throw ex1;
        	}catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(node,
                        "invalid index \"" + index + "\" of variable \""
                        + var.getName() + "\" - type "
                        + var.getType().toString(), ex);
            } 
        }else {
            stack.push(var.getTLValue());
        }
        return var;
    }

    @Override
	public Object visit(CLVFAssignment node, Object data) {
        CLVFVariableLiteral varNode = (CLVFVariableLiteral) node.jjtGetChild(0);

        TLVariable variableToAssign = stack.getVar(varNode.localVar,
                varNode.varSlot);
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue valueToAssign = stack.pop();
        if (valueToAssign==null) {
            throw new TransformLangExecutorRuntimeException(node,
                    "invalid assignment of null value to variable \"" + varNode.varName+"\"");
        }
        
        int actualType = varNode.varType;
    	
        /*
    	 * Function parameters are of type OBJECT. This is determined in compile time
    	 * However if the function parameter is passing a value of data record,
    	 * map or list, it will never by assigned correctly.
    	 * Therefore we have to determine the type dynamically in runtime.
    	 */
        if (actualType == OBJECT_VAR) {
	    	TLValueType paramType = variableToAssign.getType(); // retrieve actual type
	    	switch (paramType) {
	    	case RECORD:
	    		actualType = RECORD_VAR;
	    		break;
	    	case MAP:
	    		actualType = MAP_VAR;
	    		break;
	    	case LIST:
	    		actualType = LIST_VAR;
	    		break;
	    	}
        }

        switch (actualType) {
        case LIST_VAR:
        	 TLNumericValue index2List = null;
			if (varNode.scalarContext) {
				try {
					if (varNode.indexSet) {
						varNode.jjtGetChild(0).jjtAccept(this, data);
						index2List = (TLNumericValue)stack.pop();
						variableToAssign
								.setTLValue(index2List.getInt(), valueToAssign);
					} else {
						variableToAssign.setTLValue(-1, valueToAssign);
					}
				} catch (IndexOutOfBoundsException ex) {
					throw new TransformLangExecutorRuntimeException(
							node,
							"index \""
									+ index2List
									+ "\" is outside current limits of list/array: \""
									+ varNode.varName + "\"", ex);
				} catch (Exception ex) {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of \"" + valueToAssign
									+ "\" to variable \"" + varNode.varName
									+ "\"", ex);
				}
			} else {
				// list context
				if (valueToAssign.type.isArray() ) {
					variableToAssign.setTLValue(valueToAssign);
				} else {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of value \""+valueToAssign+"\" to list/array \""
									+ varNode.varName + "\"");
				}
			}

			break;
        case RECORD_VAR:
			TLValue fieldIndex = null;
			if (varNode.scalarContext) {
				try {
					if (varNode.fieldID != null) {
						if (varNode.arrayIndex == -1) {
							varNode.arrayIndex = ((DataRecord) variableToAssign
									.getTLValue().getValue()).getMetadata()
									.getFieldPosition(varNode.fieldID);
							
							// check if the referenced field exists and we were able to resolve it
							if (varNode.arrayIndex == -1) {
								final String fieldName = varNode.fieldID == null ? "null" : varNode.fieldID; 
								throw new TransformLangExecutorRuntimeException(node,
										"referenced field \"" + fieldName + "\" does not exist");
							}
						}
						((TLContainerValue) variableToAssign.getTLValue())
								.setStoredValue(varNode.arrayIndex,
										valueToAssign);
					} else {
						varNode.jjtGetChild(0).jjtAccept(this, data);
						fieldIndex = stack.pop();
						((TLContainerValue) variableToAssign.getTLValue())
								.setStoredValue(fieldIndex, valueToAssign);
					}
				} catch (Exception ex) {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of \"" + valueToAssign
									+ "\" to variable \"" + varNode.varName
									+ "\"", ex);
				}
			} else {
				try {
					variableToAssign.getTLValue().setValue(valueToAssign);
				} catch (Exception ex) {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of \"" + valueToAssign
									+ "\" to variable \"" + varNode.varName
									+ "\"", ex);
				}
			}
			break;
        case MAP_VAR:
        	TLValue indexMap=null;
        	if (varNode.scalarContext) {
				if (varNode.indexSet) {
					try {
						varNode.jjtGetChild(0).jjtAccept(this, data);
						indexMap = stack.pop();
						((TLContainerValue)variableToAssign.getTLValue()).setStoredValue(indexMap,valueToAssign);
					} catch (Exception ex) {
						throw new TransformLangExecutorRuntimeException(node,
								"invalid assignment of \"" + valueToAssign
										+ "\" to variable \"" + varNode.varName
										+ "\"", ex);
					}
				} else {
					if (valueToAssign instanceof TLContainerValue)
						((TLContainerValue)variableToAssign.getTLValue()).setValue(valueToAssign);
					// no key specified, 
					else throw new TransformLangExecutorRuntimeException(node,
							"no key defined when assigning to Map variable \"" + varNode.varName);
				}
			} else {
				try {
					variableToAssign.getTLValue().setValue(valueToAssign);
				} catch (Exception ex) {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of \"" + valueToAssign
									+ "\" to variable \"" + varNode.varName
									+ "\"", ex);
				}
			}
			break;
        case BYTE_VAR:
        	TLNumericValue indexByteArray = null;
        	if (varNode.scalarContext) {
				try {
					// scalar context
					if (varNode.indexSet) {
						varNode.jjtGetChild(0).jjtAccept(this, data);
						indexByteArray = (TLNumericValue)stack.pop();
						((TLContainerValue)variableToAssign.getTLValue())
								.setStoredValue(indexByteArray.getInt(), valueToAssign);
					} else {
						((TLContainerValue)variableToAssign.getTLValue()).setStoredValue(-1, valueToAssign);
					}
				} catch (IndexOutOfBoundsException ex) {
					throw new TransformLangExecutorRuntimeException(
							node,
							"index \""
									+ indexByteArray
									+ "\" is outside current limits byte array \""
									+ varNode.varName + "\"", ex);
				} catch (Exception ex) {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of \"" + valueToAssign
									+ "\" to variable \"" + varNode.varName
									+ "\"", ex);
				}
			} else {
				// list context
				if (valueToAssign.type.isArray() || valueToAssign==TLNullValue.getInstance()) {
					variableToAssign.setTLValue(valueToAssign);
				} else {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of scalar value to byte array \""
									+ varNode.varName + "\"");
				}
			}
        	break;
        default:
            TLValueType type=variableToAssign.getType();
            if (type.isCompatible(valueToAssign.type)) {
            	try{
                    variableToAssign.setTLValue(valueToAssign);
            	}catch(Exception ex){
            		throw new TransformLangExecutorRuntimeException(node,"invalid assignment of \"" + valueToAssign.toString()
                                + "\" [" + valueToAssign.type
                                + "] to variable \""
                                + variableToAssign.getName() + "\" ["
                                + variableToAssign.getType()
                                + "] \" - "+ex.getMessage(),ex);
            	}
            } else {
                throw new TransformLangExecutorRuntimeException(node,
                        "invalid assignment of \"" + valueToAssign.toString()
                                + "\" [" + valueToAssign.type
                                + "] to variable \""
                                + variableToAssign.getName() + "\" ["
                                + variableToAssign.getType()
                                + "] \" - incompatible data types");
            }
        }
        return data;
    }

    
	@Override
	public Object visit(CLVFDirectMapping node, Object data) {
		DataField field = outputRecords[node.recordNo].getField(node.fieldNo);
		TLValue value = null;
		switch (node.mappingType) {
		case MultipleLiteral2Field:
			final int arity = node.arity;
			try {
				// we try till success or no more options
				for (int i = 0; i < arity; i++) {
					node.jjtGetChild(i).jjtAccept(this, data);
					value = stack.pop();
					try {
						value.copyToDataField(field);
						break; // success during assignment, finish looping
					} catch (Exception ex) {
						if (i == arity - 1)
							throw ex;
					}
				}

			} catch (BadDataFormatException ex) {
				if (!outputRecords[node.recordNo].getField(node.fieldNo).getMetadata().isNullable()) {
					throw new TransformLangExecutorRuntimeException(node, "can't assign NULL to \"" + node.fieldName + "\"", ex);
				}

				throw new TransformLangExecutorRuntimeException(node, "bad data when mapping field \"" + node.fieldName + "\" (" + field.getMetadata().getName() + ":" + field.getMetadata().getTypeAsString() + ") - assigning \"" + value + "\" (" + value.type + ")", ex);
			} catch (TransformLangExecutorRuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				String msg = ex.getMessage();
				throw new TransformLangExecutorRuntimeException(node, (msg != null ? msg : "") + " when mapping \"" + node.fieldName + "\" (" + DataFieldMetadata.type2Str(field.getType()) + ") - assigning \"" + value + "\" (" + (value != null ? value.getType().getName() : "unknown type") + ")", ex);

			}
			break;
		case Field2Field:
			try {
				CLVFInputFieldLiteral childNode=((CLVFInputFieldLiteral)node.jjtGetChild(0));
				childNode.bindToField(inputRecords);
				node.srcField=childNode.field;
				field.setValue(node.srcField);
			} catch (BadDataFormatException ex) {
				if (!outputRecords[node.recordNo].getField(node.fieldNo).getMetadata().isNullable() && node.srcField.isNull()) {
					throw new TransformLangExecutorRuntimeException(node, "can't assign NULL to \"" + node.fieldName + "\"", ex);
				}else{
					throw new TransformLangExecutorRuntimeException(node, "bad data when mapping field \"" + node.fieldName + "\" (" + field.getMetadata().getName() + ":" + field.getMetadata().getTypeAsString() + ") - assigning \"" + node.srcField.toString() + 
							"\" (" + node.srcField.getMetadata().getName() + ":" + node.srcField.getMetadata().getTypeAsString() +" )", ex);
				}
			} catch (Exception ex) {
				String msg = ex.getMessage();
				throw new TransformLangExecutorRuntimeException(node, (msg != null ? msg : "") + " when mapping \"" + node.fieldName + "\" (" + DataFieldMetadata.type2Str(field.getType()) + ") - assigning \"" + value + "\" (" + (value != null ? value.getType().getName() : "unknown type") + ")", ex);

			}
			break;
		case Literal2Field:
			try {
				node.jjtGetChild(0).jjtAccept(this, data);
				value = stack.pop();
				value.copyToDataField(field);
			} catch (BadDataFormatException ex) {
				if (!outputRecords[node.recordNo].getField(node.fieldNo).getMetadata().isNullable()) {
					throw new TransformLangExecutorRuntimeException(node, "can't assign NULL to \"" + node.fieldName + "\"",ex);
				}

				throw new TransformLangExecutorRuntimeException(node, "bad data when mapping field \"" + node.fieldName + "\" (" + field.getMetadata().getName() + ":" + field.getMetadata().getTypeAsString() + ") - assigning \"" + value + "\" (" + value.type + ")",ex);
			} catch (TransformLangExecutorRuntimeException ex) {
				throw ex;
			} catch (Exception ex) {
				String msg = ex.getMessage();
				throw new TransformLangExecutorRuntimeException(node, (msg != null ? msg : "") + " when mapping \"" + node.fieldName + "\" (" + DataFieldMetadata.type2Str(field.getType()) + ") - assigning \"" + value + "\" (" + (value != null ? value.getType().getName() : "unknown type") + ")", ex);

			}
			break;
		default:
			// this should not happen
			throw new TransformLangExecutorRuntimeException(node, "unrecognized mapping type (internal error)");
		}

		return data;
	}

    @Override
	public Object visit(CLVFWildCardMapping node, Object data) {
    	if (!node.initialized) {
			try {
				node.custTrans.setLogger(logger);
				node.custTrans.init(null, parser.getInRecordMetadata(), parser
						.getOutRecordMetadata());
			} catch (ComponentNotReadyException ex) {
				throw  new TransformLangExecutorRuntimeException(node,ex.getMessage(),ex);
			}
			node.initialized = true;
		}
		try {
			node.custTrans.transform(inputRecords, outputRecords);
		} catch (Exception ex) {
			throw  new TransformLangExecutorRuntimeException(node,ex.getMessage(),ex);
		}

		return data;
	}

    
    /*
	 * Declaration & calling of Functions here
	 */
    @Override
	public Object visit(CLVFFunctionCallStatement node, Object data) {
        // EXTERNAL FUNCTION
        if (node.externalFunction != null) {
            // put call parameters on stack
            node.childrenAccept(this, data);
            // convert stack content into values
            try {
            	node.context.setGraph(this.graph);
                TLValue returnVal = node.externalFunction.execute(stack.pop(
                        node.externalFunctionParams, node.jjtGetNumChildren()),
                        node.context);
                stack.push(returnVal);
            } catch (TransformLangExecutorRuntimeException ex) {
                ex.setNode(node);
                throw ex;
            } catch (Exception ex){
            	String msg="Java exception ["+ex.getClass().getName()+"] occured during call of external function: "+node.externalFunction.getLibrary()+"."+node.externalFunction.getName();
            	logger.debug(msg,ex);
            	throw new TransformLangExecutorRuntimeException(node,msg,ex);
            }

        } else {
            // INTERNAL FUNCTION
            // put call parameters on stack
            node.childrenAccept(this, data);
            CLVFFunctionDeclaration executionNode = node.callNode;
            // open call frame
            stack.pushFuncCallFrame();

            // store call parameters from stack as local variables
            for (int i = executionNode.numParams - 1; i >= 0; stack
                    .storeLocalVar(i--, new TLVariable("local", stack.pop())))
                ;

            // execute function body
            // loop execution
            TLValue returnData;
            final int numChildren = executionNode.jjtGetNumChildren();
            for (int i = 0; i < numChildren; i++) {
                executionNode.jjtGetChild(i).jjtAccept(this, data);
                returnData = stack.pop(); // in case there is anything on top
                // of stack
                // check for break or continue statements
                if (breakFlag) {
                    breakFlag = false;
                    if (breakType == BREAK_RETURN) {
                        if (returnData != null)
                            stack.push(returnData);
                        break;
                    }
                }
            }
            stack.popFuncCallFrame();
        }
        return data;
    }

    @Override
	public Object visit(CLVFFunctionDeclaration node, Object data) {
        return data;
    }

    public Object visit(CLVFStatementExpression node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        return data;
    }

    public Object executeFunction(CLVFFunctionDeclaration executionNode, TLValue[] data) {
        //put call parameters on stack
        if (data==null){
            data=new TLValue[0];
        }
        //TODO - check for function call parameter types
        
        // open call frame
        stack.pushFuncCallFrame();
        // store call parameters from stack as local variables
        for (int i=executionNode.numParams-1;i>=0; i--) {
        	stack.storeLocalVar(i, new TLVariable(executionNode.varNames[i], data[i]));
        }
       
        // execute function body
        // loop execution
        TLValue returnData;
        final int numChildren=executionNode.jjtGetNumChildren();
        for (int i=0;i<numChildren;i++){
            executionNode.jjtGetChild(i).jjtAccept(this,data);
            returnData=stack.pop(); // in case there is anything on top of stack
            // check for break or continue statements
            if (breakFlag){ 
                breakFlag=false;
                if (breakType==BREAK_RETURN){
                    if (returnData!=null)
                        stack.push(returnData);
                    break;
                }
            }
        }
        stack.popFuncCallFrame();
        return data;
    }
    
    
    @Override
	public Object visit(CLVFRaiseErrorNode node,Object data){
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        throw new TransformLangExecutorRuntimeException(node,null,
                    "!!! Exception raised by user: "+((a!=null) ? a.toString() : "no message"));
        
    }
    
    
    @Override
	public Object visit(CLVFEvalNode node, Object data) {
        // get TL expression
    	node.jjtGetChild(0).jjtAccept(this, data);
    	String src=stack.pop().toString();
    	Node parseTree;
    	// construct parser
    	try{
    	((TransformLangParser)parser).ReInit(new CharSequenceReader(src));
    		if (node.expMode)
    			parseTree = ((TransformLangParser)parser).StartExpression();
    		else
    			parseTree = ((TransformLangParser)parser).Start();
    	}catch(ParseException ex){
    		 throw new TransformLangExecutorRuntimeException(node,
                     "Can't parse \"eval\" expression:"+ex.getMessage(), ex);
    	}catch(NullPointerException ex){
    		throw new RuntimeException("Error in \"eval\" execution/parsing (parser is missing)." ,ex);
    	}
    	
    	/* 
    	 * option to permanently store parsed expression in this tree
    	if (true){
    		//add this subtree to eclosing AST
    	}
    	*/
    	
    	// execute eval
    	if (node.expMode)
    		visit((CLVFStartExpression)parseTree,data);
    	else
    		visit((CLVFStart)parseTree,data);
    	
        return data;
    }

    
    
    @Override
	public Object visit(CLVFSequenceNode node,Object data){
        if (node.sequence==null){
            if (graph!=null){
                node.sequence=graph.getSequence(node.sequenceName);
            }else{
                throw new TransformLangExecutorRuntimeException(node,
                        "Can't obtain Sequence \""+node.sequenceName+
                        "\" from graph - graph is not assigned");
            }
            if (node.sequence==null){
                throw new TransformLangExecutorRuntimeException(node,
                        "Can't obtain Sequence \""+node.sequenceName+
                        "\" from graph \""+graph.getName()+"\"");
            }
            
            // initialize the sequence if necessary
            if (!node.sequence.isInitialized()) {
            	try {
					node.sequence.init();
				} catch (ComponentNotReadyException e) {
					throw new TransformLangExecutorRuntimeException(
							node,"Unable to initialize sequence " 
							+ "\"" + node.sequenceName + "\"",e);
				}
            }
        }
        if (node.value==null){
        	switch(node.retType){
        	case LONG_VAR:
        		node.value=TLValue.create(TLValueType.LONG);
        		break;
        	case STRING_VAR:
        		node.value=TLValue.create(TLValueType.STRING);
        		break;
        	default:
        		node.value=TLValue.create(TLValueType.INTEGER);
        	}
        	
        }
        TLValue retVal=node.value;
        
        switch(node.opType){
        case CLVFSequenceNode.OP_RESET:
//        	try{
        		node.sequence.resetValue();
//        	}catch(ComponentNotReadyException ex){
//        		throw new TransformLangExecutorRuntimeException(node,"Error when resetting sequence \""+node.sequenceName+"\"",ex);
//        	}
            retVal=TLNumericValue.ZERO;
            break;
        case CLVFSequenceNode.OP_CURRENT:
            switch(node.retType){
            case LONG_VAR:
            	((TLNumericValue)retVal).setValue(node.sequence.currentValueLong());
                break;
            case STRING_VAR:
                retVal.setValue(node.sequence.currentValueString());
                break;
            default:
                ((TLNumericValue)retVal).setValue(node.sequence.currentValueInt());
            }
            break;
            default: // default is next value from sequence
                switch(node.retType){
                case LONG_VAR:
                	((TLNumericValue)retVal).setValue(node.sequence.nextValueLong());
                    break;
                case STRING_VAR:
                    retVal.setValue(node.sequence.nextValueString());
                    break;
                default:
                	((TLNumericValue)retVal).setValue(node.sequence.nextValueInt());
                }
        }
        stack.push(retVal);
        return data;
    }
    
    @Override
	public Object visit(CLVFLookupNode node, Object data) {
		DataRecord record = null;
		if (node.lookupTable == null) {
			node.lookupTable = graph.getLookupTable(node.lookupName);
			if (node.lookupTable == null) {
				throw new TransformLangExecutorRuntimeException(node,
						"Can't obtain LookupTable \"" + node.lookupName
								+ "\" from graph \"" + graph.getName() + "\"");
			} 
			else {
				// we have to initialize the lookup table ourselves, graph is not doing it for us
				try {
					if (! node.lookupTable.isInitialized()) {
						node.lookupTable.init();
					}
				} catch (ComponentNotReadyException e) {
					throw new TransformLangExecutorRuntimeException(node,
							"Error when initializing lookup table \""
									+ node.lookupName + "\" :", e);
				}
			}
			if (node.lookup == null && lookups.containsKey(node.lookupTable.getId())) {
				node.lookup = lookups.get(node.lookupTable.getId());
			}
			if (node.opType == CLVFLookupNode.OP_GET || node.opType==CLVFLookupNode.OP_NEXT) {
				DataRecordMetadata metadata = node.lookupTable.getMetadata();
				if (metadata != null) {
					node.fieldNum = metadata.getFieldPosition(
							node.fieldName);
					if (node.fieldNum < 0) {
						throw new TransformLangExecutorRuntimeException(node,
								"Invalid field name \"" + node.fieldName
										+ "\" at LookupTable \"" + node.lookupName
										+ "\" in graph \"" + graph.getName() + "\"");
					}
				}
			}
		}
		switch (node.opType) {
		case CLVFLookupNode.OP_INIT:
			// The code is removed from CTL1 after discuession with Kokon, in CTL2 these functions do not exist anymore as they are not needed 
			/*try {
				node.lookupTable.init();
				node.lookupTable.preExecute();
			} catch (ComponentNotReadyException ex) {
				throw new TransformLangExecutorRuntimeException(node,
						"Error when initializing lookup table \""
								+ node.lookupName + "\" :", ex);
			}*/
			return data;
		case CLVFLookupNode.OP_FREE:
			// The code is removed from CTL1 after discuession with Kokon, in CTL2 these functions do not exist anymore as they are not needed
			/*node.lookupTable.free();
			node.lookup = null;
			lookups.remove(node.lookupTable.getId());*/
			return data;
		case CLVFLookupNode.OP_NUM_FOUND:
			stack.push(new TLNumericValue(TLValueType.INTEGER, new CloverInteger(
					node.lookup.getNumFound())));
			return data;
		case CLVFLookupNode.OP_GET:
            node.childrenAccept(this, data);
            if (node.lookup == null) {
    			try {
    				node.createLookup(stack);
    			} catch (ComponentNotReadyException ex) {
    				throw new TransformLangExecutorRuntimeException(node,
    						"Error when initializing lookup table \""
    								+ node.lookupName + "\" :", ex);
    			}
    			lookups.put(node.lookupTable.getId(), node.lookup);
    		}  
			node.seek(stack);
			if (node.fieldNum == -1) {
				node.fieldNum = node.lookupTable.getMetadata().getFieldPosition(node.fieldName);
			}
			if (node.lookup.hasNext()) {
				record = node.lookup.next();
			}else{
				record = null;
			}
				break;
		default: // CLVFLookupNode.OP_NEXT:
			if (node.lookup.hasNext()) {
				record = node.lookup.next();
			}else{
				record = null;
			}
		}

		if (record != null) {
			stack.push(TLValue.convertValue(record.getField(node.fieldNum)));
		} else {
			stack.push(TLNullValue.getInstance());
		}

		return data;
	}
    
    @Override
	public Object visit(CLVFDictionaryNode node, Object data) {
    	final Dictionary d = graph.getDictionary();
    	
    	if (d == null) {
    		throw new TransformLangExecutorRuntimeException("No dictionary defined on the graph");
    	}
    	
    	TLValue key = null;
    	TLValue value = null;
    	switch (node.operation) {
    	case CLVFDictionaryNode.OP_READ:
    		// evaluate the key
    		node.jjtGetChild(0).jjtAccept(this, data);
    		key = stack.pop();
    		if (key.getType() != TLValueType.STRING) {
    			throw new TransformLangExecutorRuntimeException("Dictionary supports only non-null string keys");
    		}
    		
    		final Object dictValue = d.getValue(key.getValue().toString());
    		stack.push(dictValue == null ? TLNullValue.getInstance() : new TLStringValue(dictValue.toString()));
    		break;
    	case CLVFDictionaryNode.OP_WRITE:
    		node.jjtGetChild(0).jjtAccept(this, data);
    		key = stack.pop();
    		if (key.getType() != TLValueType.STRING) {
    			throw new TransformLangExecutorRuntimeException("Dictionary supports only string keys");
    		}
    		final String keyToWrite = key.getValue().toString();
    		
    		node.jjtGetChild(1).jjtAccept(this, data);
    		value = stack.pop();
    		
    		String valueToWrite = null;
    		if (value == TLNullValue.getInstance()) {
    			// writing null value
    			valueToWrite = null;
    		} else if (value.getType() == TLValueType.STRING) {
    			// convert string value to string
    			valueToWrite = value.getValue().toString();
    		} else {
    			// anything non-null, non-string is error
    			throw new TransformLangExecutorRuntimeException("Dictionary supports only string values");
    		}
    		
    		try {
    			d.setValue(keyToWrite, StringDictionaryType.TYPE_ID, valueToWrite);
    		} catch (ComponentNotReadyException e) {
    			throw new TransformLangExecutorRuntimeException("Cannot set dictionary key '" + keyToWrite + "' to value '" + valueToWrite + "'",e);
    		}
    		break;
    	case CLVFDictionaryNode.OP_DELETE:
    		// evaluate the key
    		node.jjtGetChild(0).jjtAccept(this, data);
    		key = stack.pop();
    		if (key.getType() != TLValueType.STRING) {
    			throw new TransformLangExecutorRuntimeException("Dictionary supports only non-null string keys");
    		}
    		
    		final String keyToDelete = key.getValue().toString();
    		try {
				d.setValue(keyToDelete, null);
			} catch (ComponentNotReadyException e) {
				throw new TransformLangExecutorRuntimeException("Cannot delete key '" + keyToDelete + "'", e);
			}
    		break;
    	default:
    		throw new TransformLangExecutorRuntimeException("Unknown dictionary operation: " + node.operation);
    	}
    	
    	return data;
    }
    
    @Override
	public Object visit(CLVFPrintLogNode node, Object data) {
        if (runtimeLogger == null) {
            throw new TransformLangExecutorRuntimeException(node,
                    "Can NOT perform logging operation - no logger defined");
        }
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue msg =  stack.pop();
        switch (node.level) {
        case 1: //| "debug" 
            runtimeLogger.debug(msg);
            break;
        case 2: //| "info"
            runtimeLogger.info(msg);
            break;
        case 3: //| "warn"
            runtimeLogger.warn(msg);
            break;
        case 4: //| "error"
            runtimeLogger.error(msg);
            break;
        case 5: //| "fatal"
            runtimeLogger.fatal(msg);
            break;
        default:
            runtimeLogger.trace(msg);
        }
        return data;
    }


    @Override
	public Object visit(CLVFImportSource node,Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
	public Object visit(CLVFSymbolNameExp node,Object data) {
        stack.push(node.typeValue);
        return data;
    }
    
    @Override
	public Object visit(CLVFOperator node,Object data) {
        return data;
    }
 
    @Override
	public Object visit(CLVFPostfixExpression node,Object data) {
    	// get variable && put value on stack by executing child node
    	Node child=node.jjtGetChild(0);
    	if (! (child instanceof CLVFVariableLiteral)){
    		throw new TransformLangExecutorRuntimeException(node,"postfix expression is allowed only on variable");
    	}
    	TLVariable var=(TLVariable)child.jjtAccept(this, data);
    	int operatorType=((CLVFOperator)node.jjtGetChild(1)).kind;
    	
    	// value instance on stack is variable's internal value
    	// duplicate it before incrementing 
    	TLValue origValue = stack.pop();
    	stack.push(origValue.duplicate());
    	
    	if (operatorType==INCR) {
            if (var.getType().isNumeric()) {
                ((TLNumericValue)var.getTLValue()).add(Stack.NUM_ONE_P);
            }else if (var.getType()==TLValueType.DATE) {
                stack.calendar.setTime(((TLDateValue)var.getTLValue()).getDate());
                stack.calendar.add(Calendar.DATE, 1);
                ((TLDateValue)var.getTLValue()).setValue(stack.calendar.getTime());
            }else {
                throw new TransformLangExecutorRuntimeException(node,"variable ["+var+"] is not of numeric or date type");
            }
    	}else{
    		if (var.getType().isNumeric()) {
                ((TLNumericValue)var.getTLValue()).sub(Stack.NUM_ONE_P);
            }else if (var.getType()==TLValueType.DATE) {
                stack.calendar.setTime(((TLDateValue)var.getTLValue()).getDate());
                stack.calendar.add(Calendar.DATE, -1);
                var.getTLValue().setValue(stack.calendar.getTime());
            }else {
                throw new TransformLangExecutorRuntimeException(node,"variable ["+var+"] is not of numeric or date type");
            }
    	}
    	
        return data;
    }
    
    @Override
	public Object visit(CLVFUnaryExpression node,Object data) {
    	int operatorType=((CLVFOperator)node.jjtGetChild(0)).kind;
    	Node child=node.jjtGetChild(1);
    	TLValue val;
    	
    	switch (operatorType) {
		case INCR:
		case DECR:
			// get variable && put value on stack by executing child node
			if (!(child instanceof CLVFVariableLiteral)) {
				throw new TransformLangExecutorRuntimeException(node,
						"postfix expression is allowed only on variable");
			}
			TLVariable var = (TLVariable) child.jjtAccept(this, data);
			if (var.getType().isNumeric()) {
				((TLNumericValue)var.getTLValue()).add(
						operatorType == INCR ? Stack.NUM_ONE_P
								: Stack.NUM_MINUS_ONE_P);
			} else if (var.getType() == TLValueType.DATE) {
				stack.calendar.setTime(((TLDateValue)var.getTLValue()).getDate());
				stack.calendar
						.add(Calendar.DATE, operatorType == INCR ? 1 : -1);
				var.getTLValue().setValue(stack.calendar.getTime());
			} else {
				throw new TransformLangExecutorRuntimeException(node,
						"variable [" + var + "] is not of numeric or date type");
			}
			child.jjtAccept(this, data);
			break;
		case NOT:
			child.jjtAccept(this, data);
			val = stack.pop();
			if (val.type == TLValueType.BOOLEAN) {
				stack.push(val==TLBooleanValue.TRUE ? TLBooleanValue.FALSE : TLBooleanValue.TRUE);
			} else {
				throw new TransformLangExecutorRuntimeException(node,
						new Object[] { val },
						"logical condition does not evaluate to BOOLEAN value");
			}
			break;

		case MINUS:
			child.jjtAccept(this, data);
			val = stack.pop();
			if (val.type.isNumeric()) {
				val = val.duplicate();
				((TLNumericValue)val).neg();
				stack.push(val);
			} else {
				throw new TransformLangExecutorRuntimeException(node,
						new Object[] { val }, "variable is not of numeric type");
			}
			break;
		case PLUS:
			child.jjtAccept(this, data);
			
			val = stack.pop();
			if (val.type.isNumeric()) {
				val = val.duplicate();
				((TLNumericValue)val).abs();
				stack.push(val);
			} else {
				throw new TransformLangExecutorRuntimeException(node,
						new Object[] { val }, "variable is not of numeric type");
			}
			break;
		default:
			throw new TransformLangExecutorRuntimeException(node,
					"unsupported operation");
		}

		return data;
	}

	@Override
	public Object visit(CLVFListOfLiterals node, Object data) {
		stack.push(node.value);
		return data;
	}
	
}
