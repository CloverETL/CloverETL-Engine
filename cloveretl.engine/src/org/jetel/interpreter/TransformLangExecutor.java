/*
 *    Copyright (C) 2002-2004  David Pavlis <david_pavlis@hotmail.com>
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
 */

package org.jetel.interpreter;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.ASTnode.CLVFAddNode;
import org.jetel.interpreter.ASTnode.CLVFAnd;
import org.jetel.interpreter.ASTnode.CLVFAssignment;
import org.jetel.interpreter.ASTnode.CLVFBlock;
import org.jetel.interpreter.ASTnode.CLVFBreakStatement;
import org.jetel.interpreter.ASTnode.CLVFBreakpointNode;
import org.jetel.interpreter.ASTnode.CLVFCaseExpression;
import org.jetel.interpreter.ASTnode.CLVFComparison;
import org.jetel.interpreter.ASTnode.CLVFContinueStatement;
import org.jetel.interpreter.ASTnode.CLVFDivNode;
import org.jetel.interpreter.ASTnode.CLVFDoStatement;
import org.jetel.interpreter.ASTnode.CLVFForStatement;
import org.jetel.interpreter.ASTnode.CLVFForeachStatement;
import org.jetel.interpreter.ASTnode.CLVFFunctionCallStatement;
import org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration;
import org.jetel.interpreter.ASTnode.CLVFIfStatement;
import org.jetel.interpreter.ASTnode.CLVFTryCatchStatement;
import org.jetel.interpreter.ASTnode.CLVFIffNode;
import org.jetel.interpreter.ASTnode.CLVFImportSource;
import org.jetel.interpreter.ASTnode.CLVFIncrDecrStatement;
import org.jetel.interpreter.ASTnode.CLVFInputFieldLiteral;
import org.jetel.interpreter.ASTnode.CLVFIsNullNode;
import org.jetel.interpreter.ASTnode.CLVFLiteral;
import org.jetel.interpreter.ASTnode.CLVFLookupNode;
import org.jetel.interpreter.ASTnode.CLVFMapping;
import org.jetel.interpreter.ASTnode.CLVFMinusNode;
import org.jetel.interpreter.ASTnode.CLVFModNode;
import org.jetel.interpreter.ASTnode.CLVFMulNode;
import org.jetel.interpreter.ASTnode.CLVFNVLNode;
import org.jetel.interpreter.ASTnode.CLVFNVL2Node;
import org.jetel.interpreter.ASTnode.CLVFEvalNode;
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
import org.jetel.interpreter.ASTnode.CLVFUnaryExpression;
import org.jetel.interpreter.ASTnode.CLVFVarDeclaration;
import org.jetel.interpreter.ASTnode.CLVFVariableLiteral;
import org.jetel.interpreter.ASTnode.CLVFWhileStatement;
import org.jetel.interpreter.ASTnode.Node;
import org.jetel.interpreter.ASTnode.SimpleNode;
import org.jetel.interpreter.data.TLListVariable;
import org.jetel.interpreter.data.TLMapVariable;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.interpreter.data.TLVariable;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.Compare;
import org.jetel.util.StringUtils;

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
    public void setInputRecords(DataRecord[] inputRecords){
        this.inputRecords=inputRecords;
    }
    
    /**
     * Set output data records for processing.<br>
     * Referenced output data fields will be resolved from
     * these data records - assignment (in code) to output data field
     * will result in assignment to one of these data records.
     * 
     * @param outputRecords array of output data records for setting values
     */
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
        stack.push(new TLValue(TLValueType.STRING,obj));
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

    public Object visit(SimpleNode node, Object data) {
//        throw new TransformLangExecutorRuntimeException(node,
//                "Error: Call to visit for SimpleNode");
        return data;
    }

    public Object visit(CLVFStart node, Object data) {

        int i, k = node.jjtGetNumChildren();

        for (i = 0; i < k; i++)
            node.jjtGetChild(i).jjtAccept(this, data);

        return data; // this value is ignored in this example
    }
    
    public Object visit(CLVFStartExpression node, Object data) {

        int i, k = node.jjtGetNumChildren();

        for (i = 0; i < k; i++)
            node.jjtGetChild(i).jjtAccept(this, data);

        return data; // this value is ignored in this example
    }


    public Object visit(CLVFOr node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a=stack.pop();
        
        if (a.type!=TLValueType.BOOLEAN){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }else if (a.getBoolean()){
        		stack.push(Stack.TRUE_VAL);
        		return data;
        }
        
        node.jjtGetChild(1).jjtAccept(this, data);
        a=stack.pop();

        if (a.type!=TLValueType.BOOLEAN){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }
        
        stack.push( a.getBoolean() ? Stack.TRUE_VAL : Stack.FALSE_VAL);
        
        return data;
    }

    public Object visit(CLVFAnd node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a=stack.pop();

        if (a.type!=TLValueType.BOOLEAN){
            Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }else if (!a.getBoolean()){
            stack.push(Stack.FALSE_VAL);
            return data;
        }

        node.jjtGetChild(1).jjtAccept(this, data);
        a=stack.pop();

        if (a.type!=TLValueType.BOOLEAN){
            Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }

        stack.push(a.getBoolean() ? Stack.TRUE_VAL : Stack.FALSE_VAL);
        return data;
    }

    public Object visit(CLVFComparison node, Object data) {
        int cmpResult = 2;
        boolean lValue = false;
        // special handling for Regular expression
        if (node.cmpType == REGEX_EQUAL) {
            node.jjtGetChild(0).jjtAccept(this, data);
            TLValue field1 = stack.pop();
            node.jjtGetChild(1).jjtAccept(this, data);
            TLValue field2 = stack.pop();
            if (field1.type == TLValueType.STRING
                    && field2.getValue() instanceof Matcher) {
                Matcher regex = (Matcher) field2.getValue();
                regex.reset(field1.getCharSequence());
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

            // other types of comparison
        } else {
            node.jjtGetChild(0).jjtAccept(this, data);
            TLValue a = stack.pop();
            node.jjtGetChild(1).jjtAccept(this, data);
            TLValue b = stack.pop();

            if (!a.type.isCompatible(b.type)) {
                Object arguments[] = { a, b };
                throw new TransformLangExecutorRuntimeException(node,
                        arguments,
                        "compare - incompatible literals/expressions");
            }
            switch (a.type) {
            case INTEGER:
            case LONG:
            case DOUBLE:
            case DECIMAL:
                cmpResult = a.getNumeric().compareTo(b.getNumeric());
                break;
            case DATE:
                cmpResult = a.getDate().compareTo(b.getDate());
                break;
            case STRING:
                cmpResult = Compare.compare(a.getCharSequence(), b
                        .getCharSequence());
                break;
            case BOOLEAN:
                if (node.cmpType == EQUAL || node.cmpType == NON_EQUAL) {
                    cmpResult = a.getBoolean() == b.getBoolean() ? 0 : -1;
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
        stack.push(lValue ? Stack.TRUE_VAL : Stack.FALSE_VAL);
        return data;
    }

    public Object visit(CLVFAddNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        

        if (a.isNull()|| b.isNull()) {
            //TODO: (check) we allow empty strings to be concatenated
            if (a.type!=TLValueType.STRING)
                throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "add - NULL value not allowed");
        }
        
        if (node.nodeVal==null) {
                node.nodeVal= a.isNull() ? TLValue.create(a.type) : a.duplicate();
        }

        try {
            if (a.type.isNumeric() && b.type.isNumeric()) {
                node.nodeVal.getNumeric().setValue(a.getNumeric());
                node.nodeVal.getNumeric().add(b.getNumeric());
                stack.push(node.nodeVal);
            } else if (a.type==TLValueType.DATE && b.type.isNumeric()) {
                Calendar result = Calendar.getInstance();
                result.setTime(a.getDate());
                result.add(Calendar.DATE, b.getInt());
                node.nodeVal.getDate().setTime(result.getTimeInMillis());
                stack.push(node.nodeVal);
            } else if (a.type==TLValueType.STRING) {
                CharSequence a1 = a.getCharSequence();
                StringBuilder buf=(StringBuilder)node.nodeVal.getValue();
                buf.setLength(0);
                StringUtils.strBuffAppend(buf,a1);
                if (b.type==TLValueType.STRING) {
                    StringUtils.strBuffAppend(buf,b.getCharSequence());
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
                    "add - wrong type of literal(s)");
        }

        return data;
    }

    public Object visit(CLVFSubNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
       

        if (a.isNull()|| b.isNull()) {
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
            node.nodeVal.getNumeric().setValue(a.getNumeric());
            node.nodeVal.getNumeric().sub(b.getNumeric());
            stack.push(node.nodeVal);
        } else if (a.type==TLValueType.DATE) {
            Calendar result = Calendar.getInstance();
            result.setTime(a.getDate());
            result.add(Calendar.DATE, b.getInt() * -1);
            node.nodeVal.getDate().setTime(result.getTimeInMillis());
            stack.push(node.nodeVal);
        } else {
            Object[] arguments = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "sub - wrong type of literal(s)");
        }

        return data;
    }

    public Object visit(CLVFMulNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        
        if (a.isNull()|| b.isNull()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mul - NULL value not allowed");
        }

        if (!a.type.isNumeric() && !b.type.isNumeric()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a,b },
                    "mul - wrong type of literals");
        }

        if (node.nodeVal==null) {
            node.nodeVal=a.duplicate();
        }
        
        node.nodeVal.getNumeric().setValue(a.getNumeric());
        node.nodeVal.getNumeric().mul(b.getNumeric());
        stack.push(node.nodeVal);
        return data;
        
    }

    public Object visit(CLVFDivNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        

        if (a.isNull()|| b.isNull()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "div - NULL value not allowed");
        }

        if (!a.type.isNumeric() && !b.type.isNumeric()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a,b },
                    "div - wrong type of literals");
        }

        if (node.nodeVal==null || node.nodeVal.type!=a.type) {
            node.nodeVal=TLValue.create(a.type);
        }
        
        node.nodeVal.getNumeric().setValue(a.getNumeric());
        try {
            node.nodeVal.getNumeric().div(b.getNumeric());
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

    public Object visit(CLVFModNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue b = stack.pop();
        

        if (a.isNull()|| b.isNull()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mod - NULL value not allowed");
        }

        if (!a.type.isNumeric() && !b.type.isNumeric()) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mod - wrong type of literals");
        }
        
        if (node.nodeVal==null) {
            node.nodeVal=a.duplicate();
        }

        
        node.nodeVal.getNumeric().setValue(a.getNumeric());
        node.nodeVal.getNumeric().mod(b.getNumeric());
        stack.push(node.nodeVal);
        return data;
    }

     public Object visit(CLVFIsNullNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value.isNull()) {
            stack.push(Stack.TRUE_VAL);
        } else {
            if (value.type==TLValueType.STRING) {
                stack.push( value.getCharSequence().length()==0 ? Stack.TRUE_VAL : Stack.FALSE_VAL);
            }else {
                stack.push(Stack.FALSE_VAL);
            }
        }

        return data;
    }

    public Object visit(CLVFNVLNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value.isNull()) {
            node.jjtGetChild(1).jjtAccept(this, data);
        } else {
            if (value.type==TLValueType.STRING && value.getCharSequence().length()==0) {
                node.jjtGetChild(1).jjtAccept(this, data);
            }else {
                stack.push(value);
            }
        }

        return data;
    }
    
    public Object visit(CLVFNVL2Node node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value.isNull() || (value.type==TLValueType.STRING && value.getCharSequence().length()==0)) {
            node.jjtGetChild(2).jjtAccept(this, data);
        } else {
            node.jjtGetChild(1).jjtAccept(this, data);
        }

        return data;
    }

    public Object visit(CLVFLiteral node, Object data) {
        stack.push(node.valueTL);
        return data;
    }

    public Object visit(CLVFInputFieldLiteral node, Object data) {
    	DataRecord record = inputRecords[node.recordNo];
		if (record == null) {
			stack.push(Stack.NULL_VAL);
			return null;
		}
		
		node.field = record.getField(node.fieldNo);
        if (node.field.isNull()){
                stack.push(Stack.NULL_VAL);
        		return null;
        }
        
        if (node.value==null){
        	node.value=new TLValue(node.field);
		}else{
			node.value.setValue(node.field);
		}
        
        stack.push(node.value);

        // we return reference to DataField so we can
        // perform extra checking in special cases
        return node.field;
    }

    public Object visit(CLVFOutputFieldLiteral node, Object data) {
        //stack.push(inputRecords[node.recordNo].getField(node.fieldNo));
        // we return reference to DataField so we can
        // perform extra checking in special cases
        return data;
    }
    
 
    
    public Object visit(CLVFRegexLiteral node, Object data) {
        stack.push(new TLValue(TLValueType.OBJECT,node.matcher));
        return data;
    }
   
 
    public Object visit(CLVFMinusNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();

        if (value.type.isNumeric()) {
            TLValue newVal=value.duplicate();
            newVal.getNumeric().mul(Stack.NUM_MINUS_ONE_P);
            stack.push(newVal);
        } else {
            Object arguments[] = { value };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "minus - not a number");
        }

        return data;
    }

    
    public Object visit(CLVFIffNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue condition = stack.pop();

        if (condition.type==TLValueType.BOOLEAN) {
            if (condition.getBoolean()) {
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

    public Object visit(CLVFPrintErrNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        if (node.printLine){
            StringBuilder buf=new StringBuilder((a != null ? a.toString() : "<null>"));
            buf.append(" (on line: ").append(node.getLineNumber());
            buf.append(" col: ").append(node.getColumnNumber()).append(")");
            System.err.println(buf);
        }else{
            System.err.println(a != null ? a : "<null>");
        }

        return data;
    }

    public Object visit(CLVFPrintStackNode node, Object data) {
        for (int i=stack.top;i>=0;i--){
            System.err.println("["+i+"] : "+stack.stack[i]);
        }
        

        return data;
    }

    
    /***************************************************************************
     * Transformation Language executor starts here.
     **************************************************************************/

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

        try {
            loopCondition.jjtAccept(this, data); // evaluate the condition
            condition = stack.pop().getBoolean();
        } catch (ClassCastException ex) {
            throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
        }catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition");
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
            try {
                condition = stack.pop().getBoolean();
            } catch (ClassCastException ex) {
                throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
            }
        }

        return data;
    }

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
        Iterator<TLValue> iter;
        switch(arrayVariable.getType()) {
        case LIST:
            iter=((TLListVariable)arrayVariable).getList().iterator();
            break;
        case MAP:
            iter=((TLMapVariable)arrayVariable).getMap().values().iterator();
            break;
            default:
                throw new TransformLangExecutorRuntimeException(node,"not a Map or List variable");
        }
        while(iter.hasNext()) {
            variableToAssign.setValue(iter.next());
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
        return data;
    }

    
    public Object visit(CLVFWhileStatement node, Object data) {
        boolean condition = false;
        Node loopCondition = node.jjtGetChild(0);
        Node body;
        try{
            body=node.jjtGetChild(1);
        }catch(ArrayIndexOutOfBoundsException ex){
            body=emptyNode;
        }

        try {
            loopCondition.jjtAccept(this, data); // evaluate the condition
            condition = stack.pop().getBoolean();
        } catch (ClassCastException ex) {
            throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
        }catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition");
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
            try {
                condition = stack.pop().getBoolean();
            } catch (ClassCastException ex) {
                throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
            }
        }

        return data;
    }

    public Object visit(CLVFIfStatement node, Object data) {
        boolean condition = false;
        try {
            node.jjtGetChild(0).jjtAccept(this, data); // evaluate the
            // condition
            condition = stack.pop().getBoolean();
        } catch (ClassCastException ex) {
            throw new TransformLangExecutorRuntimeException(node,"condition does not evaluate to BOOLEAN value");
        } catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition");
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
            try {
                condition = stack.pop().getBoolean();
            }catch (ClassCastException ex) {
                throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
            }catch (NullPointerException ex){
                throw new TransformLangExecutorRuntimeException(node,"missing or invalid condition");
            }
        } while (condition);

        return data;
    }

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
            if (node.jjtGetChild(i).jjtAccept(this, data)==Stack.TRUE_VAL){
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

    public Object visit(CLVFCaseExpression node, Object data) {
        // test if literal (as child 0) is equal to data on stack
        // if so, execute block (child 1)
        boolean match = false;
        TLValue switchVal = stack.pop();
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue value = stack.pop();
        try {
            switch(switchVal.type) {
            case INTEGER:
            case LONG:
            case DOUBLE:
            case DECIMAL:
                match = (value.getNumeric().compareTo(switchVal.getNumeric()) == 0);
                break;
            case STRING:
                match = (Compare.compare(switchVal.getCharSequence(),
                        value.getCharSequence()) == 0);
                break;
            case DATE:
                match = ( switchVal.getDate().compareTo(value.getDate()) == 0);
                break;
            case BOOLEAN:
                match = (switchVal.getBoolean()==value.getBoolean());
                break;
            default:
            }
        } catch (ClassCastException ex) {
            Object[] args=new Object[] {switchVal,value};
            throw new TransformLangExecutorRuntimeException(node,args,"incompatible literals in case clause");
        }catch (NullPointerException ex){
            throw new TransformLangExecutorRuntimeException(node,"missing or invalid case value");
        }
        if (match){
            node.jjtGetChild(1).jjtAccept(this, data);
            return Stack.TRUE_VAL;
        }
        return Stack.FALSE_VAL;
    }

    public Object visit(CLVFTryCatchStatement node, Object data) {
    	try {
            node.jjtGetChild(0).jjtAccept(this, data); // evaluate the
        } catch (Exception ex) {
        	//	 populate chosen variable with exception name
        	CLVFVariableLiteral varLit = (CLVFVariableLiteral)node.jjtGetChild(1);
        	TLVariable var=stack.getVar(varLit.localVar, varLit.varSlot);
        	var.getValue().setValue(ex.getClass().getName());
        	
        	// call the catch block
        	node.jjtGetChild(2).jjtAccept(this, data);
        }

        return data;
    }

    

    public Object visit(CLVFIncrDecrStatement node, Object data) {
        Node childNode = node.jjtGetChild(0);
            CLVFVariableLiteral varNode=(CLVFVariableLiteral) childNode;
            TLVariable var=stack.getVar(varNode.localVar, varNode.varSlot);
            
            if (var.getType().isNumeric()) {
                var.getValue().getNumeric().add( node.kind==INCR ? Stack.NUM_ONE_P : Stack.NUM_MINUS_ONE_P);
            }else if (var.getType()==TLValueType.DATE) {
                stack.calendar.setTime(var.getValue().getDate());
                stack.calendar.add(Calendar.DATE, node.kind == INCR ? 1 : -1);
                var.getValue().setValue(stack.calendar.getTime());
            }else {
                throw new TransformLangExecutorRuntimeException(node,"variable is not of numeric or date type");
            }
        
        return data;
    }

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

    public Object visit(CLVFBreakStatement node, Object data) {
        breakFlag = true; // we encountered break statement;
        breakType=BREAK_BREAK;
        return data;
    }

    public Object visit(CLVFContinueStatement node, Object data) {
        breakFlag = true; // we encountered continue statement;
        breakType= BREAK_CONTINUE;
        return data;
    }

    public Object visit(CLVFReturnStatement node, Object data) {
        if (node.jjtHasChildren()){
            node.jjtGetChild(0).jjtAccept(this, data);
        }
        breakFlag = true;
        breakType = BREAK_RETURN;
        return data;
    }

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
    public Object visit(CLVFVarDeclaration node, Object data) {
        TLValue value=null;
        TLVariable variable=null;
        // create global/local variable
        switch (node.type) {
        case INT_VAR:
            value = new TLValue(TLValueType.INTEGER, new CloverInteger(0));
            break;
        case LONG_VAR:
            value = new TLValue(TLValueType.LONG,new CloverLong(0));
            break;
        case DOUBLE_VAR:
            value = new TLValue(TLValueType.DOUBLE, new CloverDouble(0));
            break;
        case DECIMAL_VAR:
            if (node.length > 0) {
                if (node.precision > 0) {
                    value = new TLValue(TLValueType.DECIMAL, DecimalFactory.getDecimal(node.length,
                            node.precision));
                } else {
                    value = new TLValue(TLValueType.DECIMAL,DecimalFactory.getDecimal(node.length, 0));
                }
            } else {
                value = new TLValue(TLValueType.DECIMAL,DecimalFactory.getDecimal());
            }
            break;
        case STRING_VAR:
            value = new TLValue(TLValueType.STRING,new StringBuilder());
            break;
        case DATE_VAR:
            value = new TLValue(TLValueType.DATE,new Date());
            break;
        case BOOLEAN_VAR:
            value = Stack.FALSE_VAL;
            break;
        case BYTE_VAR:
        	value = TLValue.create(TLValueType.BYTE);
        	break;
        case LIST_VAR:
            if (node.length>0) {
                variable = new TLListVariable(node.name,node.length);
                ((TLListVariable)variable).fill(Stack.NULL_VAL, node.length);
            }else {
                variable = new TLListVariable(node.name);
            }
            
            break;
        case MAP_VAR:
            if (node.length>0){
                variable = new TLMapVariable(node.name,node.length);
            }else {
                variable = new TLMapVariable(node.name);
            }
            break;
        default:
            throw new TransformLangExecutorRuntimeException(node,
                    "variable declaration - "
                            + "unknown type for variable \""
                            + node.name + "\"");

        }
        
        if (variable==null)
            variable=new TLVariable(node.name,value);
        stack.storeVar(node.localVar, node.varSlot,variable );

        if (node.jjtHasChildren()) {
            node.jjtGetChild(0).jjtAccept(this, data);
            TLValue initValue = stack.pop();
            TLValueType type =variable.getType();
            if (type.isStrictlyCompatible(initValue.type)) {
                  variable.setValue(initValue);
            }else {
                throw new TransformLangExecutorRuntimeException(node,
                        "invalid assignment of \"" + initValue
                                + "\" to variable \"" + node.name
                                + "\" - incompatible data types");
            }
        }
        return data;
    }

    public Object visit(CLVFVariableLiteral node, Object data) {
        TLVariable var = stack.getVar(node.localVar, node.varSlot);
        TLValue index = null;
        if (node.indexSet) {
            try {
                switch (var.getType()) {
                case LIST:
                    node.jjtGetChild(0).jjtAccept(this, data);
                    index = stack.pop();
                    stack.push(var.getValue(index.getInt()));
                    break;
                case MAP:
                    node.jjtGetChild(0).jjtAccept(this, data);
                    index = stack.pop();
                    stack.push(var.getValue(index.getString()));
                    break;
                }
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(node,
                        "invalid index \"" + index + "\" of variable \""
                        + var.getName() + "\" - type "
                        + var.getType().toString(), ex);
            } 
        }else {
            stack.push(var.getValue());
        }
        return var;
    }

    public Object visit(CLVFAssignment node, Object data) {
        CLVFVariableLiteral varNode = (CLVFVariableLiteral) node.jjtGetChild(0);

        TLVariable variableToAssign = stack.getVar(varNode.localVar,
                varNode.varSlot);
        node.jjtGetChild(1).jjtAccept(this, data);
        TLValue valueToAssign = stack.pop();
        if (valueToAssign==null) {
            throw new TransformLangExecutorRuntimeException(node,
                    "invalid assignment of \"" + valueToAssign
                            + "\" to variable \"" + varNode.varName+"\"");
        }
        
        
        TLValue index = null;
        switch (varNode.varType) {
        case LIST_VAR:
			if (varNode.usedIndex) {
				try {
					//	we always assign duplicate object to list variable
					if (!valueToAssign.isNull() && valueToAssign.getType().isPrimitive())
						valueToAssign = valueToAssign.duplicate();
					// scalar context
					if (varNode.indexSet) {
						varNode.jjtGetChild(0).jjtAccept(this, data);
						index = stack.pop();
						variableToAssign
								.setValue(index.getInt(), valueToAssign);
					} else {
						variableToAssign.setValue(-1, valueToAssign);
					}
				} catch (IndexOutOfBoundsException ex) {
					throw new TransformLangExecutorRuntimeException(
							node,
							"index \""
									+ index
									+ "\" is outside current limits of list/array \""
									+ varNode.varName + "\"", ex);
				} catch (Exception ex) {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of \"" + valueToAssign
									+ "\" to variable \"" + varNode.varName
									+ "\"", ex);
				}
			} else {
				// list context
				if (valueToAssign.type == variableToAssign.getType()) {
					((TLListVariable) variableToAssign).setValue(valueToAssign);
				} else {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of scalar value to list/array \""
									+ varNode.varName + "\"");
				}
			}

			break;
        case MAP_VAR:
        	if (varNode.usedIndex) {
				// scalar context
        		// we always assign duplicate object to list variable
				if (!valueToAssign.isNull() && valueToAssign.getType().isPrimitive())
					valueToAssign = valueToAssign.duplicate();
				if (varNode.indexSet) {
					try {
						varNode.jjtGetChild(0).jjtAccept(this, data);
						index = stack.pop();
						variableToAssign.setValue(index.getString(),
								valueToAssign);
					} catch (Exception ex) {
						throw new TransformLangExecutorRuntimeException(node,
								"invalid assignment of \"" + valueToAssign
										+ "\" to variable \"" + varNode.varName
										+ "\"", ex);
					}
				} else {
					// no key specified, we add values from map
					variableToAssign.setValue(null, valueToAssign);
				}
			} else {
				variableToAssign.setValue(valueToAssign);
			}
			break;
        case BYTE_VAR:
        	if (varNode.usedIndex) {
				try {
					// scalar context
					if (varNode.indexSet) {
						varNode.jjtGetChild(0).jjtAccept(this, data);
						index = stack.pop();
						variableToAssign
								.setValue(index.getInt(), valueToAssign);
					} else {
						variableToAssign.setValue(-1, valueToAssign);
					}
				} catch (IndexOutOfBoundsException ex) {
					throw new TransformLangExecutorRuntimeException(
							node,
							"index \""
									+ index
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
				if (valueToAssign.type == variableToAssign.getType()) {
					((TLListVariable) variableToAssign).setValue(valueToAssign);
				} else {
					throw new TransformLangExecutorRuntimeException(node,
							"invalid assignment of scalar value byte array \""
									+ varNode.varName + "\"");
				}
			}
        	break;
        default:
            TLValueType type=variableToAssign.getType();
            if (type.isCompatible(valueToAssign.type)) {
                    variableToAssign.setValue(valueToAssign);
            } else {
                throw new TransformLangExecutorRuntimeException(node,
                        "invalid assignment of \"" + valueToAssign
                                + "\" [" + valueToAssign.type
                                + "] to variable \""
                                + variableToAssign.getName() + "\" ["
                                + variableToAssign.getType()
                                + "] \" - incompatible data types");
            }
        }
        return data;
    }

    
    public Object visit(CLVFMapping node, Object data) {
        DataField field=outputRecords[node.recordNo].getField(node.fieldNo);
        int arity=node.jjtGetNumChildren(); // how many children we have defined
        TLValue value=null;
        try{
            // we try till success or no more options
            for (int i=0;i<arity;i++){
                node.jjtGetChild(i).jjtAccept(this, data);
                value=stack.pop();
                try{
                    value.copyToDataField(field);
                    break; // success during assignment, finish looping
               }catch(Exception ex){
                    if (i == arity-1)
                        throw ex;
               }
            }
            
        }catch(BadDataFormatException ex){
            if (!outputRecords[node.recordNo].getField(node.fieldNo).getMetadata().isNullable()){
                throw new TransformLangExecutorRuntimeException(node,"can't assign NULL to \"" + node.fieldName + "\"");
            }
            
            throw new TransformLangExecutorRuntimeException(node,"data format exception when mapping \"" + node.fieldName + "\" - assigning \""
                        + value + "\"");
        }catch(TransformLangExecutorRuntimeException ex){
        	throw ex;
    	}catch(Exception ex){
            String msg=ex.getMessage();
            throw new TransformLangExecutorRuntimeException(node,
                    (msg!=null ? msg : "") +
                    " when mapping \"" + node.fieldName + "\" ("+DataFieldMetadata.type2Str(field.getType())
                    +") - assigning \"" + value + "\" ("+(value!=null ? value.getType().getName(): "unknown type" )+")");
            
        }
        return data;
    }
    
    
    
    /*
     * Declaration & calling of Functions here
     */
    public Object visit(CLVFFunctionCallStatement node, Object data) {
        // EXTERNAL FUNCTION
        if (node.externalFunction != null) {
            // put call parameters on stack
            node.childrenAccept(this, data);
            // convert stack content into values
            try {
                TLValue returnVal = node.externalFunction.execute(stack.pop(
                        node.externalFunctionParams, node.jjtGetNumChildren()),
                        node.context);
                stack.push(returnVal);
            } catch (TransformLangExecutorRuntimeException ex) {
                ex.setNode(node);
                throw ex;
            } catch (Exception ex){
            	String msg="Java exception ["+ex.getClass().getName()+"] occured during external function call !";
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
            int numChildren = executionNode.jjtGetNumChildren();
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
        int numChildren=executionNode.jjtGetNumChildren();
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
    
    
    public Object visit(CLVFRaiseErrorNode node,Object data){
        node.jjtGetChild(0).jjtAccept(this, data);
        TLValue a = stack.pop();
        throw new TransformLangExecutorRuntimeException(node,null,
                    "!!! Exception raised by user: "+((a!=null) ? a.toString() : "no message"));
        
    }
    
    
    public Object visit(CLVFEvalNode node, Object data) {
        // get TL expression
    	node.jjtGetChild(0).jjtAccept(this, data);
    	String src=stack.pop().getString();
    	CLVFStart parseTree;
    	
    	// construct parser
    	try{
    	((TransformLangParser)parser).ReInit(new ByteArrayInputStream(src.getBytes(TransformLangParser.SRC_ENCODING)), TransformLangParser.SRC_ENCODING);
	     	parseTree = ((TransformLangParser)parser).Start();
    	}catch(ParseException ex){
    		 throw new TransformLangExecutorRuntimeException(node,
                     "Can't parse \"eval\" expression");
    	}catch(UnsupportedEncodingException ex){
    		throw new RuntimeException("Error in \"eval\" execution/parsing." ,ex);
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
    	visit(parseTree,data);
    	
        return data;
    }

    
    
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
            node.sequence.reset();
            retVal=Stack.NUM_ZERO;
            break;
        case CLVFSequenceNode.OP_CURRENT:
            switch(node.retType){
            case LONG_VAR:
            	retVal.getNumeric().setValue(node.sequence.currentValueLong());
                break;
            case STRING_VAR:
                retVal.setValue(node.sequence.currentValueString());
                break;
            default:
                retVal.getNumeric().setValue(node.sequence.currentValueInt());
            }
            break;
            default: // default is next value from sequence
                switch(node.retType){
                case LONG_VAR:
                    retVal.getNumeric().setValue(node.sequence.nextValueLong());
                    break;
                case STRING_VAR:
                    retVal.setValue(node.sequence.nextValueString());
                    break;
                default:
                    retVal.getNumeric().setValue(node.sequence.nextValueInt());
                }
        }
        stack.push(retVal);
        return data;
    }
    
    public Object visit(CLVFLookupNode node, Object data) {
		DataRecord record = null;
		if (node.lookup == null) {
			node.lookup = graph.getLookupTable(node.lookupName);
			if (node.lookup == null) {
				throw new TransformLangExecutorRuntimeException(node,
						"Can't obtain LookupTable \"" + node.lookupName
								+ "\" from graph \"" + graph.getName() + "\"");
			}
			if (node.opType == CLVFLookupNode.OP_GET || node.opType==CLVFLookupNode.OP_NEXT) {
				node.fieldNum = node.lookup.getMetadata().getFieldPosition(
						node.fieldName);
				if (node.fieldNum < 0) {
					throw new TransformLangExecutorRuntimeException(node,
							"Invalid field name \"" + node.fieldName
									+ "\" at LookupTable \"" + node.lookupName
									+ "\" in graph \"" + graph.getName() + "\"");
				}
			}
		}
		switch (node.opType) {
		case CLVFLookupNode.OP_INIT:
			try {
				node.lookup.init();
			} catch (ComponentNotReadyException ex) {
				throw new TransformLangExecutorRuntimeException(node,
						"Error when initializing lookup table \""
								+ node.lookupName + "\" :", ex);
			}
			return data;
		case CLVFLookupNode.OP_FREE:
			node.lookup.free();
			return data;
		case CLVFLookupNode.OP_NUM_FOUND:
			stack.push(new TLValue(TLValueType.INTEGER, new CloverInteger(
					node.lookup.getNumFound())));
			return data;
		case CLVFLookupNode.OP_GET:
			Object keys[] = new Object[node.jjtGetNumChildren()];
			for (int i = 0; i < node.jjtGetNumChildren(); i++) {
				node.jjtGetChild(i).jjtAccept(this, data);
				keys[i] = stack.pop().getValue();
			}
			record = node.lookup.get(keys);
			break;
		default: // CLVFLookupNode.OP_NEXT:
			record = node.lookup.getNext();

		}

		if (record != null) {
			stack.push(new TLValue(record.getField(node.fieldNum)));
		} else {
			stack.push(Stack.NULL_VAL);
		}

		return data;
	}
    
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


    public Object visit(CLVFImportSource node,Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    public Object visit(CLVFSymbolNameExp node,Object data) {
        stack.push(node.typeValue);
        return data;
    }
    
    public Object visit(CLVFOperator node,Object data) {
        return data;
    }
 
    public Object visit(CLVFPostfixExpression node,Object data) {
    	// get variable && put value on stack by executing child node
    	Node child=node.jjtGetChild(0);
    	if (! (child instanceof CLVFVariableLiteral)){
    		throw new TransformLangExecutorRuntimeException(node,"postfix expression is allowed only on variable");
    	}
    	TLVariable var=(TLVariable)child.jjtAccept(this, data);
    	int operatorType=((CLVFOperator)node.jjtGetChild(1)).kind;
    	
    	if (operatorType==INCR) {
            if (var.getType().isNumeric()) {
                var.getValue().getNumeric().add(Stack.NUM_ONE_P);
            }else if (var.getType()==TLValueType.DATE) {
                stack.calendar.setTime(var.getValue().getDate());
                stack.calendar.add(Calendar.DATE, 1);
                var.getValue().setValue(stack.calendar.getTime());
            }else {
                throw new TransformLangExecutorRuntimeException(node,"variable ["+var+"] is not of numeric or date type");
            }
    	}else{
    		if (var.getType().isNumeric()) {
                var.getValue().getNumeric().sub(Stack.NUM_ONE_P);
            }else if (var.getType()==TLValueType.DATE) {
                stack.calendar.setTime(var.getValue().getDate());
                stack.calendar.add(Calendar.DATE, -1);
                var.getValue().setValue(stack.calendar.getTime());
            }else {
                throw new TransformLangExecutorRuntimeException(node,"variable ["+var+"] is not of numeric or date type");
            }
    	}
    	
        return data;
    }
    
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
				var.getValue().getNumeric().add(
						operatorType == INCR ? Stack.NUM_ONE_P
								: Stack.NUM_MINUS_ONE_P);
			} else if (var.getType() == TLValueType.DATE) {
				stack.calendar.setTime(var.getValue().getDate());
				stack.calendar
						.add(Calendar.DATE, operatorType == INCR ? 1 : -1);
				var.getValue().setValue(stack.calendar.getTime());
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
				stack.push(val.getBoolean() ? Stack.FALSE_VAL : Stack.TRUE_VAL);
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
				val.getNumeric().neg();
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
				val.getNumeric().abs();
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

}
