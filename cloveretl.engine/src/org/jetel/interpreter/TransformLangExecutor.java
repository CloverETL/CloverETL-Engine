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

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.HugeDecimal;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.node.*;
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
     * these data records - assigment (in code) to output data field
     * will result in assigment to one of these data records.
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
     * Method which returns result of executing parse tree.<br>
     * Basically, it returns whatever object was left on top of executor's
     * stack.
     * 
     * @return
     */
    public Object getResult() {
        return stack.pop();
    }
    
    public Object getGlobalVariable(int varSlot){
        return stack.getGlobalVar(varSlot);
    }
    

    public void setGlobalVariable(int varSlot,Object value){
        stack.storeGlobalVar(varSlot,value);
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
        Object a=stack.pop();
        
        if (! (a instanceof Boolean)){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }else{
        	if (((Boolean)a).booleanValue()){
        		stack.push(Stack.TRUE_VAL);
        		return data;
        	}
        }
        
        node.jjtGetChild(1).jjtAccept(this, data);
        a=stack.pop();

        if (! (a instanceof Boolean)){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }
        
        if (((Boolean) a).booleanValue()) {
            stack.push(Stack.TRUE_VAL);

        } else {
            stack.push(Stack.FALSE_VAL);
        }
        return data;
    }

    public Object visit(CLVFAnd node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a=stack.pop();
        
        if (! (a instanceof Boolean)){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }else{
        	if (!((Boolean)a).booleanValue()){
        		stack.push(Stack.FALSE_VAL);
        		return data;
        	}
        }
        
        node.jjtGetChild(1).jjtAccept(this, data);
        a=stack.pop();

        if (! (a instanceof Boolean)){
        	Object params[]=new Object[]{a};
            throw new TransformLangExecutorRuntimeException(node,params,"logical condition does not evaluate to BOOLEAN value");
        }

        
        if (!((Boolean)a).booleanValue()) {
            stack.push(Stack.FALSE_VAL);
            return data;
        } else {
            stack.push(Stack.TRUE_VAL);
            return data;
        }
    }

    public Object visit(CLVFComparison node, Object data) {
        int cmpResult = 2;
        boolean lValue = false;
        // special handling for Regular expression
        if (node.cmpType == REGEX_EQUAL) {
            node.jjtGetChild(0).jjtAccept(this, data);
            Object field1 = stack.pop();
            node.jjtGetChild(1).jjtAccept(this, data);
            Object field2 = stack.pop();
            if (field1 instanceof CharSequence && field2 instanceof Matcher) {
                Matcher regex = (Matcher) field2;
                regex.reset(((CharSequence) field1));
                if (regex.matches()) {
                    lValue = true;
                } else {
                    lValue = false;
                }
            } else {
                Object[] arguments = { field1, field2 };
                throw new TransformLangExecutorRuntimeException(node,arguments,
                        "regex equal - wrong type of literal(s)");
            }

            // other types of comparison
        } else {
            node.jjtGetChild(0).jjtAccept(this, data);
            Object a = stack.pop();
            node.jjtGetChild(1).jjtAccept(this, data);
            Object b = stack.pop();

            try {
                if (a instanceof Numeric && b instanceof Numeric) {
                    cmpResult = ((Numeric) a).compareTo((Numeric) b);
                    /*
                     * }else if (a instanceof Number && b instanceof Number){
                     * cmpResult=Compare.compare((Number)a,(Number)b);
                     */
                } else if (a instanceof Date && b instanceof Date) {
                    cmpResult = ((Date) a).compareTo((Date) b);
                } else if (a instanceof CharSequence
                        && b instanceof CharSequence) {
                    cmpResult = Compare.compare((CharSequence) a,
                            (CharSequence) b);
                } else if (a instanceof Boolean && b instanceof Boolean) {
                	if (node.cmpType==EQUAL || node.cmpType==NON_EQUAL){ 
                		cmpResult = ((Boolean) a).equals(b) ? 0 : -1;
                	}else{
                		Object arguments[] = { a, b };
                		throw new TransformLangExecutorRuntimeException(node,arguments,
                        "compare - unsupported cmparison operator ["+tokenImage[node.cmpType]+"] for literals/expressions");
                	}
                } else {
                    Object arguments[] = { a, b };
                    throw new TransformLangExecutorRuntimeException(node,arguments,
                            "compare - incompatible literals/expressions");
                }
            } catch (ClassCastException ex) {
                Object arguments[] = { a, b };
                throw new TransformLangExecutorRuntimeException(node,arguments,
                        "compare - incompatible literals/expressions");
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
                logger.fatal("Internal error: Unsupported comparison operator !");
                throw new RuntimeException("Internal error - Unsupported comparison operator !");
            }
        }
        stack.push(lValue ? Stack.TRUE_VAL : Stack.FALSE_VAL);
        return data;
    }

    public Object visit(CLVFAddNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object b = stack.pop();
        

        if (a == null || b == null) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "add - NULL value not allowed");
        }

//        if (!(b instanceof Numeric || b instanceof CharSequence)) {
//            throw new TransformLangExecutorRuntimeException(node, new Object[] { b },
//                    "add - wrong type of literal");
//        }

        try {
            if (a instanceof Numeric && b instanceof Numeric) {
                Numeric result = ((Numeric) a).duplicateNumeric();
                result.add((Numeric) b);
                stack.push(result);
            } else if (a instanceof Date && b instanceof Numeric) {
                Calendar result = Calendar.getInstance();
                result.setTime((Date) a);
                result.add(Calendar.DATE, ((Numeric) b).getInt());
                stack.push(result.getTime());
            } else if (a instanceof CharSequence) {
                CharSequence a1 = (CharSequence) a;
                StringBuilder buf=new StringBuilder(a1.length()*2);
                StringUtils.strBuffAppend(buf,a1);
                if (b instanceof CharSequence) {
                    StringUtils.strBuffAppend(buf,(CharSequence)b);
                } else {
                    buf.append(b);
                }
                stack.push(buf);
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
        Object a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object b = stack.pop();
       

        if (a == null || b == null) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "sub - NULL value not allowed");
        }

        if (!(b instanceof Numeric)) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { b },
                    "sub - wrong type of literal");
        }

        if (a instanceof Numeric) {
            Numeric result = ((Numeric) a).duplicateNumeric();
            result.sub((Numeric) b);
            stack.push(result);
        } else if (a instanceof Date) {
            Calendar result = Calendar.getInstance();
            result.setTime((Date) a);
            result.add(Calendar.DATE, ((Numeric) b).getInt() * -1);
            stack.push(result.getTime());
        } else {
            Object[] arguments = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "sub - wrong type of literal(s)");
        }

        return data;
    }

    public Object visit(CLVFMulNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object b = stack.pop();
        

        if (a == null || b == null) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mul - NULL value not allowed");
        }

        if (!(b instanceof Numeric)) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { b },
                    "mul - wrong type of literal");
        }

        if (a instanceof Numeric) {
            Numeric result = ((Numeric) a).duplicateNumeric();
            result.mul((Numeric) b);
            stack.push(result);
        } else {
            Object[] arguments = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "mul - wrong type of literal(s)");
        }
        return data;
    }

    public Object visit(CLVFDivNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object b = stack.pop();
        

        if (a == null || b == null) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "div - NULL value not allowed");
        }

        if (!(b instanceof Numeric)) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { b },
                    "div - wrong type of literal");
        }

        if (a instanceof Numeric) {
            Numeric result = ((Numeric) a).duplicateNumeric();
            try{
            	result.div((Numeric) b);
            }catch(ArithmeticException ex){
            	Object[] arguments = { a, b };
            	throw new TransformLangExecutorRuntimeException(node,arguments,"div - aritmetic exception - "+ex.getMessage());
            }
            stack.push(result);
        } else {
            Object[] arguments = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "div - wrong type of literal(s)");
        }

        return data;
    }

    public Object visit(CLVFModNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object b = stack.pop();
        

        if (a == null || b == null) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { a, b },
                    "mod - NULL value not allowed");
        }

        if (!(b instanceof Numeric)) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { b },
                    "mod - wrong type of literal");
        }

        if (a instanceof Numeric) {
            Numeric result = ((Numeric) a).duplicateNumeric();
            result.mod((Numeric) b);
            stack.push(result);
        } else {
            Object[] arguments = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "mod - wrong type of literal(s)");
        }

        return data;
    }

    public Object visit(CLVFNegation node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object value = stack.pop();

        
        if (value instanceof Boolean) {
            stack.push(((Boolean) value).booleanValue() ? Stack.FALSE_VAL
                    : Stack.TRUE_VAL);
        } else {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { value },
                    "logical condition does not evaluate to BOOLEAN value");
        }

        return data;
    }

    public Object visit(CLVFSubStrNode node, Object data) {
        int length, from;

        node.jjtGetChild(0).jjtAccept(this, data);
        Object str = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object fromO = stack.pop();
        node.jjtGetChild(2).jjtAccept(this, data);
        Object lengthO = stack.pop();
       
        

        if (lengthO == null || fromO == null || str == null) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { lengthO,
                    fromO, str }, "substring - NULL value not allowed");
        }

        try {
            length = ((Numeric) lengthO).getInt();
            from = ((Numeric) fromO).getInt();
        } catch (Exception ex) {
            Object arguments[] = { lengthO, fromO, str };
            throw new TransformLangExecutorRuntimeException(node,arguments, "substring - "
                    + ex.getMessage());
        }

        if (str instanceof CharSequence) {
            stack.push(((CharSequence) str).subSequence(from, from + length));

        } else {
            Object[] arguments = { lengthO, fromO, str };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "substring - wrong type of literal(s)");
        }

        return data;
    }

    public Object visit(CLVFUppercaseNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof CharSequence) {
            CharSequence seq = (CharSequence) a;
            node.strBuf.setLength(0);
            node.strBuf.ensureCapacity(seq.length());
            for (int i = 0; i < seq.length(); i++) {
                node.strBuf.append(Character.toUpperCase(seq.charAt(i)));
            }
            stack.push(node.strBuf);
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "uppercase - wrong type of literal");
        }

        return data;
    }

    public Object visit(CLVFLowercaseNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof CharSequence) {
            CharSequence seq = (CharSequence) a;
            node.strBuf.setLength(0);
            node.strBuf.ensureCapacity(seq.length());
            for (int i = 0; i < seq.length(); i++) {
                node.strBuf.append(Character.toLowerCase(seq.charAt(i)));
            }
            stack.push(node.strBuf);
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "lowercase - wrong type of literal");
        }
        return data;
    }

    public Object visit(CLVFTrimNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        int start, end;

        if (a instanceof CharSequence) {
            CharSequence seq = (CharSequence) a;
            int length = seq.length();
            for (start = 0; start < length; start++) {
                if (seq.charAt(start) != ' ' && seq.charAt(start) != '\t') {
                    break;
                }
            }
            for (end = length - 1; end >= 0; end--) {
                if (seq.charAt(end) != ' ' && seq.charAt(end) != '\t') {
                    break;
                }
            }
            if (start > end)
                stack.push("");
            else
                stack.push(seq.subSequence(start, end + 1));
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "trim - wrong type of literal");
        }
        return data;
    }

    public Object visit(CLVFLengthNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof CharSequence) {
            stack.push(new CloverInteger(((CharSequence) a).length()));
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "lenght - wrong type of literal");
        }

        return data;
    }

    public Object visit(CLVFTodayNode node, Object data) {
        stack.push(stack.calendar.getTime() );

        return data;
    }

    public Object visit(CLVFIsNullNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object value = stack.pop();

        if (value == null) {
            stack.push(Stack.TRUE_VAL);
        } else {
            stack.push(Stack.FALSE_VAL);
        }

        return data;
    }

    public Object visit(CLVFNVLNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object value = stack.pop();

        if (value == null) {
            node.jjtGetChild(1).jjtAccept(this, data);
            // not necessary: stack.push(stack.pop());
        } else {
            stack.push(value);
        }

        return data;
    }

    public Object visit(CLVFLiteral node, Object data) {
        stack.push(node.value);
        return data;
    }

    public Object visit(CLVFInputFieldLiteral node, Object data) {
        DataRecord record = inputRecords[node.recordNo];
		if (record == null) {
			stack.push(null);
		} else {
			DataField field = record.getField(node.fieldNo);
			if (field instanceof Numeric) {
				stack.push(((Numeric) field).duplicateNumeric());
			} else {
				stack.push(field.getValue());
			}
		}
        
        // old
		// stack.push(inputRecords[node.recordNo].getField(node.fieldNo).getValue());
        
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
    
    public Object visit(CLVFGlobalParameterLiteral node, Object data) {
        stack.push(globalParameters!=null ? globalParameters.getProperty(node.name) : null);
        return data;
    }
    
    public Object visit(CLVFRegexLiteral node, Object data) {
        stack.push(node.matcher);
        return data;
    }

    public Object visit(CLVFConcatNode node, Object data) {
        Object a;
        StringBuilder strBuf = new StringBuilder(40);
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, data);
            a = stack.pop();

            if (a instanceof CharSequence) {
                StringUtils.strBuffAppend(strBuf,(CharSequence) a);
            } else {
                if (a != null) {
                    strBuf.append(a);
                } else {
                    Object[] arguments = { a };
                    throw new TransformLangExecutorRuntimeException(node,arguments,
                            "concat - wrong type of literal(s)");
                }
            }
        }
        stack.push(strBuf);
        return data;
    }

    public Object visit(CLVFDateAddNode node, Object data) {
        int shiftAmount;

        node.jjtGetChild(0).jjtAccept(this, data);
        Object date = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object amount = stack.pop();

        try {
            shiftAmount = ((Numeric) amount).getInt();
        } catch (Exception ex) {
            Object arguments[] = { amount };
            throw new TransformLangExecutorRuntimeException(node,arguments, "dateadd - "
                    + ex.getMessage());
        }
        if (date instanceof Date) {
            node.calendar.setTime((Date) date);
            node.calendar.add(node.calendarField, shiftAmount);
            stack.push(node.calendar.getTime());
        } else {
            Object arguments[] = { date };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "dateadd - no Date expression");
        }

        return data;
    }

    public Object visit(CLVFDate2NumNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object date = stack.pop();

        if (date instanceof Date) {
            node.calendar.setTime((Date) date);
            stack.push(new CloverInteger(node.calendar.get(node.calendarField)));
        } else {
            Object arguments[] = { date };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "date2num - no Date expression");
        }

        return data;
    }

    
    public Object visit(CLVFDateDiffNode node, Object data) {
        Object date1, date2;

        node.jjtGetChild(0).jjtAccept(this, data);
        date1 = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        date2 = stack.pop();

        if (date1 instanceof Date && date2 instanceof Date) {
            long diffSec = (((Date) date1).getTime() - ((Date) date2).getTime()) / 1000;
            int diff = 0;
            switch (node.calendarField) {
            case Calendar.SECOND:
                // we have the difference in seconds
                diff = (int) diffSec;
                break;
            case Calendar.MINUTE:
                // how many minutes'
                diff = (int) diffSec / 60;
                break;
            case Calendar.HOUR_OF_DAY:
                diff = (int) diffSec / 3600;
                break;
            case Calendar.DAY_OF_MONTH:
                // how many days is the difference
                diff = (int) diffSec / 86400;
                break;
            case Calendar.WEEK_OF_YEAR:
                // how many weeks
                diff = (int) diffSec / 604800;
                break;
            case Calendar.MONTH:
                node.start.setTime((Date) date1);
                node.end.setTime((Date) date2);
                diff = (node.start.get(Calendar.MONTH) + node.start
                        .get(Calendar.YEAR) * 12)
                        - (node.end.get(Calendar.MONTH) + node.end
                                .get(Calendar.YEAR) * 12);
                break;
            case Calendar.YEAR:
                node.start.setTime((Date) date1);
                node.end.setTime((Date) date2);
                diff = node.start.get(node.calendarField)
                        - node.end.get(node.calendarField);
                break;
            default:
                Object arguments[] = { new Integer(node.calendarField) };
                throw new TransformLangExecutorRuntimeException(node,arguments,
                        "datediff - wrong difference unit");
            }
            stack.push(new CloverInteger(diff));
        } else {
            Object arguments[] = { date1, date2 };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "datediff - no Date expression");
        }

        return data;
    }

    public Object visit(CLVFMinusNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object value = stack.pop();

        if (value instanceof Numeric) {
            Numeric result = ((Numeric) value).duplicateNumeric();
            result.mul(Stack.NUM_MINUS_ONE);
            stack.push(result);
        } else {
            Object arguments[] = { value };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "minus - not a number");
        }

        return data;
    }

    public Object visit(CLVFReplaceNode node, Object data) {

        node.jjtGetChild(0).jjtAccept(this, data);
        Object str = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object regexO = stack.pop();
        node.jjtGetChild(2).jjtAccept(this, data);
        Object withO = stack.pop();
        
        

        if (withO == null || regexO == null || str == null) {
            throw new TransformLangExecutorRuntimeException(node, new Object[] { withO,
                    regexO, str }, "NULL value not allowed");
        }

        if (str instanceof CharSequence && withO instanceof CharSequence
                && regexO instanceof CharSequence) {

            if (node.pattern == null || !node.stored.equals(regexO)) {
                node.pattern = Pattern.compile(((CharSequence) regexO)
                        .toString());
                node.matcher = node.pattern.matcher((CharSequence) str);
                node.stored = regexO;
            } else {
                node.matcher.reset((CharSequence) str);
            }
            stack.push(node.matcher.replaceAll(((CharSequence) withO)
                    .toString()));

        } else {
            Object[] arguments = { withO, regexO, str };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "replace - wrong type of literal(s)");
        }

        return data;
    }

    public Object visit(CLVFNum2StrNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof Numeric) {
            if (node.radix == 10) {
                stack.push(((Numeric) a).toString());
            } else {
                if (a instanceof CloverInteger) {
                    stack.push(Integer.toString(((CloverInteger) a).getInt(),
                            node.radix));
                } else if (a instanceof CloverLong) {
                    stack.push(Long.toString(((CloverLong) a).getLong(),
                            node.radix));
                } else if (a instanceof CloverDouble && node.radix == 16) {
                    stack.push(Double.toHexString(((CloverDouble) a)
                            .getDouble()));
                } else {
                    Object[] arguments = { a, new Integer(node.radix) };
                    throw new TransformLangExecutorRuntimeException(node,
                            arguments,
                            "num2str - can't convert number to string using specified radix");
                }
            }
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node, arguments,
                    "num2str - wrong type of literal");
        }

        return data;
    }

    public Object visit(CLVFStr2NumNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof CharSequence) {
            try {
                Object value = null;
                switch (node.numType) {
                case INT_VAR:
                    value = new CloverInteger(Integer.parseInt(
                            ((CharSequence) a).toString(), node.radix));
                    break;
                case LONG_VAR:
                    value = new CloverLong(Long.parseLong(((CharSequence) a)
                            .toString(), node.radix));
                    break;
                case DECIMAL_VAR:
                    if (node.radix == 10) {
                        value = DecimalFactory.getDecimal(((CharSequence) a)
                                .toString());
                    } else {
                        Object[] arguments = { a, new Integer(node.radix) };
                        throw new TransformLangExecutorRuntimeException(node,
                                arguments,
                                "str2num - can't convert string to decimal number using specified radix");
                    }
                    break;
                default:
                    // get double/number type
                    switch (node.radix) {
                    case 10:
                    case 16:
                        value = new CloverDouble(Double
                                .parseDouble(((CharSequence) a).toString()));
                        break;
                    default:
                        Object[] arguments = { a, new Integer(node.radix) };
                        throw new TransformLangExecutorRuntimeException(node,
                                arguments,
                                "str2num - can't convert string to number/double number using specified radix");
                    }
                }
                stack.push(value);
            } catch (NumberFormatException ex) {
                Object[] arguments = { a };
                throw new TransformLangExecutorRuntimeException(node,
                        arguments, "str2num - can't convert \"" + a + "\"");
            }
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node, arguments,
                    "str2num - wrong type of literal");
        }

        return data;
    }
    
    public Object visit(CLVFDate2StrNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof Date) {
                stack.push(node.dateFormat.format((Date)a));
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "date2str - wrong type of literal");
        }

        return data;
    }
    
    public Object visit(CLVFStr2DateNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof CharSequence) {
            try {
                stack.push(node.dateFormat.parse(((CharSequence)a).toString()));
            } catch (java.text.ParseException ex) {
                Object[] arguments = { a };
                throw new TransformLangExecutorRuntimeException(node,arguments,
                        "str2date - can't convert \"" + a + "\"");
            }
        } else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "str2date - wrong type of literal");
        }

        return data;
    }

    public Object visit(CLVFIffNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object condition = stack.pop();

        if (condition instanceof Boolean) {
            if (((Boolean) condition).booleanValue()) {
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
        Object a = stack.pop();
        if (node.jjtGetNumChildren()>1){
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
            condition = ((Boolean) stack.pop()).booleanValue();
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
                condition = ((Boolean) stack.pop()).booleanValue();
            } catch (ClassCastException ex) {
                throw new TransformLangExecutorRuntimeException(node,"loop condition does not evaluate to BOOLEAN value");
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
            condition = ((Boolean) stack.pop()).booleanValue();
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
                condition = ((Boolean) stack.pop()).booleanValue();
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
            condition = ((Boolean) stack.pop()).booleanValue();
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
                condition = ((Boolean) stack.pop()).booleanValue();
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
        Object switchVal=stack.pop();
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
        Object switchVal = stack.pop();
        node.jjtGetChild(0).jjtAccept(this, data);
        Object value = stack.pop();
        try {
            if (switchVal instanceof Numeric) {
                match = (((Numeric) value).compareTo((Numeric) switchVal) == 0);
            } else if (switchVal instanceof CharSequence) {
                match = (Compare.compare((CharSequence) switchVal,
                        (CharSequence) value) == 0);
            } else if (switchVal instanceof Date) {
                match = (((Date) switchVal).compareTo((Date) value) == 0);
            } else if (switchVal instanceof Boolean) {
                match = ((Boolean) switchVal).equals((Boolean) value);
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
        }else{
            return Stack.FALSE_VAL;
        }
    }

    public Object visit(CLVFPlusPlusNode node, Object data) {
        Node childNode = node.jjtGetChild(0);
        if (childNode instanceof CLVFVariableLiteral) {
                CLVFVariableLiteral varNode=(CLVFVariableLiteral) childNode;
                Object var=stack.getVar(varNode.localVar, varNode.varSlot);
                    if (var instanceof Numeric){
                        ((Numeric)var).add(Stack.NUM_ONE);
                        stack.push(((Numeric)var).duplicateNumeric());
                    }else if (var instanceof Date){
                        stack.calendar.setTime((Date)var);
                        stack.calendar.add(Calendar.DATE, 1);
                        stack.push(stack.calendar.getTime());
                    }else{
                        throw new TransformLangExecutorRuntimeException(node,"variable is not of numeric or date type");
                    }
        } else {
            childNode.jjtAccept(this, data);
            try {
                Numeric num = ((Numeric) stack.pop()).duplicateNumeric();
                num.add(Stack.NUM_ONE);
                stack.push(num);
            } catch (ClassCastException ex) {
                throw new TransformLangExecutorRuntimeException(node,"expression is not of numeric type");
            }catch (NullPointerException ex){
                throw new TransformLangExecutorRuntimeException(node,"missing or invalid numeric expression");
            }
        }
        return data;
    }

    public Object visit(CLVFMinusMinusNode node, Object data) {
        Node childNode = node.jjtGetChild(0);
        if (childNode instanceof CLVFVariableLiteral) {
            CLVFVariableLiteral varNode=(CLVFVariableLiteral) childNode;
            Object var=stack.getVar(varNode.localVar, varNode.varSlot);
                if (var instanceof Numeric){
                    ((Numeric)var).sub(Stack.NUM_ONE);
                    stack.push(((Numeric)var).duplicateNumeric());
                }else if (var instanceof Date){
                    stack.calendar.setTime((Date)var);
                    stack.calendar.add(Calendar.DATE, 1);
                    stack.push(stack.calendar.getTime());
                }else{
                    throw new TransformLangExecutorRuntimeException(node,"variable is not of numeric or date type");
                }
        } else {
            childNode.jjtAccept(this, data);
            try {
                Numeric num = ((Numeric) stack.pop()).duplicateNumeric();
                num.add(Stack.NUM_ONE);
                stack.push(num);
            } catch (ClassCastException ex) {
                throw new TransformLangExecutorRuntimeException(node,"expression is not of numeric type");
            }catch (NullPointerException ex){
                throw new TransformLangExecutorRuntimeException(node,"missing or invalid numeric expression");
            }
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
        // test for duplicite declaration - should have been done before
        /*if (stack.symtab.containsKey(node.name)) {
            throw new TransformLangExecutorRuntimeException(node,
                    "variable already declared - \"" + node.name + "\"");
        }*/
        Object value;
        // create global/local variable
        switch (node.type) {
        case INT_VAR:
            value= new CloverInteger(0);
            break;
        case LONG_VAR:
            value= new CloverLong(0);
            break;
        case DOUBLE_VAR:
            value= new CloverDouble(0);
            break;
        case DECIMAL_VAR:
            if (node.length>0){
                if (node.precision>0){
                    value = DecimalFactory.getDecimal(node.length,node.precision);
                }else{
                    value = DecimalFactory.getDecimal(node.length,0);
                }
            }else{
                value= DecimalFactory.getDecimal();
            }
            break;
        case STRING_VAR:
            value= new StringBuilder();
            break;
        case DATE_VAR:
            value=new Date();
            break;
        case BOOLEAN_VAR:
            value=  Stack.FALSE_VAL;
            break;
        default:
            throw new TransformLangExecutorRuntimeException(node,
                    "variable declaration - "
                            + "unknown variable type for variable \""
                            + node.name + "\"");

        }
        stack.storeVar(node.localVar, node.varSlot, value);
        
        return data;
    }

    public Object visit(CLVFVariableLiteral node, Object data) {
        Object var = stack.getVar(node.localVar, node.varSlot);
        // variable can be null
            stack.push(var);
        /*
        if (var != null) {
            stack.push(var);
        } else {
            throw new TransformLangExecutorRuntimeException(node, "unknown variable \""
                    + node.varName + "\"");
        }
        */
        return data;
    }

    public Object visit(CLVFAssignment node, Object data) {
        CLVFVariableLiteral childNode=(CLVFVariableLiteral) node.jjtGetChild(0);

        Object variable = stack.getVar(childNode.localVar,childNode.varSlot);
        node.jjtGetChild(1).jjtAccept(this, data);
        Object value = stack.pop();
        try {
            if (variable instanceof Numeric) {
                    ((Numeric) variable).setValue((Numeric) value);
            } else if (variable instanceof StringBuilder) {
                StringBuilder var = (StringBuilder) variable;
                var.setLength(0);
                StringUtils.strBuffAppend(var,(CharSequence) value);
            } else if (variable instanceof Boolean) {
                stack.storeVar(childNode.localVar,childNode.varSlot, (Boolean)value); // boolean is not updatable - we replace the reference
                // stack.put(varName,((Boolean)value).booleanValue() ?
                // Stack.TRUE_VAL : Stack.FALSE_VAL);
            } else if (variable instanceof Date) {
                ((Date) variable).setTime(((Date) value).getTime());
            } else {
                throw new TransformLangExecutorRuntimeException(node,
                        "unknown variable \"" + childNode.varName + "\"");
            }
        } catch (ClassCastException ex) {
            throw new TransformLangExecutorRuntimeException(node,
                    "invalid assignment of \"" + value + "\" to variable \""
                            + childNode.varName + "\" - incompatible data types");
        } catch (NumberFormatException ex){
            throw new TransformLangExecutorRuntimeException(node,
                    "invalid assignment of number \"" + value + "\" to variable \"" + childNode.varName + "\" : "+ex.getMessage());    
        } catch (TransformLangExecutorRuntimeException ex){
            throw ex;
        } catch (Exception ex){
            throw new TransformLangExecutorRuntimeException(node,
                    "invalid assignment of \"" + value + "\" to variable \"" + childNode.varName + "\" : "+ex.getMessage());  
        }

        return data;
    }

    
    public Object visit(CLVFMapping node, Object data) {
        DataField field=outputRecords[node.recordNo].getField(node.fieldNo);
        int arity=node.arity; // how many children we have defined
        Object value=null;
        try{
            // we try till success or no more options
            for (int i=0;i<arity;i++){
                node.jjtGetChild(i).jjtAccept(this, data);
                value=stack.pop();
                try{
                    // TODO: small hack
                    if (field instanceof Numeric){
                        ((Numeric)field).setValue((Numeric)value);
                    }else{
                        field.setValue(value);
                    }
                }catch(BadDataFormatException ex){
                    if (i == arity)
                        throw ex;
                    else
                        continue;
                    
                }catch(Exception ex){
                    if (i == arity)
                        throw ex;
                    else
                        continue;
                }
                break; // success during assignment, finish looping
            }
            
        }catch(BadDataFormatException ex){
            if (!outputRecords[node.recordNo].getField(node.fieldNo).getMetadata().isNullable()){
                throw new TransformLangExecutorRuntimeException(node,"can't assign NULL to \"" + node.fieldName + "\"");
            }else{
                throw new TransformLangExecutorRuntimeException(node,"data format exception when mapping \"" + node.fieldName + "\" - assigning \""
                            + value + "\"");
            }
        }catch(TransformLangExecutorRuntimeException ex){
        	throw ex;
    	}catch(Exception ex){
            String msg=ex.getMessage();
            throw new TransformLangExecutorRuntimeException(node,
                    (msg!=null ? msg : "") +
                    " when mapping \"" + node.fieldName + "\" ("+DataFieldMetadata.type2Str(field.getType())
                    +") - assigning \"" + value + "\" ("+(value!=null ? value.getClass(): "unknown class" )+")");
            
        }
        
        return data;
    }
    
    
    
    /*
     * Declaration & calling of Functions here
     */
    public Object visit(CLVFFunctionCallStatement node, Object data) {
        //put call parameters on stack
        node.childrenAccept(this,data);
        CLVFFunctionDeclaration executionNode=node.callNode;
        // open call frame
        stack.pushFuncCallFrame();
        // store call parameters from stack as local variables
        for (int i=executionNode.numParams-1;i>=0; stack.storeLocalVar(i--,stack.pop()));
       
        // execute function body
        // loop execution
        Object returnData;
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

    public Object visit(CLVFFunctionDeclaration node, Object data) {
        return data;
    }

    public Object visit(CLVFStatementExpression node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        return data;
    }

    public Object executeFunction(CLVFFunctionDeclaration executionNode, Object[] data) {
        //put call parameters on stack
        if (data==null){
            data=new Object[0];
        }
        // open call frame
        stack.pushFuncCallFrame();
        // store call parameters from stack as local variables
        for (int i=executionNode.numParams-1;i>=0; stack.storeLocalVar(i--,data[i]));
       
        // execute function body
        // loop execution
        Object returnData;
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
    
    /*
     * MATH functions log,log10,exp,pow,sqrt,round
     */
   
    public Object visit(CLVFSqrtNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof Numeric) {
            try{
                stack.push(new CloverDouble(Math.sqrt(((Numeric)a).getDouble()) ));
            }catch(Exception ex){
                throw new TransformLangExecutorRuntimeException(node,"Error when executing SQRT function",ex);
            }
        }else {
            Object[] arguments = { a};
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "sqrt - wrong type of literal(s)");
        }

        return data;
    }
    
    public Object visit(CLVFLogNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof Numeric) {
            try{
                stack.push(new CloverDouble(Math.log(((Numeric)a).getDouble()) ));
            }catch(Exception ex){
                throw new TransformLangExecutorRuntimeException(node,"Error when executing LOG function",ex);
            }
        }else {
            Object[] arguments = { a};
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "log - wrong type of literal(s)");
        }

        return data;
    }
    
    public Object visit(CLVFLog10Node node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof Numeric) {
            try{
                stack.push(new CloverDouble( Math.log10(((Numeric)a).getDouble())));
            }catch(Exception ex){
                throw new TransformLangExecutorRuntimeException(node,"Error when executing LOG10 function",ex);
            }
        }else {
            Object[] arguments = { a};
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "log10 - wrong type of literal(s)");
        }

        return data;
    }
    
    public Object visit(CLVFExpNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof Numeric) {
            try{
                stack.push(new CloverDouble( Math.exp(((Numeric)a).getDouble())));
            }catch(Exception ex){
                throw new TransformLangExecutorRuntimeException(node,"Error when executing EXP function",ex);
            }
        }else {
            Object[] arguments = { a};
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "exp - wrong type of literal(s)");
        }

        return data;
    }
    
    public Object visit(CLVFRoundNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();

        if (a instanceof Numeric) {
            try{
                stack.push(new CloverLong(Math.round(((Numeric)a).getDouble())));
            }catch(Exception ex){
                throw new TransformLangExecutorRuntimeException(node,"Error when executing ROUND function",ex);
            }
        }else {
            Object[] arguments = { a};
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "round - wrong type of literal(s)");
        }


        return data;
    }
    
    public Object visit(CLVFPowNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        node.jjtGetChild(1).jjtAccept(this, data);
        Object b = stack.pop();

        if (a instanceof Numeric && b instanceof Numeric) {
            try{
                stack.push(new CloverDouble(Math.pow(((Numeric)a).getDouble(),
                    ((Numeric)b).getDouble())));
            }catch(Exception ex){
                throw new TransformLangExecutorRuntimeException(node,"Error when executing POW function",ex);
            }
        }else {
            Object[] arguments = { a, b };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "pow - wrong type of literal(s)");
        }

        return data;
    }
    
    public Object visit(CLVFPINode node, Object data) {
        stack.push(Stack.NUM_PI);
        return data;
    }
    
    public Object visit(CLVFTruncNode node, Object data) {
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        
        if (a instanceof Date ) {
            stack.calendar.setTime((Date)a);
            stack.calendar.set(Calendar.HOUR_OF_DAY, 0);
            stack.calendar.set(Calendar.MINUTE , 0);
            stack.calendar.set(Calendar.SECOND , 0);
            stack.calendar.set(Calendar.MILLISECOND , 0);
            stack.push( stack.calendar.getTime() );
        }else if (a instanceof Numeric){
            stack.push(new CloverLong(((Numeric)a).getLong()));
        }else {
            Object[] arguments = { a };
            throw new TransformLangExecutorRuntimeException(node,arguments,
                    "trunc - wrong type of literal(s)");
        }

        return data;
    }
    
    public Object visit(CLVFRaiseErrorNode node,Object data){
        node.jjtGetChild(0).jjtAccept(this, data);
        Object a = stack.pop();
        throw new TransformLangExecutorRuntimeException(node,null,
                    "!!! Exception raised by user: "+((a!=null) ? a.toString() : "no message"));
        
    }
    
    public Object visit(CLVFSequenceNode node,Object data){
        Object seqVal=null;
        if (node.sequence==null){
            node.sequence=graph.getSequence(node.sequenceName);
            if (node.sequence==null){
                throw new TransformLangExecutorRuntimeException(node,
                        "Can't obtain Sequence \""+node.sequenceName+
                        "\" from graph \""+graph.getName()+"\"");
            }
        }
        switch(node.opType){
        case CLVFSequenceNode.OP_RESET:
            node.sequence.reset();
            return data;
        case CLVFSequenceNode.OP_CURRENT:
            switch(node.retType){
            case LONG_VAR:
                seqVal=new CloverLong(node.sequence.currentValueLong());
                break;
            case STRING_VAR:
                seqVal=node.sequence.currentValueString();
            default:
                seqVal=new CloverInteger(node.sequence.currentValueInt());
            }
            default: // default is next value from sequence
                switch(node.retType){
                case LONG_VAR:
                    seqVal=new CloverLong(node.sequence.nextValueLong());
                    break;
                case STRING_VAR:
                    seqVal=node.sequence.nextValueString();
                default:
                    seqVal=new CloverInteger(node.sequence.nextValueInt());
                }
        }
        stack.push(seqVal);
        return data;
    }
    
    public Object visit(CLVFLookupNode node,Object data){
        DataRecord record=null;
        if (node.lookup==null){
            node.lookup=graph.getLookupTable(node.lookupName);
            if (node.lookup==null){
                throw new TransformLangExecutorRuntimeException(node,
                        "Can't obtain LookupTable \""+node.lookupName+
                        "\" from graph \""+graph.getName()+"\"");
            }
            node.fieldNum=node.lookup.getMetadata().getFieldPosition(node.fieldName);
            if (node.fieldNum<0){
                throw new TransformLangExecutorRuntimeException(node,
                        "Invalid field name \""+node.fieldName+"\" at LookupTable \""+node.lookupName+
                        "\" in graph \""+graph.getName()+"\"");
            }
        }
        switch(node.opType){
        case CLVFLookupNode.OP_INIT:
            try{
                node.lookup.init();
            }catch(ComponentNotReadyException ex){
                throw new TransformLangExecutorRuntimeException(node,
                        "Error when initializing lookup table \""+node.lookupName+"\" :",
                        ex);
            }
            return data;
        case CLVFLookupNode.OP_FREE:
            node.lookup.free();
            return data;
        case CLVFLookupNode.OP_NUM_FOUND:
            stack.push(new CloverInteger(node.lookup.getNumFound()));
            return data;
        case CLVFLookupNode.OP_GET:
            Object keys[]=new Object[node.jjtGetNumChildren()];
            for(int i=0;i<node.jjtGetNumChildren();i++){
                node.jjtGetChild(i).jjtAccept(this, data);
                keys[i]=stack.pop();
            }
            record=node.lookup.get(keys);
            break;
        case CLVFLookupNode.OP_NEXT:
            record=node.lookup.getNext();
        }
        
        if(record!=null){
            stack.push(record.getField(node.fieldNum).getValue());
        }else{
            stack.push(null);
        }
        
        return data;
    }
    
    public Object visit(CLVFPrintLogNode node, Object data) {
        if (runtimeLogger == null) {
            throw new TransformLangExecutorRuntimeException(node,
                    "Can NOT perform" + "log operation - no logger defined");
        }
        Object msg = stack.pop();
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
}