/*
 *    jETeL/Clover.ETL - Java based ETL application framework.
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

import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;

import org.jetel.data.DataField;
import org.jetel.data.DateDataField;

/**
 * @author dpavlis
 * @since  16.9.2004
 *
 * Executor of FilterExpression parse tree
 */
public class FilterExpParserExecutor implements FilterExpParserVisitor, FilterExpParserConstants {
	
	protected Stack stack;
	
	
	/**
	 * Constructor
	 */
	public FilterExpParserExecutor() {
		stack=new Stack();
	}
	
	
	/**
	 * Method which returns result of executing parse tree.<br>
	 * Basically, it returns whatever object was left on top of
	 * executor's stack.
	 * 
	 * @return
	 */
	public Object getResult(){
		return stack.pop();
	}
	
	/* ************************************************************/
	
	/* implementation of visit methods for each class of AST node */
	
	/* ************************************************************/
	
	/* it seems to be necessary to define a visit() method for SimpleNode */
	
	public Object visit(SimpleNode node, Object data) {
		throw new InterpreterRuntimeException(node,"Error: Call to visit for SimpleNode");
	}
	
	
	public Object visit(CLVFStart node, Object data) {
		
		int i, k = node.jjtGetNumChildren();
		
		for (i = 0; i < k; i++)
			node.jjtGetChild(i).jjtAccept(this, data);
		
		return data ;  // this value is ignored in this example 
	}
	
	
	public Object visit(CLVFOr node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		if (((Boolean)(stack.pop())).booleanValue())
		{
			stack.push(Stack.TRUE_VAL);
			return data;
		}
		
		node.jjtGetChild(1).jjtAccept(this, data);
		
		if (((Boolean)stack.pop()).booleanValue())
		{
			stack.push(Stack.TRUE_VAL);
			
		}else{
			stack.push(Stack.FALSE_VAL);
		}
		return data;
	}
	
	public Object visit(CLVFAnd node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		
		if (!((Boolean)(stack.pop())).booleanValue())
		{
			stack.push(Stack.FALSE_VAL);
			return data;
		}
		
		node.jjtGetChild(1).jjtAccept(this, data);
		
		if (!((Boolean)(stack.pop())).booleanValue()){
			stack.push(Stack.FALSE_VAL);
			return data;
		}else{
			stack.push(Stack.TRUE_VAL);
			return data;
		}
	}
	
	public Object visit(CLVFComparison node, Object data){
		int cmpResult=2;
		boolean lValue=false; 
		// special handling for Regular expression
		if (node.cmpType==REGEX_EQUAL){
			node.jjtGetChild(0).jjtAccept(this, data);
			Object field1=stack.pop();
			node.jjtGetChild(1).jjtAccept(this, data);
			Object field2=stack.pop();
			if (field1 instanceof CharSequence && field2 instanceof Matcher ){
				Matcher regex=(Matcher)field2;
				regex.reset(((CharSequence)field1));
				if(regex.matches()){
					lValue=true;
				}else{
					lValue=false;
				}	
			}else{
				Object[] arguments={field1,field2};
				throw new InterpreterRuntimeException(arguments,"regex equal - wrong type of literal(s)");
			}
			
			// other types of comparison
		}else{
			node.jjtGetChild(0).jjtAccept(this, data);
			Object a=stack.pop();
			node.jjtGetChild(1).jjtAccept(this, data);
			Object b=stack.pop();
			
			try{
				if (a instanceof Double){
					cmpResult=compare(((Double)a).doubleValue(),((Number)b).doubleValue());
				}else if (a instanceof Integer){
					cmpResult=compare(((Integer)a).intValue(),((Number)b).intValue());
				}else if (a instanceof Date){
					cmpResult=((Date)a).compareTo((Date)b);
				}else if (a instanceof CharSequence){
					cmpResult=compare((CharSequence)a,(CharSequence)b);
				}else {
					Object arguments[]={a,b};
					throw new InterpreterRuntimeException(arguments,"compare - incompatible literals/expressions");
				}
			}catch(ClassCastException ex){
				Object arguments[]={a,b};
				throw new InterpreterRuntimeException(arguments,"compare - incompatible literals/expressions");
			}
			switch (node.cmpType) {
			case EQUAL:
				if (cmpResult == 0) {
					lValue=true;
				}
				break;// equal
			case LESS_THAN:
				if (cmpResult == -1) {
					lValue=true;
				}
				break;// less than
			case GREATER_THAN:
				if (cmpResult == 1) {
					lValue=true;
				}
				break;// grater than
			case LESS_THAN_EQUAL:
				if (cmpResult <= 0) {
					lValue=true;
				}
				break;// less than equal
			case GREATER_THAN_EQUAL:
				if (cmpResult >= 0) {
					lValue=true;
				}
				break;// greater than equal
			case NON_EQUAL:
				if (cmpResult != 0) {
					lValue=true;
				}
				break;
			default:
				// this should never happen !!!
				throw new RuntimeException("Unsupported cmparison operator !");
			}
		}
		stack.push(lValue ? Stack.TRUE_VAL : Stack.FALSE_VAL);
		return data;
	}
	
	public Object visit(CLVFAddNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		Object b = stack.pop();
		Object a = stack.pop();
		
		if ( a instanceof Integer){
			stack.push(new Integer(((Number)a).intValue() 
					+ ((Number)b).intValue())); 
		}else if (a instanceof Double){
			stack.push(new Double(((Number)a).doubleValue()  
					+ ((Number)b).doubleValue())); 
		}else {
			Object[] arguments={a,b};
			throw new InterpreterRuntimeException(arguments,"add - wrong type of literal(s)");
		}
		return data;
	}
	
	public Object visit(CLVFSubNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		Object b = stack.pop();
		Object a = stack.pop();
		
		if ( a instanceof Integer){
			stack.push(new Integer(((Number)a).intValue() 
					- ((Number)b).intValue())); 
		}else if (a instanceof Double){
			stack.push(new Double(((Number)a).doubleValue()  
					- ((Number)b).doubleValue())); 
		}else {
			Object[] arguments={a,b};
			throw new InterpreterRuntimeException(arguments,"sub - wrong type of literal(s)");
		}
		
		return data;
	}
	
	public Object visit(CLVFMulNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		Object b = stack.pop();
		Object a = stack.pop();
		
		if ( a instanceof Integer){
			stack.push(new Integer(((Number)a).intValue() 
					* ((Number)b).intValue())); 
		}else if (a instanceof Double){
			stack.push(new Double(((Number)a).doubleValue()  
					* ((Number)b).doubleValue())); 
		}else {
			Object[] arguments={a,b};
			throw new InterpreterRuntimeException(arguments,"mul - wrong type of literal(s)");
		}
		return data;
	}	
	
	public Object visit(CLVFDivNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		
		Object b = stack.pop();
		Object a = stack.pop();
		
		if ( a instanceof Integer){
			stack.push(new Integer(((Number)a).intValue() 
					/ ((Number)b).intValue())); 
		}else if (a instanceof Double){
			stack.push(new Double(((Number)a).doubleValue()  
					/ ((Number)b).doubleValue())); 
		}else {
			Object[] arguments={a,b};
			throw new InterpreterRuntimeException(arguments,"div - wrong type of literal(s)");
		}
		
		return data;
	}
	
	public Object visit(CLVFModNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data); 
		Object b = stack.pop();
		Object a = stack.pop();
		
		if ( a instanceof Integer){
			stack.push(new Integer(((Number)a).intValue() 
					% ((Number)b).intValue())); 
		}else if (a instanceof Double){
			stack.push(new Double(((Number)a).doubleValue()  
					% ((Number)b).doubleValue())); 
		}else {
			Object[] arguments={a,b};
			throw new InterpreterRuntimeException(arguments,"mod - wrong type of literal(s)");
		}
		
		return data;
	}
	
	public Object visit(CLVFNegation node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		stack.push( ((Boolean)stack.pop()).booleanValue() 
				? Stack.FALSE_VAL : Stack.TRUE_VAL);
		
		return data;
	}
	
	public Object visit(CLVFSubStrNode node, Object data){
		int length,from;
		
		node.jjtGetChild(0).jjtAccept(this, data);
		node.jjtGetChild(1).jjtAccept(this, data);
		node.jjtGetChild(2).jjtAccept(this, data); 
		
		try{
			length=((Number)stack.pop()).intValue();
			from=((Number)stack.pop()).intValue();
		}catch(Exception ex){
			Object arguments[]={};
			throw new InterpreterRuntimeException(arguments,"substring - "+ex.getMessage());
		}
		
		Object str=stack.pop();
		if (str instanceof CharSequence){
			stack.push(((CharSequence)str).subSequence(from,from+length));
			
		}else{
			Object[] arguments={str};
			throw new InterpreterRuntimeException(arguments,"substring - wrong type of literal(s)");
		}
		
		return data;
	}
	
	public Object visit(CLVFUppercaseNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		Object a = stack.pop();
		
		if ( a instanceof CharSequence){
			CharSequence seq=(CharSequence)a;
			node.strBuf.setLength(0);
			node.strBuf.ensureCapacity(seq.length());
			for(int i=0;i<seq.length();i++){
				node.strBuf.append(Character.toUpperCase(seq.charAt(i)));
			}
			stack.push(node.strBuf); 
		}else {
			Object[] arguments={a};
			throw new InterpreterRuntimeException(arguments,"uppercase - wrong type of literal");
		}
		
		return data;
	}
	
	public Object visit(CLVFLowercaseNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		
		Object a = stack.pop();
		
		if ( a instanceof CharSequence){
			CharSequence seq=(CharSequence)a;
			node.strBuf.setLength(0);
			node.strBuf.ensureCapacity(seq.length());
			for(int i=0;i<seq.length();i++){
				node.strBuf.append(Character.toLowerCase(seq.charAt(i)));
			}
			stack.push(node.strBuf); 
		}else {
			Object[] arguments={a};
			throw new InterpreterRuntimeException(arguments,"lowercase - wrong type of literal");
		}
		return data;
	}
	
	public Object visit(CLVFTrimNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		
		Object a = stack.pop();
		int start,end;
		
		if(a == null) {
			stack.push("");
		} else if ( a instanceof CharSequence){
			CharSequence seq=(CharSequence)a;
			int length=seq.length();
			for(start=0;start<length;start++){
				if (seq.charAt(start)!=' ' && seq.charAt(start)!='\t'){
					break;
				}
			}
			for(end=length-1;end>=0;end--){
				if (seq.charAt(end)!=' ' && seq.charAt(end)!='\t'){
					break;
				}
			}
			if(start > end) stack.push("");
			else stack.push(seq.subSequence(start,end + 1));
		}else {
			Object[] arguments={a};
			throw new InterpreterRuntimeException(arguments,"trim - wrong type of literal");
		}
		return data;
	}
	
	public Object visit(CLVFLengthNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		
		Object a = stack.pop();
		
		if ( a instanceof CharSequence){
			stack.push(new Integer(((CharSequence)a).length()));
		}else{
			Object[] arguments={a};
			throw new InterpreterRuntimeException(arguments,"lenght - wrong type of literal");
		}
		
		return data;
	}
	
	public Object visit(CLVFTodayNode node, Object data){
		java.util.Date today=Calendar.getInstance().getTime();
		stack.push(today);  
		
		return data;
	}
	
	public Object visit(CLVFIsNullNode node, Object data){
		Object value=node.jjtGetChild(0).jjtAccept(this, data);
		
		if (value instanceof DataField){
			if (((DataField)value).isNull()){
				stack.push(Stack.TRUE_VAL);
			}else{
				stack.push(Stack.FALSE_VAL);
			}
		}else{
			Object[] arguments={value};
			throw new InterpreterRuntimeException(arguments,"isnull - wrong type of literal");
		}
		
		
		return data;
	}
	
	public Object visit(CLVFLiteral node, Object data){
		stack.push(node.value);	 	
		return data;
	}
	
	public Object visit(CLVFJetelFieldLiteral node, Object data){
		stack.push(node.field.getValue());
		// we return reference to DataField so we can
		// perform extra checking in special cases
		return node.field;
	}
	
	public Object visit(CLVFRegexLiteral node, Object data){
		stack.push(node.matcher);
		return data;
	}
	
	public Object visit(CLVFConcatNode node, Object data){
		Object a;
		node.strBuf.setLength(0);
		for (int i=0;i<node.jjtGetNumChildren();i++){
			node.jjtGetChild(i).jjtAccept(this, data);
			a = stack.pop();
			
			if (a instanceof DataField){
				a = ((DataField)a).getValue();
			}
			
			if ( !(a instanceof CharSequence)){
				Object[] arguments={a};
				throw new InterpreterRuntimeException(arguments,"concat - wrong type of literal(s)");
			}else{
				CharSequence seqA=(CharSequence)a;
				node.strBuf.ensureCapacity(node.strBuf.length()+seqA.length());
				for(int j=0;j<seqA.length();j++){
					node.strBuf.append(seqA.charAt(j));
				}
			}
		}
		stack.push(node.strBuf);
		return data;
	}
	
	public Object visit(CLVFDateAddNode node, Object data){
		int amount;
		
		node.jjtGetChild(0).jjtAccept(this, data);
		Object date=stack.pop();
		
		if (date instanceof DateDataField){
			date=((DateDataField)date).getValue();
		}
		
		node.jjtGetChild(1).jjtAccept(this, data);
		
		try{
			amount=((Number)stack.pop()).intValue();  		
		}catch(Exception ex){
			Object arguments[]={};
			throw new InterpreterRuntimeException(arguments,"dateadd - "+ex.getMessage());
		}
		if (date instanceof Date){
			node.calendar.setTime((Date)date);
			node.calendar.add(node.calendarField,amount);
			stack.push(node.calendar.getTime());
		}else{
			Object arguments[]={date};
			throw new InterpreterRuntimeException(arguments,"dateadd - no Date expression");
		}
		
		return data;
	}
	
	
	public Object visit(CLVFDateDiffNode node, Object data) {
		Object date1, date2;

		node.jjtGetChild(0).jjtAccept(this, data);
		date1 = stack.pop();

		if (date1 instanceof DateDataField) {
			date1 = ((DateDataField) date1).getValue();
		}

		node.jjtGetChild(1).jjtAccept(this, data);
		date2 = stack.pop();
		if (date2 instanceof DateDataField) {
			date2 = ((DateDataField) date2).getValue();
		}

		if (date1 instanceof Date && date2 instanceof Date) {
			long diffSec = (((Date) date1).getTime() - ((Date) date2).getTime()) / 1000;
			int diff = 0;
			switch (node.calendarField) {
			case Calendar.SECOND:
				// we have the difference in seconds
				diff = (int) diffSec;
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
				throw new InterpreterRuntimeException(arguments,
						"datediff - wrong difference unit");
			}
			stack.push(new Integer(diff));
		} else {
			Object arguments[] = { date1, date2 };
			throw new InterpreterRuntimeException(arguments,
					"datediff - no Date expression");
		}

		return data;
	}
	
	
	public Object visit(CLVFMinusNode node, Object data){
		node.jjtGetChild(0).jjtAccept(this, data);
		Object value=stack.pop();
		if (value instanceof Double){
			stack.push(new Double(-1*((Double)value).doubleValue()));
		}else if (value instanceof Integer){
			stack.push(new Double(-1*((Integer)value).intValue()));
		}else{
			Object arguments[]={value};
			throw new InterpreterRuntimeException(arguments,"minus - not a number");
		}
		
		return data;
	}
	
	/**********************************************/
	/*
	 *  Miscelaneous utilities
	 * @author dpavlis
	 * @since  16.9.2004
	 *
	 */
	int compare(CharSequence a,CharSequence b){
		int aLenght = a.length();
		int bLenght = b.length();
		int compLength = (aLenght< bLenght  ? aLenght : bLenght );
		for (int i = 0; i < compLength; i++) {
			if (a.charAt(i) > b.charAt(i)) {
				return 1;
			} else if (a.charAt(i) < b.charAt(i)) {
				return -1;
			}
		}
		// strings seem to be the same (so far), decide according to the length
		if (aLenght == bLenght) {
			return 0;
		} else if (aLenght > bLenght) {
			return 1;
		} else {
			return -1;
		}
	}
	
	int compare(double a,double b){
		if (a>b) return 1; else if (b>a) return -1; else return 0;
	}
	
	int compare(int a,int b){
		if (a>b) return 1; else if (b>a) return -1; else return 0;
	}
	
}