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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jetel.ctl.data.Scope;
import org.jetel.data.DataRecord;

/**
 * @author David Palis <david.pavlis@javlin.cz>
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since  9.8.2004
 *
 * The class represents stack of CTL computation.
 */
public class Stack {
	
	/** Initial size */
    public static final int STACK_DEPTH = 32;
    /** Grow factor for stack array realocation */
    public static final float GROW_FACTOR = 1.6f;
    
    // useful instance variables (used when evaluating certain expressions) 
    public Calendar calendar; 
    
    private Object[] stack;
    private ArrayList<Object[]> variableStack;
    private int top = -1;
    
	public Stack(){
        this(STACK_DEPTH);
	}
	 
	public Stack(int depth){
		stack= new Object[depth];
		variableStack = new ArrayList<Object[]>();
		calendar = Calendar.getInstance();
	}

	/**
	 * clear stack (remove all object references) 
	 */
	public final void clear(){
		Arrays.fill(stack,null);
		variableStack.clear();
		top=-1;
	}
    
	/**
	 * Push any value on top of the stack
	 * @param o
	 */
	public final void push(Object o){
		if (top>=stack.length-1){
			// stack overflow -> reallocate
			Object[] newStack = new Object[(int)(stack.length * GROW_FACTOR)];
			System.arraycopy(stack, 0, newStack, 0, stack.length);
			this.stack = newStack;
		}
		stack[++top]=o;
	}
	
	
	/****************** POP METHODS FOLLOW ***************/
	public Integer popInt() {
		return (Integer)pop();
	}
	
	public Long popLong() {
		return (Long)pop();
	}
	
	public Double popDouble() {
		return (Double)pop();
	}
	
	public BigDecimal popDecimal() {
		return (BigDecimal)pop();
	}
	
	public Boolean popBoolean() {
		return (Boolean)pop();
	}
	
	public String popString() {
		return (String)pop();
	}
	
	public Date popDate() {
		return (Date)pop();
	}
	
	@SuppressWarnings("unchecked")
	public List<Object> popList() {
		return (List<Object>)pop();
	}
	
	@SuppressWarnings("unchecked")
	public Map<Object,Object> popMap() {
		return (Map<Object,Object>)pop();
	}
	
	public DataRecord popRecord() {
		return (DataRecord)pop();
	}
	
	public byte[] popByteArray() {
		return (byte[])pop();
	}

	
	/**
	 * Extracts the object on the top of the stack.
	 * @return top object placed on stack (last put)
	 */
	public final Object pop(){
        Object obj=stack[top];
		stack[top--]=null;
		return obj;
	}
	
	/**
	 * Peeks the object on the top of the stack.
	 * The stack is not modified by this operation.
	 * @return object on the top of the stack
	 */
	public final Object peek() {
		return stack[top];
	}
    
    /**
     * @return length/depth of stack - how many parameters it contains
     * @since 20.3.2007
     */
    public final int length() {
        return top+1;
    }
    
	/**
	 * Adds 'frame' on the variable stack for active block or function
	 * 
	 * @param blockScope	scope of active block
	 */
	public void enteredBlock(Scope blockScope) {
		variableStack.add(new Object[blockScope.size()]);
	}
	
	/**
	 * Removes 'frame' for active block or function
	 */
	public void exitedBlock() {
		variableStack.remove(variableStack.size()-1);
	}
	
	/**
	 * Sets value of variable specified by variableIndex in block specified by blockIndex
	 * 
	 * @param blockOffset
	 * @param variableOffset
	 * @param value
	 */
	public void setVariable(int blockOffset, int variableOffset, Object value) {
		// blockIndex < 0 indicates access to the (always accessible) global scope
		if (blockOffset < 0) {
			variableStack.get(0)[variableOffset] = value;
		} else {
			// otherwise jump back blockOffset blocks and set variable at variableOffset
			final int top = variableStack.size() - 1;
			variableStack.get(top-blockOffset)[variableOffset] = value;
		}
	}
	
	/**
	 * Retrieves value of a variable form given block
	 * @param blockOffset	 jump 'blockOffset' block back
	 * @param variableOffset	collect variable at slot 'variableOffset'
	 * @return value of given variable
	 */
	public Object getVariable(int blockOffset, int variableOffset) {
		if (blockOffset < 0) {
			return variableStack.get(0)[variableOffset];
		} else {
			// otherwise jump back blockOffset blocks and set variable at variableOffset
			final int top = variableStack.size() - 1;
			return variableStack.get(top-blockOffset)[variableOffset];
		}
	}
	
	/**
	 * @return stack contents that should not be modified
	 */
	public Object[] getStackContents() {
		Object[] ret = new Object[top+1];
		System.arraycopy(stack, 0, ret, 0, ret.length);
		return ret;
	}

	public Object[] getLocalVariables() {
		return variableStack.get(variableStack.size()-1);
	}
	
	public Object[] getGlobalVariables() {
		if (variableStack.isEmpty()) {
			throw new IllegalArgumentException("Global scope not found. Preserve the global scope with keepGlobalScope() method");
		}
		return variableStack.get(0);
	}
	
	@Override
	public String toString() {
		return "(" + 
			Arrays.toString(this.stack) + 
			"," +
			variableStack.toString() +
			")";
	}
	
}
