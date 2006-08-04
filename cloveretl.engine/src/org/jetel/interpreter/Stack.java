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

import java.util.Arrays;

import org.jetel.data.Numeric;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverDouble;

/**
 * @author dpavlis
 * @since  9.8.2004
 *
 * This is class for holding Interpreter's stack and symbol table.
 */
public class Stack {
	
    public static final int STACK_DEPTH = 32;
    public static final int DEFAULT_VAR_SLOT_LENGTH = 64;
    public static final float GROW_FACTOR = 1.6f;
	public static final int MAX_ALLOWED_DEPTH = 1024;
    
	// these constants are used by interpreter when true or false
	// result has to be indicated
	public static final Boolean TRUE_VAL= Boolean.TRUE;
	public static final Boolean FALSE_VAL= Boolean.FALSE;
    
	public static final Numeric NUM_ZERO = new CloverInteger(0);
    public static final Numeric NUM_ONE = new CloverInteger(1); 
    public static final Numeric NUM_MINUS_ONE = new CloverInteger(-1); 
	public static final Numeric NUM_PI = new CloverDouble(Math.PI);
    public static final Numeric NUM_E = new CloverDouble(Math.E);
    
    
    Object[] stack;
    Object[] globalVarSlot;
    Object[] localVarSlot;
    int [] functionCallStack;
    int top;
    int funcStackTop;
    int localVarSlotOffset;
    int localVarCounter;
	
	public Stack(){
        this(STACK_DEPTH,DEFAULT_VAR_SLOT_LENGTH );
	}
	 
	public Stack(int depth,int varSlotLength){
		stack= new Object[depth];
        globalVarSlot= new Object[varSlotLength];
        localVarSlot= new Object[varSlotLength];
        functionCallStack = new int[STACK_DEPTH];
		top= -1;
        funcStackTop= -1;
        localVarSlotOffset =localVarCounter = 0;
	}

	/**
	 * clear stack (remove all object references) 
	 */
	public final void clear(){
		Arrays.fill(stack,null);
		top=-1;
	}
    
	public final void push(Object obj){
		if (top>=stack.length){
			throw new TransformLangExecutorRuntimeException("Internal error: exceeded Interpreter stack depth !!!");
		}
		stack[++top]=obj;
	}
	
    
	/**
	 * @return top object placed on stack (last put)
	 */
	public final Object pop(){
        if (top<0) return null;
		Object obj=stack[top];
		stack[top--]=null;
		return obj;
	}
	
	public final void set(Object obj){
		stack[top]=obj;
	}
	
	public final Object get(){
		return stack[top];
	}
    
    public final void storeVar(boolean local, int slot,Object value){
        if (local){
            storeLocalVar(slot,value);
        }else{
            storeGlobalVar(slot,value);
        }
    }
    
    public final Object getVar(boolean local,int slot){
        return local ? getLocalVar(slot) : getGlobalVar(slot);
    }
    
	public final void storeGlobalVar(int slot,Object value){
       try{
        globalVarSlot[slot]=value;
       }catch(IndexOutOfBoundsException ex){
           if (globalVarSlot.length>=MAX_ALLOWED_DEPTH){
               throw new TransformLangExecutorRuntimeException("Internal error: exceeded max length of global variable storage");
           }
           Object[] temp=new Object[(int)(GROW_FACTOR*globalVarSlot.length)];
           System.arraycopy(globalVarSlot,0,temp,0,globalVarSlot.length);
           globalVarSlot=temp;
           globalVarSlot[slot]=value;
       }
	}
	
	public final Object getGlobalVar(int slot){
		return globalVarSlot[slot];
	}
    
	public final void storeLocalVar(int slot,Object value){
	    int slotNum=slot+localVarSlotOffset;
	    try{
	        localVarSlot[slotNum]=value;
	    }catch(IndexOutOfBoundsException ex){
            if (localVarSlot.length>=MAX_ALLOWED_DEPTH){
                   throw new TransformLangExecutorRuntimeException("Internal error: exceeded max length of local variable storage");
               }
	        Object[] temp=new Object[(int)(GROW_FACTOR*localVarSlot.length)];
	        System.arraycopy(localVarSlot,0,temp,0,localVarSlot.length);
	        localVarSlot=temp;
	        localVarSlot[slotNum]=value;
	    }
        localVarCounter++;
	}
	
	public final Object getLocalVar(int slot){
	    return localVarSlot[slot+localVarSlotOffset];
	}
	
	public final void pushFuncCallFrame(){
        try{
            functionCallStack[++funcStackTop]=localVarSlotOffset;
        }catch(ArrayIndexOutOfBoundsException ex){
            if (functionCallStack.length>=MAX_ALLOWED_DEPTH){
                throw new TransformLangExecutorRuntimeException("Internal error: exceeded max length of function call frame storage");
            }
            int[] temp=new int[(int)(GROW_FACTOR*functionCallStack.length)];
            System.arraycopy(functionCallStack,0,temp,0,functionCallStack.length);
            functionCallStack=temp;
            functionCallStack[++funcStackTop]=localVarSlotOffset;
        }
        localVarSlotOffset+=localVarCounter;
        localVarCounter=0;
      /* DEBUG
        for(int i=0;i<functionCallStack.length;System.out.print(functionCallStack[i++]));
        System.out.println("var counter:"+localVarCounter);
        System.out.println("var offset:"+localVarSlotOffset);
        */
	}
	
	public final void popFuncCallFrame(){
	    // clean local variables objects
        Arrays.fill(localVarSlot,localVarSlotOffset,localVarSlotOffset+localVarCounter,null);
        localVarSlotOffset=functionCallStack[funcStackTop--];
        // DEBUG System.out.println("var offset:"+localVarSlotOffset);
        localVarCounter=0;
	}
}
