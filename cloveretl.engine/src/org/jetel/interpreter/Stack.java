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
import java.util.Calendar;

import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.Numeric;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLVariable;

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
    
    public static final Numeric NUM_ONE_P = new CloverInteger(1);
    public static final Numeric NUM_MINUS_ONE_P = new CloverInteger(-1);
    
    // useful instance variables (used when evaluating certain expressions) 
    public Calendar calendar; 
    
    TLValue[] stack;
    TLVariable[] globalVarSlot;
    TLVariable[] localVarSlot;
    int [] functionCallStack;
    int top;
    int funcStackTop;
    int localVarSlotOffset;
    int localVarCounter;
    int stackSave[];
	
	public Stack(){
        this(STACK_DEPTH,DEFAULT_VAR_SLOT_LENGTH );
	}
	 
	public Stack(int depth,int varSlotLength){
		stack= new TLValue[depth];
        globalVarSlot= new TLVariable[varSlotLength];
        localVarSlot= new TLVariable[varSlotLength];
        functionCallStack = new int[STACK_DEPTH];
		top= -1;
        funcStackTop= -1;
        localVarSlotOffset =localVarCounter = 0;
        calendar = Calendar.getInstance();
        stackSave=new int[4];
	}

	
	public void saveStack(){
		stackSave[0]=top;
		stackSave[1]=funcStackTop;
		stackSave[2]=localVarSlotOffset;
		stackSave[3]=localVarCounter;
		
	}
	
	public void restoreStack(){
		top=stackSave[0];
		funcStackTop=stackSave[1];
		localVarSlotOffset=stackSave[2];
		localVarCounter=stackSave[3];
	}
	
	/**
	 * clear stack (remove all object references) 
	 */
	public final void clear(){
		Arrays.fill(stack,null);
		top=-1;
	}
    
	public final void push(TLValue obj){
		if (top>=stack.length){
			throw new TransformLangExecutorRuntimeException("Internal error: exceeded Interpreter stack depth !!!");
		}
		stack[++top]=obj;
	}
	
    
	/**
	 * @return top object placed on stack (last put)
	 */
	public final TLValue pop(){
        if (top<0) return null;
        TLValue obj=stack[top];
		stack[top--]=null;
		return obj;
	}
    
    public final TLValue[] pop(int length) {
        TLValue[] val=new TLValue[length];
       return pop(val,length);
    }
    
    public final TLValue[] pop(TLValue[] val, int length) {
        for(int i=length;i>0;val[--i]=pop());
        return val;
    }
	
	public final void set(TLValue obj){
		stack[top]=obj;
	}
	
	public final TLValue get(){
		if (top<0) return null;
		return stack[top];
	}
    
	public final TLValue get(int depth){
		if (top<0) return null;
		return stack[top-depth];
	}
	
    /**
     * @return length/depth of stack - how many parameters it contains
     * @since 20.3.2007
     */
    public final int length() {
        return top+1;
    }
    
    public final void storeVar(boolean local, int slot,TLVariable variable){
        if (local){
            storeLocalVar(slot,variable);
        }else{
            storeGlobalVar(slot,variable);
        }
    }
    
    public final TLVariable getVar(boolean local,int slot){
        return local ? getLocalVar(slot) : getGlobalVar(slot);
    }
    
	public final void storeGlobalVar(int slot,TLVariable value){
       try{
        globalVarSlot[slot]=value;
       }catch(IndexOutOfBoundsException ex){
           if (globalVarSlot.length>=MAX_ALLOWED_DEPTH){
               throw new TransformLangExecutorRuntimeException("Internal error: exceeded max length of global variable storage");
           }
           TLVariable[] temp=new TLVariable[(int)(GROW_FACTOR*globalVarSlot.length)];
           System.arraycopy(globalVarSlot,0,temp,0,globalVarSlot.length);
           globalVarSlot=temp;
           globalVarSlot[slot]=value;
       }
	}
	
	public final TLVariable getGlobalVar(int slot){
		return globalVarSlot[slot];
	}
    
	public final void storeLocalVar(int slot,TLVariable variable){
	    int slotNum=slot+localVarSlotOffset;
	    try{
	        localVarSlot[slotNum]=variable;
	    }catch(IndexOutOfBoundsException ex){
            if (localVarSlot.length>=MAX_ALLOWED_DEPTH){
                   throw new TransformLangExecutorRuntimeException("Internal error: exceeded max length of local variable storage");
               }
            TLVariable[] temp=new TLVariable[(int)(GROW_FACTOR*localVarSlot.length)];
	        System.arraycopy(localVarSlot,0,temp,0,localVarSlot.length);
	        localVarSlot=temp;
	        localVarSlot[slotNum]=variable;
	    }
        localVarCounter++;
	}
	
	public final TLVariable getLocalVar(int slot){
	    return localVarSlot[slot+localVarSlotOffset];
	}
	
	public final void pushFuncCallFrame(){
        try{
            functionCallStack[++funcStackTop]=localVarSlotOffset;
            functionCallStack[++funcStackTop]=localVarCounter;
        }catch(ArrayIndexOutOfBoundsException ex){
            if (functionCallStack.length>=MAX_ALLOWED_DEPTH){
                throw new TransformLangExecutorRuntimeException("Internal error: exceeded max length of function call frame storage");
            }
            int[] temp=new int[(int)(GROW_FACTOR*functionCallStack.length)];
            System.arraycopy(functionCallStack,0,temp,0,functionCallStack.length);
            functionCallStack=temp;
            functionCallStack[++funcStackTop]=localVarSlotOffset;
            functionCallStack[++funcStackTop]=localVarCounter;
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
        localVarCounter=functionCallStack[funcStackTop--];
        localVarSlotOffset=functionCallStack[funcStackTop--];
        // DEBUG System.out.println("var offset:"+localVarSlotOffset);    
	}
}
