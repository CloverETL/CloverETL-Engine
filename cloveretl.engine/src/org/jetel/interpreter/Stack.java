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

/**
 * @author dpavlis
 * @since  9.8.2004
 *
 * This is class for holding Interpreter's stack and symbol table.
 */
public class Stack {
	
	Object[] stack;
	java.util.Map symtab;
	int top;
	// these constants are used by interpreter when true or false
	// result has to be indicated
	public static final Boolean TRUE_VAL=new Boolean(true);
	public static final Boolean FALSE_VAL=new Boolean(false);
	
	public Stack(){
		stack= new Object[16];
		symtab= new java.util.HashMap();
		top= -1;
	}
	 
	public Stack(int depth){
		stack= new Object[depth];
		symtab= new java.util.HashMap();
		top= -1;
	}

	public final void clear(){
		top=-1;
	}
	
	public final void push(Object obj){
		stack[++top]=obj;
	}
	
	public final Object pop(){
		return stack[top--];
	}
	
	public final void set(Object obj){
		stack[top]=obj;
	}
	
	public final Object get(){
		return stack[top];
	}
	
	public final void put(Object key,Object value){
		symtab.put(key,value);
	}
	
	public final Object get(Object key){
		return symtab.get(key);
	}
}
