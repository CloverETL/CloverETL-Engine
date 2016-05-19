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
package org.jetel.ctl.debug;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.jetel.ctl.Stack;
import org.jetel.ctl.ASTnode.CLVFFunctionCall;
import org.jetel.ctl.data.Scope;
import org.jetel.ctl.data.TLType;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Aug 20, 2014
 */
public class DebugStack extends Stack {
	
	private List<FunctionCallFrame> callStack;
	
	public static class FunctionCallFrame {
		public CLVFFunctionCall functionCall;
		public int varStackIndex = -1;
	}
	
	public DebugStack() {
		super();
		this.callStack = new ArrayList<>();
	}
	
	public int getCurrentFunctionCallIndex() {
		return callStack.size() - 1;
	}
	
	public int getPreviousFunctionCallIndex() {
		if (callStack.size() >= 2) {
			return callStack.size() - 2;
		}
		return -1;
	}
	
	@Override
	public void setVariable(int blockOffset, int variableOffset,Object value, String name, TLType type){
		super.setVariable(blockOffset, variableOffset, new Variable(name,type, blockOffset < 0, value));
	}
	
	@Override
	public void setVariable(int blockOffset, int variableOffset,Object value){
		Variable storedVar = (Variable)super.getVariable(blockOffset,variableOffset);
		storedVar.setValue(value);
		super.setVariable(blockOffset, variableOffset, storedVar);
	}
	
	@Override
	public Object getVariable(int blockOffset, int variableOffset){
		Variable variable = (Variable) super.getVariable(blockOffset, variableOffset);
		return variable != null ? variable.getValue() : null;
	}
	
	/**
	 * @return all local variables from all frames
	 */
	public Object[] getAllLocalVariables() {
		List<Object> vars = new ArrayList<Object>();
		for (int i=1;i< variableStack.size();i++){
			for(Object var:variableStack.get(i)){
				if (var!=null) vars.add(var);
			}
		}
		return vars.toArray();
	}
	
	public Object[] getLocalVariables(int functionCallIndex) {
		FunctionCallFrame frame = callStack.get(functionCallIndex);
		FunctionCallFrame nextFrame = functionCallIndex < callStack.size() - 1 ? callStack.get(functionCallIndex + 1) : null;
		
		final int startIndex = frame.varStackIndex < 0 /* global scope */ ? 1 : frame.varStackIndex;
		final int stopIndex = nextFrame != null ? nextFrame.varStackIndex : variableStack.size();
		
		List<Object> vars = new ArrayList<>();
		for (int i = startIndex; i < stopIndex; ++i) {
			for (Object var : variableStack.get(i)) {
				if (var != null) {
					vars.add(var);
				}
			}
		}
		return vars.toArray();
	}
	
	public Object getVariable(String name){
		for (int i=0;i< variableStack.size();i++){
			for(Object var:variableStack.get(i)){
				if (((Variable)var).getName().equals(name)) return var;
			}
		}
		return null;
	}
	
	public void enteredSyntheticBlock(CLVFFunctionCall functionCallNode) {
		if (functionCallNode != null) {
			FunctionCallFrame frame = new FunctionCallFrame();
			frame.functionCall = functionCallNode;
			callStack.add(frame);
		}
	}
	
	public void exitedSyntheticBlock(CLVFFunctionCall functionCallNode) {
		if (functionCallNode != null) {
			callStack.remove(callStack.size() - 1);
		}
	}
	
	/**
	 * Adds 'frame' on the variable stack for active block or function
	 * 
	 * @param blockScope	scope of active block
	 * @param functionCallNode	the CTL AST Node of the function (call)
	 */
	@Override
	public void enteredBlock(Scope blockScope, CLVFFunctionCall functionCallNode) {
		super.enteredBlock(blockScope, functionCallNode);
		if (functionCallNode != null) {
			FunctionCallFrame frame = new FunctionCallFrame();
			frame.functionCall = functionCallNode;
			frame.varStackIndex = variableStack.size() - 1;
			callStack.add(frame);
		}
	}
	
	/**
	 * Removes 'frame' for active block or function
	 * @param functionCallNode	the CTL AST Node of the function (call)
	 */
	@Override
	public void exitedBlock(CLVFFunctionCall functionCallNode) {
		super.exitedBlock(functionCallNode);
		if (functionCallNode !=null) {
			callStack.remove(callStack.size() - 1);
		}
	}
	
	public ListIterator<FunctionCallFrame> getFunctionCallStack() {
		return callStack.listIterator(callStack.size());
	}
}
