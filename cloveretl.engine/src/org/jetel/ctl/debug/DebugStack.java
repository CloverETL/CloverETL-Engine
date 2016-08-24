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
import org.jetel.exception.JetelRuntimeException;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Aug 20, 2014
 */
public class DebugStack extends Stack {
	
	private List<FunctionCallFrame> callStack = new ArrayList<>();
	private long varIdSeq;
	private long callIdSeq;
	
	public static class FunctionCallFrame {
		
		private final CLVFFunctionCall functionCall;
		private final int varStackIndex; // index where local scopes of the call start
		private final long id;
		
		public FunctionCallFrame(CLVFFunctionCall functionCall, int varStackIndex, long id) {
			super();
			this.functionCall = functionCall;
			this.varStackIndex = varStackIndex;
			this.id = id;
		}
		
		public FunctionCallFrame(CLVFFunctionCall functionCall, long id) {
			this(functionCall, -1, id);
		}
		
		public CLVFFunctionCall getFunctionCall() {
			return functionCall;
		}
		
		public int getVarStackIndex() {
			return varStackIndex;
		}
		
		public long getId() {
			return id;
		}
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
	
	public FunctionCallFrame getFunctionCall(int callStackIndex) {
		if (0 <= callStackIndex && callStackIndex < callStack.size()) {
			return callStack.get(callStackIndex);
		}
		return null;
	}
	
	@Override
	public void setVariable(int blockOffset, int variableOffset, Object value, String name, TLType type) {
		Variable storedVar = (Variable)super.getVariable(blockOffset, variableOffset);
		long id = storedVar != null ? storedVar.getId() : nextVariableId();
		super.setVariable(blockOffset, variableOffset, new Variable(name,type, blockOffset < 0, value, id));
	}
	
	@Override
	public void setVariable(int blockOffset, int variableOffset, Object value){
		Variable storedVar = (Variable)super.getVariable(blockOffset,variableOffset);
		storedVar.setValue(value);
		super.setVariable(blockOffset, variableOffset, storedVar);
	}
	
	@Override
	public Object getVariable(int blockOffset, int variableOffset){
		Variable variable = (Variable)super.getVariable(blockOffset, variableOffset);
		return variable != null ? variable.getValue() : null;
	}
	
	public Object getVariableChecked(int blockOffset, int variableOffset) throws IllegalArgumentException {
		Variable variable = (Variable)super.getVariable(blockOffset, variableOffset);
		if (variable != null) {
			return variable.getValue();
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * @return all local variables from all frames
	 */
	public Object[] getAllLocalVariables() {
		List<Object> vars = new ArrayList<Object>();
		for (int i = 1; i < variableStack.size(); i++){
			for (Object var:variableStack.get(i)){
				if (var!=null) vars.add(var);
			}
		}
		return vars.toArray();
	}
	
	public Object[] getLocalVariables(int functionCallIndex) {
		FunctionCallFrame frame = callStack.get(functionCallIndex);
		FunctionCallFrame nextFrame = functionCallIndex < callStack.size() - 1 ? callStack.get(functionCallIndex + 1) : null;
		
		final int startIndex = frame.getVarStackIndex() < 0 /* global scope */ ? 1 : frame.getVarStackIndex();
		final int stopIndex = nextFrame != null ? nextFrame.getVarStackIndex() : variableStack.size();
		
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
		for (int i = 0; i < variableStack.size(); i++){
			for (Object var:variableStack.get(i)){
				if (((Variable)var).getName().equals(name)) return var;
			}
		}
		return null;
	}
	
	public void enteredSyntheticBlock(CLVFFunctionCall functionCallNode) {
		if (functionCallNode != null) {
			FunctionCallFrame frame = new FunctionCallFrame(functionCallNode, nextCallId());
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
			FunctionCallFrame frame = new FunctionCallFrame(functionCallNode, variableStack.size() - 1, nextCallId());
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
		if (functionCallNode != null) {
			callStack.remove(callStack.size() - 1);
		}
	}
	
	public ListIterator<FunctionCallFrame> getFunctionCallStack() {
		return callStack.listIterator(callStack.size());
	}
	
	public long nextVariableId() {
		return ++varIdSeq;
	}
	
	private long nextCallId() {
		return ++callIdSeq;
	}
	
	public DebugStack createShallowCopyUpToFrame(FunctionCallFrame callFrame) {
		
		if (!this.callStack.contains(callFrame)) {
			throw new JetelRuntimeException(String.format("Call stack does not contain given call frame."));
		}
		
		DebugStack debugStack = new DebugStack();
		debugStack.variableStack = new ArrayList<>(this.variableStack.subList(0, callFrame.varStackIndex));
	    debugStack.callStack = new ArrayList<>(this.callStack.subList(0, this.callStack.indexOf(callFrame)));
		return debugStack;
	}
}
