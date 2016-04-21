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

import org.jetel.ctl.Stack;
import org.jetel.ctl.ASTnode.CLVFFunctionCall;
import org.jetel.ctl.data.TLType;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Aug 20, 2014
 */
public class DebugStack extends Stack {
	
	public DebugStack(){
		super();
	
	}
	
	@Override
	public void setVariable(int blockOffset, int variableOffset,Object value, String name, TLType type){
		super.setVariable(blockOffset, variableOffset, new Variable(name,type, blockOffset<0, value));
	}
	
	@Override
	public void setVariable(int blockOffset, int variableOffset,Object value){
		Variable storedVar= (Variable)super.getVariable(blockOffset,variableOffset);
		storedVar.setValue(value);
		super.setVariable(blockOffset, variableOffset,storedVar);
		
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
		ArrayList<Object> vars = new ArrayList<Object>();
		for (int i=1;i< variableStack.size();i++){
			for(Object var:variableStack.get(i)){
				if (var!=null) vars.add(var);
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
			previousFunctionCallNode = currentFunctionCallNode;
			currentFunctionCallNode = functionCallNode;
			functionCalls.add(functionCallNode);
		}
	}
	
	public void exitedSyntheticBlock(CLVFFunctionCall functionCallNode) {
		if (functionCallNode != null) {
			functionCalls.remove(functionCalls.size()-1);
			currentFunctionCallNode = previousFunctionCallNode;
			final int size=functionCalls.size();
			previousFunctionCallNode =  size>1 ?  functionCalls.get(size-2) : null;
		}
	}
}
