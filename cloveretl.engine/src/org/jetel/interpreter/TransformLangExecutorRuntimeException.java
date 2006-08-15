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

import org.jetel.interpreter.node.SimpleNode;

import com.sun.tools.javac.tree.Tree.Throw;

/**
 * @author dpavlis
 * @since  9.9.2004
 *
 * Exception thrown by Interpreter at runtime when error preventing
 * other run occures.
 */
public class TransformLangExecutorRuntimeException extends RuntimeException {
	SimpleNode nodeInError;
	Object[] arguments;
	
	public TransformLangExecutorRuntimeException(SimpleNode node,Object[] arguments,String message){
		super(message);
		this.nodeInError=node;
		this.arguments=arguments;
	}
	
	public TransformLangExecutorRuntimeException(Object[] arguments,String message){
		super(message);
		this.nodeInError=null;
		this.arguments=arguments;
	}
	
	public TransformLangExecutorRuntimeException(SimpleNode node,String message){
		super(message);
		this.nodeInError=node;
		this.arguments=null;
	}
	
	public TransformLangExecutorRuntimeException(String message){
		super(message);
		this.nodeInError=null;
		this.arguments=null;
	}
	
    public TransformLangExecutorRuntimeException(String message, Throwable cause){
        super(message,cause);
        this.nodeInError=null;
        this.arguments=null;
    }
	
	public SimpleNode getNode(){
		return nodeInError;
	}
	
	public Object[] getArguments(){
		return arguments;
	}
	
	public String getMessage(){
		StringBuffer strBuf=new StringBuffer("Interpreter runtime exception");
        if (nodeInError!=null){
            strBuf.append(" on line ").append(nodeInError.getLineNumber());
            strBuf.append(" column ").append(nodeInError.getColumnNumber());
        }
        strBuf.append(" : ");
		strBuf.append(super.getMessage()).append("\n");
		if (arguments !=null){
			for(int i=0;i<arguments.length;i++){
				strBuf.append("arg[").append(i).append("] ");
				strBuf.append(arguments[i] != null ? arguments[i].getClass().getName() : "null").append(" \"");
				strBuf.append(arguments[i] != null ? arguments[i] : "!! NULL !!").append("\"");
				strBuf.append("\n");
			}
		}
		return strBuf.toString();
	}
}
