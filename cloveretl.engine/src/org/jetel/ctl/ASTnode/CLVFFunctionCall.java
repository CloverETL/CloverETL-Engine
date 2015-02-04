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
package org.jetel.ctl.ASTnode;

import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParser;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.extensions.TLFunctionCallContext;
import org.jetel.ctl.extensions.TLFunctionDescriptor;
import org.jetel.ctl.extensions.TLFunctionPrototype;

public class CLVFFunctionCall extends SimpleNode {

	private String name;
	private CLVFFunctionDeclaration localFunc;
	private TLFunctionDescriptor externFunc;
	private TLFunctionCallContext functionCallContext;
	private TLFunctionPrototype executable;
	
	public CLVFFunctionCall(int id) {
		super(id);
	}

	public CLVFFunctionCall(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFFunctionCall(CLVFFunctionCall node) {
		super(node);
		this.name = node.name;
		this.localFunc = node.localFunc;
		this.externFunc = node.externFunc;
		this.functionCallContext = node.functionCallContext;
		this.executable = node.executable;
	}

	/** Accept the visitor. This method implementation is identical in all SimpleNode descendants. */
	@Override
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		try {
			return visitor.visit(this, data);
		} catch (TransformLangExecutorRuntimeException e) {
			if (e.getNode() == null) {
				e.setNode(this);
			}
			throw e;
		} catch (RuntimeException e) {
			throw new TransformLangExecutorRuntimeException(this, null, e);
		}
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		// this piece of code makes prefixing of functions in the AST post processing step possible
		return (localFunc != null) ? localFunc.getName() : name;
	}

	public void setCallTarget(CLVFFunctionDeclaration localFunc) {
		this.localFunc = localFunc;
	}
	
	public void setCallTarget(TLFunctionDescriptor externFunc) {
		this.externFunc = externFunc;
	}
	
	public boolean isExternal() {
		return externFunc !=null;
	}
	
	public CLVFFunctionDeclaration getLocalFunction() {
		return localFunc;
	}
	
	public TLFunctionDescriptor getExternalFunction() {
		return externFunc;
	}

	@Override
	public String toString() {
		return super.toString() + " name '" + name + "'";
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFFunctionCall(this);
	}

	public void setFunctionCallContext(TLFunctionCallContext callContext) {
		this.functionCallContext = callContext;
	}
	
	public TLFunctionCallContext getFunctionCallContext() {
		return functionCallContext;
	}

	public void setExecutable(TLFunctionPrototype executable) {
		this.executable = executable;
	}

	public TLFunctionPrototype getExtecutable() {
		return executable;
	}
	
	
}
