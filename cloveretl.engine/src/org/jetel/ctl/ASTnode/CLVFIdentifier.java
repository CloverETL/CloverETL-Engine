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

public class CLVFIdentifier extends SimpleNode {

	private String name;
	private CLVFVariableDeclaration variable;
	private int blockOffset;
	private int variableOffset;
	
	public CLVFIdentifier(int id) {
		super(id);
	}

	public CLVFIdentifier(TransformLangParser p, int id) {
		super(p, id);
	}

	
	public CLVFIdentifier(CLVFIdentifier node) {
		super(node);
		this.name = node.name;
		this.variable = node.variable;
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
		// this piece of code makes prefixing of variables in the AST post processing step possible
		return (variable != null) ? variable.getName() : name;
	}
	
	public void setBlockOffset(int blockOffset) {
		this.blockOffset = blockOffset;
	}
	
	public int getBlockOffset() {
		return blockOffset;
	}
	
	public void setVariableOffset(int variableOffset) {
		this.variableOffset = variableOffset;
	}
	
	public int getVariableOffset() {
		return variableOffset;
	}
	
	@Override
	public String toString() {
		return super.toString() + " " + "\"" + name  + "\"";
	}

	public void setVariable(CLVFVariableDeclaration variable) {
		this.variable = variable;
	}
	
	public CLVFVariableDeclaration getVariable() {
		return variable;
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFIdentifier(this);
	}
}
