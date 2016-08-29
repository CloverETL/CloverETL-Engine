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

import org.jetel.ctl.ExpParser;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.Scope;

public class CLVFIfStatement extends SimpleNode {

	private Scope thenScope;
	private Scope elseScope;
	
	
	public CLVFIfStatement(int id) {
		super(id);
	}

	public CLVFIfStatement(ExpParser p, int id) {
		super(p, id);
	}

	public CLVFIfStatement(CLVFIfStatement node) {
		super(node);
		this.thenScope = node.thenScope;
		this.elseScope = node.elseScope;
	}

	@Override
	public boolean isBreakable(){
		return true;
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
	
	public void setThenScope(Scope thenScope) {
		this.thenScope = thenScope;
	}
	
	public Scope getThenScope() {
		return thenScope;
	}
	
	public void setElseScope(Scope elseScope) {
		this.elseScope = elseScope;
	}
	
	public Scope getElseScope() {
		return elseScope;
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFIfStatement(this);
	}
}
