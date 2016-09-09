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

public class CLVFStartExpression extends SimpleNode implements ScopeHolder {

	private Scope scope;
	private boolean compiledMode = false;
	
	public CLVFStartExpression(int id) {
		super(id);
	}

	public CLVFStartExpression(ExpParser p, int id) {
		super(p, id);
	}

	public CLVFStartExpression(CLVFStartExpression node) {
		super(node);
		this.scope = node.scope;
	}

	public void setCompiled(boolean compiled) {
		this.compiledMode = compiled;
	}
	
	public boolean getCompiled() {
		return this.compiledMode;
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
	
	@Override
	public void setScope(Scope scope) {
		this.scope = scope;
	}
	
	@Override
	public Scope getScope() {
		return scope;
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFStartExpression(this);
	}
}
