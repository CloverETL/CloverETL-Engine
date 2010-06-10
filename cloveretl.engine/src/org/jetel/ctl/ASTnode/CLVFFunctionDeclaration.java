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
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.Scope;
import org.jetel.ctl.data.TLType;

public class CLVFFunctionDeclaration extends SimpleNode {

	public String name;
	private TLType[] formalParams;
	private Scope scope;
	
	
	public CLVFFunctionDeclaration(int id) {
		super(id);
	}

	public CLVFFunctionDeclaration(ExpParser p, int id) {
		super(p, id);
	}

	public CLVFFunctionDeclaration(CLVFFunctionDeclaration node) {
		super(node);
		this.name = node.name;
		if (node.formalParams != null) {
			this.formalParams = new TLType[node.formalParams.length];
			System.arraycopy(node.formalParams, 0, this.formalParams, 0, node.formalParams.length);
		}
	}

	/** Accept the visitor. * */
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}

	public void setName(String name) {
		this.name = name;
	}

	public String toString() {
		return super.toString() + " '" + name + "'";
	}

	public String getName() {
		return name;
	}

	public void setFormalParameters(TLType[] formalParams) {
		this.formalParams = formalParams;
	}
	
	
	public TLType[] getFormalParameters() {
		return formalParams;
	}
	
	public int getParamCount() {
		if (formalParams == null) {
			return 0;
		}
		
		return formalParams.length;
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFFunctionDeclaration(this);
	}

	public void setScope(Scope currentScope) {
		this.scope = currentScope;
	}
	
	public Scope getScope() {
		return this.scope;
	}
	
	public String toHeaderString() {
		final StringBuilder buf = new StringBuilder();
		buf.append(getType() == null ? "null" : getType().toString()).append(" ");
		buf.append(name == null ? "null" : name).append("(");
		if (formalParams == null) {
			buf.append("null");
		} else {
			for (TLType f : formalParams) {
				buf.append(f.name()).append(", ");
			}
			buf.delete(buf.length() - 2, buf.length());
		}
		buf.append(")");
		return buf.toString();
		
	}
	
}
