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

public class CLVFMemberAccessExpression extends SimpleNode {
	
	private String name;
	private int fieldId;
	private boolean isWildcard = false;
	
	public CLVFMemberAccessExpression(int id) {
		super(id);
	}

	public CLVFMemberAccessExpression(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFMemberAccessExpression(CLVFMemberAccessExpression node) {
		super(node);
		this.name = node.name;
		this.fieldId = node.fieldId;
		this.isWildcard = node.isWildcard;
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
		return name;
	}

	public void setFieldId(int fieldId) {
		this.fieldId = fieldId;
	}
	
	public int getFieldId() {
		return fieldId;
	}
	
	
	@Override
	public String toString() {
		return super.toString() + " field \'" + name + "'";
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFMemberAccessExpression(this);
	}

	public void setWildcard(boolean isWildcard) {
		this.isWildcard = isWildcard;
	}
	
	public boolean isWildcard() {
		return this.isWildcard;
	}
	
	@Override
	public boolean isBreakable() {
		return true;
	}
}
