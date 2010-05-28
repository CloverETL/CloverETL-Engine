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
import org.jetel.ctl.TransformLangParserConstants;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.TLType;

public class CLVFComparison extends SimpleNode {

	private int operator;
	/** Type in which to perform the comparison */
	private TLType operationType;

	public CLVFComparison(int id) {
		super(id);
	}

	public CLVFComparison(ExpParser p, int id) {
		super(p, id);
	}
	

	public CLVFComparison(CLVFComparison node) {
		super(node);
		this.operator = node.operator;
	}

	/** Accept the visitor. * */
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}

	public String toString() {
		return super.toString() + " - " + TransformLangParserConstants.tokenImage[operator];
	}

	public void setOperator(int operator) {
		this.operator = operator;
	}

	public int getOperator() {
		return operator;
	}
	
	public void setOperationType(TLType operationType) {
		this.operationType = operationType;
	}
	
	public TLType getOperationType() {
		return operationType;
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFComparison(this);
	}
}