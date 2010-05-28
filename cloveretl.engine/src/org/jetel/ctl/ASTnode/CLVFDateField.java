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

import static org.jetel.ctl.TransformLangParserConstants.tokenImage;

import org.jetel.ctl.TransformLangParser;
import org.jetel.ctl.TransformLangParserVisitor;

public class CLVFDateField extends SimpleNode {

	int type;

	public CLVFDateField(int id) {
		super(id);
	}

	public CLVFDateField(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFDateField(CLVFDateField node) {
		super(node);
		this.type = node.type;
	}

	/** Accept the visitor. * */
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}

	public void setFieldType(int fieldType) {
		this.type = fieldType;
	}

	@Override
	public String toString() {
		return super.toString() + "\" type " + tokenImage[type];
	}

	@Override
	public SimpleNode duplicate() {
		return new CLVFDateField(this);
	}
}
