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
package org.jetel.interpreter.ASTnode;

import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.TransformLangParserVisitor;

public class CLVFDictionaryNode extends SimpleNode {

	public static final int OP_READ = 0;
	public static final int OP_WRITE = 1;
	public static final int OP_DELETE = 2;
	
	public int operation;
	
	
	
	public CLVFDictionaryNode(int id) {
		super(id);
	}

	public CLVFDictionaryNode(TransformLangParser p, int id) {
		super(p, id);
	}

	/** Accept the visitor. * */
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}

	public void setOperation(int operation) {
		this.operation = operation;
	}

}
