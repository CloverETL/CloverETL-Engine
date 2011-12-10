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
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.TLType;

/**
 * Synthetic node representing a type cast.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class CastNode extends SimpleNode  {

	/**
	 * Type to cast from
	 */
	private final TLType fromType;
	
	private CastNode(CastNode c) {
		super(c);
		fromType = c.fromType;
	}
	
	/**
	 * Allocates new type-cast node.
	 * Source type is stored within {@link #fromType} field.
	 * Target type is stored accessible via {@link #getType()}
	 * 
	 * @param id	unique node id
	 * @param fromType	type to cast from
	 */
	public CastNode(int id, TLType fromType, TLType toType) {
		super(id);
		this.fromType = fromType;
		setType(toType);
	}
	
	public TLType getFromType() {
		return fromType;
	}

	public TLType getToType() {
		return getType();
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CastNode(this);
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
	public String toString() {
		return "CastNode from '" + fromType.name() + "' to '" + getType().name() + "'";
	}
	
}
