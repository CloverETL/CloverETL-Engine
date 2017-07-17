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
import org.jetel.ctl.TransformLangParserConstants;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.extensions.TLFunctionCallContext;



public class CLVFAssignment extends SimpleNode {
	
	//this function contexted is exploited only for 'record1.* = record2.*' assignment expression type
	//with different metadata - then the records are copied based on field names
	//integral function copyByName is used for this copying 
	private TLFunctionCallContext copyByNameCallContext;

	private int operator;
	
	public CLVFAssignment(int id) {
		super(id);
	}

	public CLVFAssignment(ExpParser p, int id) {
		super(p, id);
	}

	public CLVFAssignment(CLVFAssignment node) {
		super(node);
		this.copyByNameCallContext = node.getCopyByNameCallContext();
		this.operator = node.operator;
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

	@Override
	public SimpleNode duplicate() {
		return new CLVFAssignment(this);
	}

	public void setCopyByNameCallContext(TLFunctionCallContext context) {
		copyByNameCallContext = context;
	}

	public TLFunctionCallContext getCopyByNameCallContext() {
		return copyByNameCallContext;
	}

	public void setOperator(int operator) {
		this.operator = operator;
	}
	
	public int getOperator() {
		return operator;
	}
	
	public static boolean isCompound(int operator) {
		return (operator != TransformLangParserConstants.ASSIGN) && (operator != TransformLangParserConstants.MAPPING);
	}

}
