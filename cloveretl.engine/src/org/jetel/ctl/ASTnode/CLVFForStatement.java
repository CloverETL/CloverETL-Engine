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

/**
 * AST node representing the for-loop statement.
 * Since all three expressions in the for-header are optional,
 * to access its children nodes correctly use {@link #getForInit()}, 
 * {@link #getForFinal()} and {@link #getForUpdate()} methods instead
 * of using the standard access via {@link SimpleNode#jjtGetChild(int)}
 * 
 * 
 * @author mtomcanyi
 *
 */
public class CLVFForStatement extends SimpleNode implements ScopeHolder {

	private Scope scope;
	
	/**
	 * For init, update and final expressions are all optional,
	 * so we need to keep their positions in the children[] array
	 */
	int[] positions = new int[]{-1,-1,-1}; 
	private static final int INIT = 0;
	private static final int FINAL = 1;
	private static final int UPDATE = 2;
	
	
	public CLVFForStatement(int id) {
		super(id);
	}

	public CLVFForStatement(ExpParser p, int id) {
		super(p, id);
	}

	public CLVFForStatement(CLVFForStatement node) {
		super(node);
		this.scope = node.scope;
		if (node.positions != null) {
			this.positions = new int[]{ node.positions[0], node.positions[1], node.positions[2] };
		}
		
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

	public void setInitPosition(int position) {
		positions[INIT] = position;
	}
	
	public void setFinalPosition(int position) {
		positions[FINAL] = position;
	}
	
	public void setUpdatePosition(int position) {
		positions[UPDATE] = position;
	}
	
	/**
	 * @return for-loop initialization expression or <code>null</code> if not specified
	 */
	public SimpleNode getForInit() {
		return getExpression(INIT);
	}
	
	/**
	 * @return for-loop update expression or <code>null</code> if not specified
	 */
	public SimpleNode getForUpdate() {
		return getExpression(UPDATE);
	}
	
	/**
	 * @return for-loop final test expression or <code>null</code> if not specified
	 */
	public SimpleNode getForFinal() {
		return getExpression(FINAL);
	}
	
	public SimpleNode getForBody() {
		int bodyIdx = 
			positions[UPDATE] >= 0 ? positions[UPDATE] + 1 :
			positions[FINAL] >= 0 ? positions[FINAL] + 1 :
			positions[INIT] >= 0 ? positions[INIT] + 1 : 0;
			
		return (SimpleNode)(bodyIdx < jjtGetNumChildren() ? jjtGetChild(bodyIdx) : null);
	}
	
	private SimpleNode getExpression(int i) {
		return (SimpleNode)(positions[i] >= 0 ? jjtGetChild(positions[i]) : null);
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
		return new CLVFForStatement(this);
	}

	
}
