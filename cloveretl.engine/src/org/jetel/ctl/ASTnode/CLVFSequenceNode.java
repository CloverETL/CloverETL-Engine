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

import org.jetel.ctl.SyntacticPosition;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParser;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.data.sequence.Sequence;

public class CLVFSequenceNode extends SimpleNode {

	public static final int OP_NEXT = 0;
	public static final int OP_CURRENT = 1;
	public static final int OP_RESET = 2;

	public String sequenceName;
	public int opType = 0; // default
	private SyntacticPosition nameBegin;
	
	private Sequence sequence;
	

	public CLVFSequenceNode(int id) {
		super(id);
	}

	public CLVFSequenceNode(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFSequenceNode(CLVFSequenceNode node) {
		super(node);
		this.sequenceName = node.sequenceName;
		this.opType = node.opType;
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

	public void setSequenceName(String name, int beginLine, int beginColumn) {
		this.sequenceName = name;
		this.nameBegin = new SyntacticPosition(beginLine, beginColumn);
	}

	public void setOperation(int op) {
		this.opType = op;
	}

	public String getSequenceName() {
		return sequenceName;
	}
	
	public SyntacticPosition getNameBegin() {
		return nameBegin;
	}
	
	public Sequence getSequence() {
		return sequence;
	}
	
	public void setSequence(Sequence sequence) {
		this.sequence = sequence;
	}

	@Override
	public String toString() {
		return super.toString() + " sequence " + "\"" + sequenceName + "\"";
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFSequenceNode(this);
	}

	@Override
	public boolean isBreakable() {
		return true;
	}
}