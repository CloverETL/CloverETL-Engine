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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParser;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.TLType;
import org.jetel.data.DataRecord;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;

public class CLVFLookupNode extends SimpleNode {

	public static final int OP_GET = 0;
	public static final int OP_NEXT = 1;
	public static final int OP_COUNT = 2;
	public static final int OP_PUT = 3;

	private String lookupName;
	private Integer opType = null; // default is get
	private LookupTable lookupTable;
	private /* final */ int lookupIndex;
	private /* final */ DataRecord lookupRecord;
	private /* final */ TLType[] paramTypes;
	private /* final */ List<Integer> decimalPrecisions;
	private /* final */ Lookup lookup;
	
	public CLVFLookupNode(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFLookupNode(CLVFLookupNode node) {
		super(node);
		this.lookupTable = node.lookupTable;
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

	public void setLookupName(String name) {
		this.lookupName = name;
	}

	public void setOperation(int oper) {
		this.opType = oper;
	}

	public String getLookupName() {
		return lookupName;
	}

	public int getOperation() {
		return opType;
	}

	public LookupTable getLookupTable() {
		return lookupTable;
	}

	public void setLookupTable(LookupTable lookupTable) {
		this.lookupTable = lookupTable;
	}

	public void setFormalParameters(TLType[] paramTypes) {
		this.paramTypes = paramTypes;
	}
	
	public TLType[] getFormalParameters() {
		return this.paramTypes;
	}

	@Override
	public SimpleNode duplicate() {
		return new CLVFLookupNode(this);
	}

	public void setLookupIndex(int index) {
		this.lookupIndex = index;
	}

	public int getLookupIndex() {
		return this.lookupIndex;
	}

	public void setLookupRecord(DataRecord lookupRecord) {
		this.lookupRecord = lookupRecord;
	}
	
	public DataRecord getLookupRecord() {
		return lookupRecord;
	}
	
	public void setDecimalPrecisions(List<Integer> precisions) {
		this.decimalPrecisions = precisions;
	}
	
	public List<Integer> getDecimalPrecisions() {
		return decimalPrecisions == null || decimalPrecisions.isEmpty() ? Collections.<Integer>emptyList() : new LinkedList<Integer>(decimalPrecisions);
	}

	public Lookup getLookup() {
		return lookup;
	}

	public void setLookup(Lookup lookup) {
		this.lookup = lookup;
	}

}
