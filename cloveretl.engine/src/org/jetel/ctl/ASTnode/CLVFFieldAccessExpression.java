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
import org.jetel.metadata.DataRecordMetadata;

public class CLVFFieldAccessExpression extends SimpleNode {
	
	private String recordName;
	private String fieldName;
	private String discriminator;

	private DataRecordMetadata metadata; 
	private Integer recordId = null;
	private Integer fieldId = null;
	private boolean isOutput;
	private boolean isWildcard = false;
	
	
	public CLVFFieldAccessExpression(int id) {
		super(id);
	}
	
	public CLVFFieldAccessExpression(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFFieldAccessExpression(CLVFFieldAccessExpression node) {
		super(node);
		this.recordName = node.recordName;
		this.fieldName = node.fieldName;
		this.metadata = node.metadata;
		this.recordId = node.recordId;
		this.fieldId = node.fieldId;
		this.isOutput = node.isOutput;
		this.isWildcard = node.isWildcard;
		this.discriminator = node.discriminator;
	}


	public void setMetadata(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}
	
	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	public Integer getRecordId() {
		return recordId;
	}

	public void setRecordId(int recordId) {
		this.recordId = recordId;
	}

	public Integer getFieldId() {
		return fieldId;
	}

	public void setFieldId(int fieldId) {
		this.fieldId = fieldId;
	}

	public boolean isOutput() {
		return isOutput;
	}

	public void setOutput(boolean isOutput) {
		this.isOutput = isOutput;
	}
	
	public void setRecordName(String name) {
		this.recordName = name;
	}
	
	public void setFieldName(String name) {
		this.fieldName = name;
	}
	
	public void setWildcard(boolean isWildcard) {
		this.isWildcard = isWildcard;
	}
	
	public boolean isWildcard() {
		return isWildcard;
	}
	
	public String getRecordName() {
		return recordName;
	}
	
	public String getFieldName() {
		return fieldName;
	}
	
	/**
	 * @return the discriminator
	 */
	public String getDiscriminator() {
		return discriminator;
	}

	/**
	 * @param discriminator the discriminator to set
	 */
	public void setDiscriminator(String discriminator) {
		this.discriminator = discriminator;
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
		return new CLVFFieldAccessExpression(this);
	}

	@Override
	public boolean isBreakable() {
		return true;
	}
}
