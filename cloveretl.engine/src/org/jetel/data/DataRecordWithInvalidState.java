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
package org.jetel.data;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Implementation of {@link DataRecord} interface, which use
 * {@link DataFieldWithInvalidState} instead of regular {@link DataFieldImpl}.
 * This type of record is able to mark a field with validity flag. Access to an invalid value
 * in a field throw {@link DataFieldInvalidStateException}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 5. 2014
 */
public class DataRecordWithInvalidState extends DataRecordImpl {

	private static final long serialVersionUID = 4435958666027665079L;

	@SuppressWarnings("deprecation")
	DataRecordWithInvalidState(DataRecordMetadata _metadata) {
		super(_metadata);
	}

	@Override
	protected DataRecordImpl newInstance(DataRecordMetadata metadata) {
		return new DataRecordWithInvalidState(metadata);
	}
	
	public void setValid(boolean valid) {
		for (DataField field : this) {
			((DataFieldWithInvalidState) field).setValid(valid);
		}
	}
	
	@Override
	protected DataField createField(DataFieldType fieldType, DataFieldMetadata fieldMetadata, boolean plain) {
		return DataFieldFactory.DATA_FIELD_WITH_INVALID_STATE.create(fieldType, fieldMetadata, plain);
	}
	
	@Override
	public DataFieldWithInvalidState getField(int _fieldNum) {
		return (DataFieldWithInvalidState) super.getField(_fieldNum);
	}

	@Override
	public DataFieldWithInvalidState getField(String _name) {
		return (DataFieldWithInvalidState) super.getField(_name);
	}
	
	@Override
	public DataFieldWithInvalidState getFieldByLabel(String _label) {
		return (DataFieldWithInvalidState) super.getFieldByLabel(_label);
	}
}
