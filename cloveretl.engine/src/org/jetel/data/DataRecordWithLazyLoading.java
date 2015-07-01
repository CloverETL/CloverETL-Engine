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
 * DataRecord that contains fields of type {@link DataFieldWithLazyLoading}
 * 
 * @author salamonp (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 11. 5. 2015
 */
public class DataRecordWithLazyLoading extends DataRecordImpl {

	@SuppressWarnings("deprecation")
	DataRecordWithLazyLoading(DataRecordMetadata _metadata) {
		super(_metadata);
	}

	@Override
	protected DataRecordImpl newInstance(DataRecordMetadata metadata) {
		return new DataRecordWithLazyLoading(metadata);
	}

	@Override
	protected DataField createField(DataFieldType fieldType, DataFieldMetadata fieldMetadata, boolean plain) {
		return DataFieldFactory.DATA_FIELD_WITH_LAZY_LOADING.create(fieldType, fieldMetadata, plain);
	}

	@Override
	public DataFieldWithLazyLoading getField(int _fieldNum) {
		return (DataFieldWithLazyLoading) super.getField(_fieldNum);
	}

	@Override
	public DataFieldWithLazyLoading getField(String _name) {
		return (DataFieldWithLazyLoading) super.getField(_name);
	}

	@Override
	public DataFieldWithLazyLoading getFieldByLabel(String _label) {
		return (DataFieldWithLazyLoading) super.getFieldByLabel(_label);
	}
}
