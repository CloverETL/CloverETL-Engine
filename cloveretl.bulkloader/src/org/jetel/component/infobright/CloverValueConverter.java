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
package org.jetel.component.infobright;

import java.math.BigDecimal;
import java.util.Date;

import org.jetel.data.BooleanDataField;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DateDataField;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;

/**
 * Converts clover data fields to various types
 * 
 * @author avackova (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 3 Nov 2009
 */
public class CloverValueConverter implements ValueConverter {

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getBigNumber(java.lang.Object)
	 */
	@Override
	public BigDecimal getBigNumber(Object object) throws ValueConverterException {
		DataField field = (DataField)object;
		if (field.isNull()) return null;
		try {
			return ((Numeric)field).getBigDecimal();
		} catch (ClassCastException e) {
		    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + DataFieldMetadata.type2Str(field.getType()) + 
		    		" is not convertible to BigDecimal");
		} 
	}

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getBinary(java.lang.Object)
	 */
	@Override
	public byte[] getBinary(Object object) throws ValueConverterException {
		try {
			return ((ByteDataField)object).getByteArray();
		} catch (ClassCastException e) {
		    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + DataFieldMetadata.type2Str(((DataField) object).getType()) + 
		    		" is not convertible to byte[]");
		}
	}

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getBinaryString(java.lang.Object)
	 */
	@Override
	public byte[] getBinaryString(Object object) throws ValueConverterException {
		return getBinary(object);
	}

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getBoolean(java.lang.Object)
	 */
	@Override
	public Boolean getBoolean(Object object) throws ValueConverterException {
		try {
			return ((BooleanDataField)object).getBoolean();
		} catch (ClassCastException e) {
		    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + DataFieldMetadata.type2Str(((DataField) object).getType()) + 
		    		" is not convertible to Boolean");
		}
	}

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getDate(java.lang.Object)
	 */
	@Override
	public Date getDate(Object object) throws ValueConverterException {
		try {
			return ((DateDataField)object).getDate();
		} catch (ClassCastException e) {
		    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + DataFieldMetadata.type2Str(((DataField) object).getType())+ 
		    		" is not convertible to Date");
		}
	}

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getInteger(java.lang.Object)
	 */
	@Override
	public Long getInteger(Object object) throws ValueConverterException {
		DataField field = (DataField)object;
		if (field.isNull()) return null;
		try {
			return ((Numeric)field).getLong();
		} catch (ClassCastException e) {
		    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + DataFieldMetadata.type2Str(field.getType())+ 
		    		" is not convertible to Long");
		} 
	}

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getNumber(java.lang.Object)
	 */
	@Override
	public Double getNumber(Object object) throws ValueConverterException {
		DataField field = (DataField)object;
		if (field.isNull()) return null;
		try {
			return ((Numeric)field).getDouble();
		} catch (ClassCastException e) {
		    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + DataFieldMetadata.type2Str(field.getType()) + 
		    		" is not convertible to Double");
		} 
	}

	/* (non-Javadoc)
	 * @see com.infobright.etl.model.ValueConverter#getString(java.lang.Object)
	 */
	@Override
	public String getString(Object object) throws ValueConverterException {
		return object.toString();
	}

}
