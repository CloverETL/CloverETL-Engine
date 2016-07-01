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

import java.nio.BufferOverflowException;
import java.sql.Timestamp;
import java.util.Date;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.util.CloverPublicAPI;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.Compare;
import org.threeten.bp.Instant;
import org.threeten.bp.format.DateTimeFormatter;
import org.threeten.bp.format.DateTimeParseException;

/**
 * Field which represents an instant with nanoseconds precision.
 * 
 * @author martin (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 6. 2016
 */
@CloverPublicAPI
public class NanoDateDataField extends DataFieldImpl implements Comparable<Object> {

	private NanoDate value = new NanoDate();

	private DateTimeFormatter formatter;

	private final static int LONG_SIZE = 8; // Long.BYTES (can be used from Java 1.8)
	private final static int INTEGER_SIZE = 4; // Integer.BYTES (can be used from Java 1.8)
	private final static int NANO_DATE_SIZE = LONG_SIZE + INTEGER_SIZE; // standard size of serialized field
	private final static long NULL_VALUE_SERIALIZED = Long.MIN_VALUE;

	public NanoDateDataField(DataFieldMetadata metadata) {
		super(metadata);
		formatter = metadata.createNanoDateFormatter();
		this.reset();
	}

	private NanoDateDataField(DataFieldMetadata metadata, NanoDate value, DateTimeFormatter formatter) {
		super(metadata);
		this.formatter = formatter;
		setValue(value);
		this.reset();
	}

	@Override
	public DataField duplicate() {
		NanoDateDataField newField = new NanoDateDataField(metadata, value, formatter);
		newField.setNull(isNull());
		return newField;
	}

	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
	 * @deprecated use setValue(DataField) instead
	 */
	@java.lang.SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public void copyFrom(DataField fromField) {
		setValue(fromField);
	}

	@Override
	public void setValue(Object newValue) throws BadDataFormatException {
		if (newValue == null) {
			setNull(true);
		} else if (newValue instanceof NanoDate) {
			value.setTime((NanoDate) newValue);
			setNull(false);
		} else if (newValue instanceof Date) {
			value.setTime((Date) newValue);
			setNull(false);
		} else if (newValue instanceof Timestamp) {
			value.setTime((Timestamp) newValue);
			setNull(false);
		} else {
			BadDataFormatException ex = new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + newValue.toString(), newValue.toString());
			ex.setFieldNumber(getMetadata().getNumber());
			throw ex;
		}
	}

	@Override
	public void setValue(DataField fromField) {
		if (fromField instanceof NanoDateDataField) {
			if (!fromField.isNull()) {
				this.value.setTime(((NanoDateDataField) fromField).value);
			}
			setNull(fromField.isNull());
		} else if (fromField instanceof DateDataField) {
			if (!fromField.isNull()) {
				this.value.setTime(((DateDataField) fromField).getDate());
			}
			setNull(fromField.isNull());
		} else {
			super.setValue(fromField);
		}
	}

	@Override
	public NanoDate getValue() {
		return isNull ? null : value;
	}

	/**
	 * @see org.jetel.data.DataField#getValueDuplicate()
	 */
	@Override
	public NanoDate getValueDuplicate() {
		return isNull ? null : new NanoDate(value);
	}

	public NanoDate getNanoDate() {
		return isNull ? null : value;
	}

	/**
	 * WARNING: the return value <code>org.threeten.bp.Instant</code>
	 * will be changed to <code>java.time.Instant</code> after full support of java 8 by clover
	 */
	public Instant getInstant() {
		return isNull ? null : value.getInstant();
	}

	@Override
	public void reset() {
		if (metadata.isNullable()) {
			setNull(true);
		} else if (metadata.isDefaultValueSet()) {
			setToDefaultValue();
		} else {
			value.setEpoch();
		}
	}

	@Override
	@Deprecated
	public char getType() {
		return DataFieldType.NANODATE.getObsoleteIdentifier();
	}

	/**
	 * Formats the internal date value into a string representation.<b> If metadata describing DateField contains
	 * formating string, that string is used to create output. Otherwise the standard format is used.
	 */
	@Override
	public String toString() {
		if (value == null) {
			return metadata.getNullValue();
		}

		return formatter.format(value.getInstant());
	}

	@Override
	public void fromString(CharSequence seq) {
		if (seq == null || Compare.equals(seq, metadata.getNullValues())) {
			setNull(true);
			return;
		}

		try {
			value.setTime(formatter.parse(seq, Instant.FROM));
			setNull(false);
		} catch (DateTimeParseException e) {
			throw new BadDataFormatException(String.format("%s (%s) cannot be set to \"%s\" - doesn't match defined format \"%s\"", getMetadata().getName(), getMetadata().getDataType().getName(), seq, getMetadata().getFormatStr()), seq.toString(), e);
		}
	}

	/**
	 * Performs serialization of the internal value into CloverBuffer (used when moving data records between
	 * components).
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
		try {
			if (!isNull) {
				buffer.putLong(value.getSeconds());
				buffer.putInt(value.getNanos());
			} else {
				buffer.putLong(NULL_VALUE_SERIALIZED);
			}
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}

	@Override
	public void serialize(CloverBuffer buffer, DataRecordSerializer serializer) {
		serializer.serialize(buffer, this);
	}

	/**
	 * Performs deserialization of data
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		long tmpl = buffer.getLong();
		if (tmpl == NULL_VALUE_SERIALIZED) {
			setNull(true);
			return;
		}
		value.setTime(tmpl, buffer.getInt());
		setNull(false);
	}

	@Override
	public void deserialize(CloverBuffer buffer, DataRecordSerializer serializer) {
		serializer.deserialize(buffer, this);
	}

	@Override
	public boolean equals(Object obj) {
		if (isNull || obj == null) {
			return false;
		}

		if (obj instanceof NanoDateDataField) {
			NanoDateDataField nanoDateDataField = (NanoDateDataField) obj;
			return !nanoDateDataField.isNull && this.value.equals(nanoDateDataField.value);
		} else if (obj instanceof DateDataField) {
			DateDataField dateDataField = (DateDataField) obj;
			return !dateDataField.isNull && this.value.equals(dateDataField.getNanoDate());
		} else if (obj instanceof NanoDate) {
			return this.value.equals((NanoDate) obj);
		} else if (obj instanceof Date) {
			return this.value.equals(NanoDate.from((Date) obj));
		} else if (obj instanceof Timestamp) {
			return this.value.equals(NanoDate.from((Timestamp) obj));
		} else {
			return false;
		}
	}

	/**
	 * Compares this object with the specified object for order
	 */
	@Override
	public int compareTo(Object obj) {
		if (isNull) {
			return -1;
		}
		if (obj == null) {
			return 1;
		}

		if (obj instanceof NanoDateDataField) {
			if (!((NanoDateDataField) obj).isNull) {
				return value.compareTo(((NanoDateDataField) obj).value);
			} else {
				return 1;
			}
		} else if (obj instanceof DateDataField) {
			if (!((DateDataField) obj).isNull) {
				return value.compareTo(((DateDataField) obj).getNanoDate());
			} else {
				return 1;
			}
		} else if (obj instanceof NanoDate) {
			return value.compareTo((NanoDate) obj);
		} else if (obj instanceof Date) {
			return value.compareTo(NanoDate.from((Date) obj));
		} else if (obj instanceof Timestamp) {
			return value.compareTo(NanoDate.from((Timestamp) obj));
		} else
			throw new ClassCastException("Can't compare NanoDateDataField and " + obj.getClass().getName());
	}

	@Override
	public int hashCode() {
		return HashCodeUtil.hash(value);
	}

	/**
	 * Gets the size attribute of the NanoDateDataField object
	 */
	@Override
	public int getSizeSerialized() {
		return isNull ? LONG_SIZE : NANO_DATE_SIZE;
	}

}
