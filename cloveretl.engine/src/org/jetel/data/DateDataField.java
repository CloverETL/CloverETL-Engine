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
import org.jetel.util.HashCodeUtil;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;
import org.jetel.util.string.Compare;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * Represents a date data field.
 *
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th August 2009
 * @since 28th March 2002
 *
 * @see DateFormatter
 * @see DateFormatterFactory
 *
 */
@SuppressWarnings("EI")
public class DateDataField extends DataField implements Comparable<Object> {

	private static final long serialVersionUID = 1529319195864286249L;
	
	/** the actual date value */
	private Date value;
	/** the date formatter used to format this field */
	private DateFormatter dateFormatter;

	private final static int FIELD_SIZE_BYTES = 8;// standard size of field
	private final static long DATE_NULL_VAL_SERIALIZED=Long.MIN_VALUE;

	/**
     * Constructor for the DateDataField object
     * 
	 * @param metadata Metadata describing field
	 * @param plain create plain data field - no formatters,etc. will be assigned/created
	 */
	public DateDataField(DataFieldMetadata metadata, boolean plain) {
        super(metadata);
        // create a date formatter based on the format string and locale
        dateFormatter = metadata.createDateFormatter();
        this.reset();
    }

    /**
     *  Constructor for the DateDataField object
     *
     * @param  _metadata  Metadata describing field
     * @since             April 23, 2002
     */
    public DateDataField(DataFieldMetadata _metadata){
        this(_metadata,false);
    }

	/**
	 *  Constructor for the DateDataField object
	 *
	 * @param  _metadata  Description of Parameter
	 * @param  _value     Description of Parameter
	 * @since             April 23, 2002
	 */
	public DateDataField(DataFieldMetadata _metadata, Date _value) {
		this(_metadata,false);
		setValue(_value);
	}

	/**
	 * Private constructor to be used internally when clonning object.
	 * Optimized for performance. Many checks waved.<br>
     * <i>Warning: not a thread safe as the clone will reference the same
     * DateFormat object!</i>
	 * 
	 * @param metadata
	 * @param value
	 * @param dateTimeFormatter
	 * @param dateFormat
	 */
	private DateDataField(DataFieldMetadata metadata, Date value, DateFormatter dateFormatter) {
	    super(metadata);

	    setValue(value);

	    this.dateFormatter = dateFormatter;
	}

	@Override
	public DataField duplicate(){
	    DateDataField newField = new DateDataField(metadata, value, dateFormatter);
	    newField.setNull(isNull());

	    return newField;
	}
	
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	@Override
	public void copyFrom(DataField fromField){
	    if (fromField instanceof DateDataField){
	        if (!fromField.isNull){
	        	if (this.value == null) {
	        		this.value = new Date(((DateDataField)fromField).value.getTime());
	        	} else {
	            	this.value.setTime(((DateDataField)fromField).value.getTime());
	        	}
	        }
	        setNull(fromField.isNull);
	    } else {
	        super.copyFrom(fromField);
        }
	}
	
	/**
	 *  Sets the date represented by DateDataField object
	 *
	 * @param  _value                      The new Value value
	 * @exception  BadDataFormatException  Description of the Exception
	 * @since                              April 23, 2002
	 */
	@Override
	public void setValue(Object _value) throws BadDataFormatException {
        if(_value == null) {
            setNull(true);
        } else if(_value instanceof Date) {
			if (value == null) {
				value = new Date(((Date) _value).getTime());
			} else {
				value.setTime(((Date) _value).getTime());
			}
			setNull(false);
		} else if (_value instanceof Timestamp) {
		    if (value == null){
				value = new Date(((Timestamp) _value).getTime());
		    } else {
		    	value.setTime(((Timestamp) _value).getTime());
		    }
		    setNull(false);
		}else if (_value instanceof Number){
			setValue(((Number)_value).longValue());
		}else {
			BadDataFormatException ex = new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
		}
	}
	
    @Override
    public void setValue(DataField fromField) {
        if (fromField instanceof DateDataField){
            if (!fromField.isNull){
                if (this.value == null) {
                    this.value = new Date(((DateDataField)fromField).value.getTime());
                } else {
                    this.value.setTime(((DateDataField)fromField).value.getTime());
                }
            }
            setNull(fromField.isNull);
        } else {
            super.setValue(fromField);
        }
    }
    
	/**
	 * Sets the date represented by DateDataField object
	 * 	
	 * @param time the number of milliseconds since January 1, 1970, 00:00:00 GM
	 */
	public void setValue(long time){
	    if (value==null){
			value = new Date(time);
		}else{
			value.setTime(time);
		}
		setNull(false);
	    
	}


	/**
	 *  Gets the date represented by DateDataField object
	 *
	 * @return    The Value value
	 * @since     April 23, 2002
	 */
	@Override
	public Date getValue() {
		return isNull ? null : value;
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    @Override
	public Date getValueDuplicate() {
        return isNull ? null : new Date(value.getTime());
    }

	/**
	 *  Gets the date attribute of the DateDataField object
	 *
	 * @return    The date value
	 */
	public Date getDate() {
		return isNull ? null : value;
	}


	/**
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 * @since          October 29, 2002
	 */
	@Override
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			value = null;
		}
	}
    
    @Override
	public void reset(){
            if (metadata.isNullable()){
                setNull(true);
            }else if (metadata.isDefaultValueSet()){
                setToDefaultValue();
            }else{
            	if (value == null) {
            		value = new Date(0);
            	}else{
            		value.setTime(0);
            	}
            }
        }


	/**
	 *  Gets the DataField type
	 *
	 * @return    The Type value
	 * @since     April 23, 2002
	 */
	@Override
	@Deprecated
	public char getType() {
		return DataFieldMetadata.DATE_FIELD;
	}


	/**
	 *  Formats the internal date value into a string representation.<b> If
	 *  metadata describing DateField contains formating string, that string is
	 *  used to create output. Otherwise the standard format is used.
	 *
	 * @return    Description of the Returned Value
	 * @since     April 23, 2002
	 */
	@Override
	public String toString() {
		if (value == null) {
			return metadata.getNullValue();
		}

		return dateFormatter.format(value);
	}

	@Override
	public void fromString(CharSequence seq) {
		if (seq == null || Compare.equals(seq, metadata.getNullValue())) {
		    setNull(true);
		    return;
		}

		try {
			if (value == null) {
				value = dateFormatter.parseDate(seq.toString());
			} else {
				value.setTime(dateFormatter.parseMillis(seq.toString()));
			}

			setNull(false);
		} catch (IllegalArgumentException exception) {
			throw new BadDataFormatException(String.format("%s (%s) cannot be set to \"%s\" - doesn't match defined format \"%s\"",
					getMetadata().getName(), getMetadata().getDataType().getName(), seq, getMetadata().getFormatStr()),
					seq.toString(), exception);
		}
	}

	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
		try {
			if (value != null) {
				buffer.putLong(value.getTime());
			} else {
				buffer.putLong(DATE_NULL_VAL_SERIALIZED);
			}
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		long tmpl = buffer.getLong();
		if (tmpl == DATE_NULL_VAL_SERIALIZED) {
			setNull(true);
			return;
		}
		if (value == null) {
			value = new Date(tmpl);
		} else {
			value.setTime(tmpl);
		}
		setNull(false);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  obj  Description of Parameter
	 * @return      Description of the Returned Value
	 * @since       April 23, 2002
	 */
	@Override
	public boolean equals(Object obj) {
	    if (isNull || obj==null) return false;
	    
	    if (obj instanceof DateDataField){
	        return (this.value.equals((((DateDataField) obj).value)));
	    }else if (obj instanceof java.util.Date){
	        return this.value.equals((java.util.Date) obj);
	    }else if (obj instanceof java.sql.Date){
	        return this.value.getTime()==((java.sql.Date)obj).getTime();
	    }else{
	        return false;
	    }
	}


	/**
	 *  Compares this object with the specified object for order
	 *
	 * @param  obj  Description of the Parameter
	 * @return      Description of the Return Value
	 */
	@Override
	public int compareTo(Object obj) {
		if (isNull) return -1;
		if (obj == null) return 1;
	    
		if (obj instanceof java.util.Date){
			return value.compareTo((java.util.Date) obj);
		}else if (obj instanceof DateDataField){
			if (!((DateDataField) obj).isNull()) {
				return value.compareTo(((DateDataField) obj).value);
			}else{
				return 1;
			}
		}else if (obj instanceof java.sql.Date){
		    long result=value.getTime()-((java.sql.Date)obj).getTime();
		    if (result>0) return 1; else if (result<0) return -1; else return 0;
		}else throw new ClassCastException("Can't compare DateDataField and "+obj.getClass().getName());
	}

	
	@Override
	public int hashCode() {
		return HashCodeUtil.hash(value);
	}
	
	/**
	 *  Gets the size attribute of the IntegerDataField object
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	@Override
	public int getSizeSerialized() {
		return FIELD_SIZE_BYTES;
	}

}
