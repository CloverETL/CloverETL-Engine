/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
// FILE: c:/projects/jetel/org/jetel/data/DateDataField.java

package org.jetel.data;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 *  A class that represents ...
 *
 * @author      D.Pavlis
 * @since       March 28, 2002
 * @revision    $Revision$
 * @created     January 26, 2003
 * @see         OtherClasses
 */
@SuppressWarnings("EI")
public class DateDataField extends DataField implements Comparable{

	private static final long serialVersionUID = 1529319195864286249L;
	
	// Attributes
	/**
	 *  An attribute that represents ...
	 *
	 * @since    March 28, 2002
	 */
	private Date value;
	private DateFormat dateFormat;
		
	private final static int FIELD_SIZE_BYTES = 8;// standard size of field


	// Associations

	// Operations
	
	/**
     * Constructor for the DateDataField object
     * 
	 * @param _metadata Metadata describing field
	 * @param plain create plain data field - no formatters,etc. will be assigned/created
	 */
	public DateDataField(DataFieldMetadata _metadata, boolean plain) {
        super(_metadata);
        if (!plain) {
            Locale locale;
            // handle locale
            if (_metadata.getLocaleStr() != null) {
                String[] localeLC = _metadata.getLocaleStr().split(
                        Defaults.DEFAULT_LOCALE_STR_DELIMITER_REGEX);
                if (localeLC.length > 1) {
                    locale = new Locale(localeLC[0], localeLC[1]);
                } else {
                    locale = new Locale(localeLC[0]);
                }
                // probably wrong locale string defined
                if (locale == null) {
                    throw new RuntimeException("Can't create Locale based on "
                            + _metadata.getLocaleStr());
                }
            } else {
                locale = null;
            }
            // handle format string
            String formatString;
            formatString = _metadata.getFormatStr();
            if ((formatString != null) && (formatString.length() != 0)) {
                if (locale != null) {
                    dateFormat = new SimpleDateFormat(formatString, locale);
                } else {
                    dateFormat = new SimpleDateFormat(formatString);
                }
                dateFormat.setLenient(false);
            } else if (locale != null) {
                dateFormat = DateFormat.getDateInstance(DateFormat.DEFAULT,
                        locale);
                dateFormat.setLenient(false);
            }
        }
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
	 * @param _metadata
	 * @param _value
	 * @param _dateFormat
	 */
	private DateDataField(DataFieldMetadata _metadata, Date _value, DateFormat _dateFormat){
	    super(_metadata);
	    setValue(_value);
	    this.dateFormat=_dateFormat;
	}
	

	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#copy()
	 */
	public DataField duplicate(){
	    DateDataField newField=new DateDataField(metadata,value,dateFormat);
	    newField.setNull(isNull());
	    return newField;
	}
	
	
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
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
		    throw new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
		}
	}
	
    /* (non-Javadoc)
     * @see org.jetel.data.DataField#setValue(org.jetel.data.DataField)
     */
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
	public Object getValue() {
		return isNull ? null : value;
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    public Object getValueDuplicate() {
        return isNull ? null : value.clone();
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
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			value = null;
		}
	}
    
     /* (non-Javadoc)
     * @see org.jetel.data.DataField#reset()
     */
    public void reset(){
            if (metadata.isNullable()){
                setNull(true);
            }else if (metadata.isDefaultValue()){
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
	public String toString() {
		if (value == null) {
			return "";
		}
		if ((dateFormat != null)) {
			return dateFormat.format(value);
		} else {
			return value.toString();
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer                    Description of Parameter
	 * @param  decoder                       Description of Parameter
	 * @exception  CharacterCodingException  Description of Exception
	 * @since                                October 31, 2002
	 */
	public void fromByteBuffer(ByteBuffer dataBuffer, CharsetDecoder decoder) throws CharacterCodingException {
		fromString(decoder.decode(dataBuffer));
	}


	/**
	 *  Description of the Method
	 *
	 * @param  dataBuffer                    Description of Parameter
	 * @param  encoder                       Description of Parameter
	 * @exception  CharacterCodingException  Description of Exception
	 * @since                                October 31, 2002
	 */
	public void toByteBuffer(ByteBuffer dataBuffer, CharsetEncoder encoder) throws CharacterCodingException {
		try {
			dataBuffer.put(encoder.encode(CharBuffer.wrap(toString())));
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
		}
	}

    @Override
    public void toByteBuffer(ByteBuffer dataBuffer) {
        if(!isNull) {
        	try {
        		dataBuffer.putLong(value.getTime());
        	} catch (BufferOverflowException e) {
    			throw new RuntimeException("The size of data buffer is only " + dataBuffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
        	}
        }
    }


	/* (non-Javadoc)
	 * @see org.jetel.data.DataField#fromString(java.lang.CharSequence)
	 */
	public void fromString(CharSequence seq) {
		//parsePosition.setIndex(0);
		if (seq == null || seq.length() == 0) {
		    setNull(true);
			return;
		}
		try {
			if (dateFormat != null) {
				value = dateFormat.parse(seq.toString());//, parsePosition);
			} else {
				value = SimpleDateFormat.getDateInstance().parse(seq.toString());
			}
			setNull(false);
		} catch (ParseException e) {
			throw new BadDataFormatException("not a Date", seq.toString());
		}

	}

	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void serialize(ByteBuffer buffer) {
		try {
			if (value != null) {
				buffer.putLong(value.getTime());
			} else {
				buffer.putLong(0);
			}
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + buffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
		}
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer  Description of Parameter
	 * @since          April 23, 2002
	 */
	public void deserialize(ByteBuffer buffer) {
		long tmpl = buffer.getLong();
		if (tmpl == 0) {
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
	public int compareTo(Object obj) {
		if (isNull) return -1;
	    
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

	
	public int hashCode(){
		if (isNull) return 0;
		return value.hashCode();
	}
	
	/**
	 *  Gets the size attribute of the IntegerDataField object
	 *
	 * @return    The size value
	 * @see	      org.jetel.data.DataField
	 */
	public int getSizeSerialized() {
		return FIELD_SIZE_BYTES;
	}

}
/*
 *  end class DateDataField
 */

