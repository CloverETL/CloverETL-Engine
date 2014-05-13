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

import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.formatter.BooleanFormatter;
import org.jetel.util.formatter.BooleanFormatterFactory;
import org.jetel.util.formatter.ParseBooleanException;
import org.jetel.util.string.Compare;

/**
 * Instance of this class represents boolean value.
 * Use formatString attribute of metadata to specify string values, which will be evaluated as true.
 * It's regExp which is used by method fromString() to recognize true values.
 * It's case sensitive and default is "T|TRUE|YES|Y||t|true|1|yes|y".
 * 
 * @author Martin Varecha <martin.varecha@javlinconsulting.cz>
 * (c) JavlinConsulting s.r.o.
 * www.javlinconsulting.cz
 * @created Nov 20, 2007
 * @since Nov 20, 2007
 */
public class BooleanDataField extends DataFieldImpl implements Comparable<Object> {

	private static final long serialVersionUID = 7318127447273839212L;
	
	private boolean value;
	private final BooleanFormatter booleanFormatter;
	// standard size of field in serialized form
	private final static int FIELD_SIZE_BYTES = 1;

    /**
     *  Constructor for the BooleanDataField object
     *
     * @param  _metadata  Metadata describing field
     * @since Nov 20, 2007
     */
    public BooleanDataField(DataFieldMetadata _metadata){
        this(_metadata,false);
    }

	/**
	 * Private constructor to be used internally when clonning object.
	 * @param _metadata
	 * @param _value
	 */
	private BooleanDataField(DataFieldMetadata _metadata, boolean _value){
	    super(_metadata);
	    setValue(_value);
	    
	    booleanFormatter = BooleanFormatterFactory.createFormatter(_metadata.getFormat());
	}
	
	@Override
	public DataField duplicate(){
	    BooleanDataField newField=new BooleanDataField(metadata,value);
	    newField.setNull(isNull());
	    return newField;
	}
	
	/**
	 * @see org.jetel.data.DataField#copyField(org.jetel.data.DataField)
     * @deprecated use setValue(DataField) instead
	 */
	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public void copyFrom(DataField fromField){
	    if (fromField instanceof BooleanDataField){
       		this.value = ((BooleanDataField)fromField).value;
	        setNull(fromField.isNull());
	    } else {
	        super.copyFrom(fromField);
        }
	}
	
	/**
	 *  Sets the boolean value.
	 *
	 * @param  _value                      The new Value value
	 * @exception  BadDataFormatException  Description of the Exception
	 */
	@Override
	public void setValue(Object _value) throws BadDataFormatException {
        if(_value == null) {
            setNull(true);
        } else if(_value instanceof Boolean) {
			value = (Boolean)_value;
			setNull(false);
        } else if(_value instanceof String) {
			value = Boolean.valueOf( (String)_value );
			setNull(false);
		}else {
			BadDataFormatException ex = new BadDataFormatException(getMetadata().getName() + " field can not be set with this object - " + _value.toString(), _value.toString());
        	ex.setFieldNumber(getMetadata().getNumber());
        	throw ex;
		}
	}
	
    @Override
    public void setValue(DataField fromField) {
        if (fromField instanceof BooleanDataField){
        	this.value = ((BooleanDataField)fromField).value;
            setNull(fromField.isNull());
        } else {
            super.setValue(fromField);
        }
    }
    
    public void setValue(boolean value){
    	this.value=value;
    	setNull(false);
    }

	/**
	 *  Gets the date represented by DateDataField object
	 *
	 * @return    The Value value
	 * @since     April 23, 2002
	 */
	@Override
	public Boolean getValue() {
		return isNull ? null : Boolean.valueOf(value);
	}

    /**
     * @see org.jetel.data.DataField#getValueDuplicate()
     */
    @Override
	public Boolean getValueDuplicate() {
        return getValue();
    }

	/**
	 *  Gets the boolean attribute of the BooleanDataField object
	 *
	 * @return    The boolean value
	 */
	public boolean getBoolean() {
		return value;
	}

	/**
	 *  Sets the Null value indicator
	 *
	 * @param  isNull  The new Null value
	 */
	@Override
	public void setNull(boolean isNull) {
		super.setNull(isNull);
		if (this.isNull) {
			value = Boolean.FALSE;
		}
	}
    
    @Override
	public void reset(){
            if (metadata.isNullable()){
                setNull(true);
            }else if (metadata.isDefaultValueSet()){
                setToDefaultValue();
            }else{
            	throw new IllegalStateException(getMetadata().getName() + " Field cannot be null and default value is not defined");
            }
    }

	/**
	 *  Gets the DataField type
	 *
	 * @return    The Type value
	 */
	@Override
	@Deprecated
	public char getType() {
		return DataFieldMetadata.BOOLEAN_FIELD;
	}

	/**
	 * Returns value in String format.
	 * @return    
	 */
	@Override
	public String toString() {
		if (isNull()) {
			return metadata.getNullValue();
		} else {
			return booleanFormatter.formatBoolean(value);
		}
	}

	@Override
	public void fromString(CharSequence seq) {
		if (seq == null || Compare.equals(seq, metadata.getNullValues())) {
		    setNull(true);
			return;
		}
		try{
			value = booleanFormatter.parseBoolean(seq);
			setNull(false);
		} catch (ParseBooleanException e) {
			throw new BadDataFormatException(String.format("%s (%s) cannot be set to \"%s\" - - doesn't match defined True/False format \"%s\" ",
					getMetadata().getName(), getMetadata().getDataType().getName(), seq, booleanFormatter.toString()),
					seq.toString(),e);
		}
	}

	/**
	 *  Performs serialization of the internal value into ByteBuffer (used when
	 *  moving data records between components).
	 *
	 * @param  buffer  
	 */
	@Override
	public void serialize(CloverBuffer buffer) {
		try {
			if (isNull())
				buffer.put((byte)-1);
			else
				buffer.put( value ? (byte)1 : (byte)0 ); // dataBuffer accepts only bytes
		} catch (BufferOverflowException e) {
			throw new RuntimeException("The size of data buffer is only " + buffer.maximumCapacity() + ". Set appropriate parameter in defaultProperties file.", e);
		}
	}


	/**
	 *  Performs deserialization of data
	 *
	 * @param  buffer
	 */
	@Override
	public void deserialize(CloverBuffer buffer) {
		byte tmpl = buffer.get();
		if (tmpl == (byte)-1) {
			setNull(true);
			return;
		}
		value = (tmpl == (byte)1);
		setNull(false);
	}

	@Override
	public boolean equals(Object obj) {
	    if (obj==null) return false;
	    
	    if (obj instanceof BooleanDataField){
	    	BooleanDataField objBool = (BooleanDataField)obj; 
	    	if (isNull && objBool.isNull) 
	    		return true;
	    	if (!isNull && !objBool.isNull)
	    		return ( this.value == ((BooleanDataField) obj).value );
	    } else if (obj instanceof Boolean){
	    	if (isNull) 
	    		return false;
	        return this.value == ((Boolean)obj).booleanValue() ;
	    }
	    return false;
	}

	/**
	 *  Compares this object with the specified object for order
	 *
	 * @param  obj  
	 * @return      
	 */
	@Override
	public int compareTo(Object obj) {
		if (isNull) return -1;
	    if (obj == null) return 1;
	    
		if (obj instanceof Boolean){
			Boolean v = Boolean.valueOf(value);
			return v.compareTo((Boolean) obj);
		}else if (obj instanceof BooleanDataField){
			if (!((BooleanDataField) obj).isNull()) {
				Boolean v = Boolean.valueOf(value);
				return v.compareTo(((BooleanDataField) obj).value);
			}else{
				return 1;
			}
		}else throw new ClassCastException("Can't compare DateDataField and "+obj.getClass().getName());
	}

	@Override
	public int hashCode(){
		if (isNull) return HashCodeUtil.SEED;
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
