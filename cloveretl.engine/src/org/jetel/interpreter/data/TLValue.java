/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 21.3.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetel.data.ByteDataField;
import org.jetel.data.CompressedByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DateDataField;
import org.jetel.data.primitive.ByteArray;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.string.Compare;

public class TLValue implements Comparable<TLValue>{
    
    public static final TLValue NULL_VAL=new TLValue(TLValueType.NULL,null);
    public static final TLValue TRUE_VAL=new TLValue(TLValueType.BOOLEAN,Boolean.TRUE);
    public static final TLValue FALSE_VAL=new TLValue(TLValueType.BOOLEAN,Boolean.FALSE);
    public static final TLValue NUM_ZERO_VAL = new TLValue(TLValueType.INTEGER,new CloverInteger(0));
    public static final TLValue NUM_ONE_VAL = new TLValue(TLValueType.INTEGER,new CloverInteger(1)); 
    public static final TLValue NUM_MINUS_ONE_VAL = new TLValue(TLValueType.INTEGER,new CloverInteger(-1)); 
    public static final TLValue NUM_PI_VAL = new TLValue(TLValueType.DOUBLE,new CloverDouble(Math.PI));
    public static final TLValue NUM_E_VAL = new TLValue(TLValueType.DOUBLE,new CloverDouble(Math.E));
    
    
    public TLValueType type;
    public Object value;
    
    public TLValue(TLValueType type,Object value) {
        this.type=type;
        this.value=value;
    }
    
    public TLValue(TLValueType type) {
        this(type,null);
    }
    
    public TLValue(DataField field) {
    	this.type=TLValueType.convertType(field.getMetadata());
        setValue(field);
    }
    
    public void setValue(Object _value) {
        this.value=_value;
    }
    
    public final void setValue(DataField field) {
        if (field.isNull()) {
            this.value=null;
        }else {
            if (type.isNumeric()) {
                this.value=((Numeric)field).duplicateNumeric();
            }else {
                this.value=field.getValueDuplicate();
            }
        }
    }
    
    
    public Object getValue() {
        return value;
    }
    
    public final boolean getBoolean() {
        if (type==TLValueType.BOOLEAN) {
            return ((Boolean)value).booleanValue();
        }
        throw new RuntimeException("not a "+TLValueType.BOOLEAN+" value");
    }
    
    public final int getInt() {
        if (type.isNumeric()) {
            return ((Numeric)value).getInt();
        }
        throw new RuntimeException("not a "+TLValueType.INTEGER+" value");
    }
    
    public final long getLong() {
        if (type.isNumeric()) {
            return ((Numeric)value).getLong();
        }
        throw new RuntimeException("not a "+TLValueType.LONG+" value");
    }
    
    public final double getDouble() {
        if (type.isNumeric()) {
            return ((Numeric)value).getDouble();
        }
        throw new RuntimeException("not a "+TLValueType.DOUBLE+" value");
    }
    
    public final Numeric getNumeric() {
        if (type.isNumeric()) {
            return (Numeric)value;
        }
        throw new RuntimeException("not a "+TLValueType.DOUBLE+" value");
    }
    
    public final CharSequence getCharSequence() {
        if (type==TLValueType.STRING) {
            return (CharSequence)value;
        }
        throw new RuntimeException("not a "+TLValueType.STRING+" value");
    }
    
    public final String getString() {
        if (type.isPrimitive()) {
            return value.toString();
        }
        throw new RuntimeException("not a "+TLValueType.STRING+" value");
    }
    
    public final ByteArray getByte() {
    	if (type==TLValueType.BYTE){
    		return (ByteArray)value;
    	}
    	throw new RuntimeException("not a "+TLValueType.BYTE+" value");
    }
    
    public final Date getDate() {
        if (type==TLValueType.DATE) {
            return (Date)value;
        }
        throw new RuntimeException("not a "+TLValueType.DATE+" value");
    }
    
    
    public final Map getMap() {
        if (type==TLValueType.MAP) {
            return (Map)value;
        }
        throw new RuntimeException("not a "+TLValueType.MAP+" value");
    }
    
    public final List<TLValue> getList() {
        if (type==TLValueType.LIST) {
            return (List<TLValue>)value;
        }
        throw new RuntimeException("not a "+TLValueType.LIST+" value");
    }
    
    
    public final boolean isNull() {
        return (value==null || (type==TLValueType.STRING && ((CharSequence)value).length()==0 )) ;
    }
    
    public final TLValueType getType() {
        return type;
    }

    public final static TLValue convertValue(DataField field) {
        switch(field.getMetadata().getType()) {
        case DataFieldMetadata.INTEGER_FIELD:
            return new TLValue(TLValueType.INTEGER,new CloverInteger((Numeric)field));
        case DataFieldMetadata.LONG_FIELD:
            return new TLValue(TLValueType.LONG,new CloverLong((Numeric)field));
        case DataFieldMetadata.NUMERIC_FIELD:
            return new TLValue(TLValueType.DOUBLE,new CloverDouble((Numeric)field));
        case DataFieldMetadata.DECIMAL_FIELD:
            return new TLValue(TLValueType.DECIMAL,((Decimal)field).createCopy());
        case DataFieldMetadata.DATE_FIELD:
           return new TLValue(TLValueType.DATE,field.getValueDuplicate());
        case DataFieldMetadata.BYTE_FIELD:
        	return new TLValue(TLValueType.BYTE,field.getValue());
        case DataFieldMetadata.STRING_FIELD:
            return new TLValue(TLValueType.STRING,field.getValueDuplicate());
        default:
            throw new IllegalArgumentException("Don't know how to convert "+field.getType());
        
        }
    }
    
    public final void copyToDataField(DataField field) {
		if (value == null) {
			field.setNull(true);
		} else {
			switch (field.getMetadata().getType()) {
			case DataFieldMetadata.INTEGER_FIELD:
			case DataFieldMetadata.LONG_FIELD:
			case DataFieldMetadata.NUMERIC_FIELD:
			case DataFieldMetadata.DECIMAL_FIELD:
				if (type.isNumeric()) {
					((Numeric) field).setValue(getNumeric());
					return;
				}
			case DataFieldMetadata.DATE_FIELD:
				if (type == TLValueType.DATE) {
					((DateDataField) field).setValue(getDate().getTime());
					return;
				}
			case DataFieldMetadata.BYTE_FIELD:
				((CompressedByteDataField) field).setValue(getByte()
						.getValueDuplicate());
				break;
			case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
				((ByteDataField) field).setValue(getByte().getValueDuplicate());
				break;

			}
			field.fromString(getCharSequence());
		}
	}
    
    @Override public String toString() {
        return value!=null ? value.toString() : "null";
    }
    
    public TLValue duplicate() {
        TLValue newVal = new TLValue(type);
        switch (type) {
        case INTEGER:
        case DOUBLE:
        case LONG:
        case DECIMAL:
            newVal.value = ((Numeric) value).duplicateNumeric();
            break;
        case DATE:
            newVal.value = ((Date) value).clone();
            break;
        case STRING:
            newVal.value = new StringBuilder((CharSequence)value);
            break;
        case BYTE:
        	newVal.value = getByte().duplicate();
        	break;
        case BOOLEAN:
            newVal.value = value;
            break;
        case LIST:
            newVal.value = new ArrayList<TLValue>((List) value);
            break;
        case MAP:
            newVal.value = new HashMap<String, TLValue>((Map) value);
            break;
        default:
            throw new IllegalArgumentException(
                    "Can't duplicate value type: " + type);
        }

        return newVal;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TLValue) {
            return compareTo((TLValue)obj)==0;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        if (this.value!= null)
            return this.value.hashCode();
        return super.hashCode();
    }

    public int compareTo(TLValue o) {
        if (this.value==null) return -1;
        else if (o.value==null) return 1;
        else if (type!=o.type) return -1;
        switch (type) {
        case INTEGER:
        case DOUBLE:
        case LONG:
        case DECIMAL:
            return getNumeric().compareTo(o.getNumeric());
        case DATE:
            return getDate().compareTo(o.getDate());
        case STRING:
            return Compare.compare(getCharSequence(), o.getCharSequence());
        case BYTE:
        	return getByte().compareTo(o.getByte());
        case BOOLEAN:
            return getBoolean()==o.getBoolean() ? 0 : -1;
        case LIST:
        case MAP:
            return -1;
        default:
            throw new IllegalArgumentException(
                    "Can't compare value type: " + type + " with type: "+o.type);
        }

    }
    
    public final static TLValue create(TLValueType type) {
        switch (type) {
        case INTEGER:
            return new TLValue(type,new CloverInteger(0));
        case DOUBLE:
            return new TLValue(type,new CloverDouble(0));
        case LONG:
            return new TLValue(type,new CloverLong(0));
        case DECIMAL:
            return new TLValue(type,DecimalFactory.getDecimal());
        case DATE:
            return new TLValue(type,new Date(0));
        case STRING:
            return new TLValue(type,new StringBuilder());
        case BYTE:
        	return new TLValue(type, new ByteArray());
        case BOOLEAN:
            return TLValue.FALSE_VAL;
        case LIST:
            return new TLValue(type,new ArrayList<TLValue>());
        case MAP:
            return new TLValue(type,new HashMap<String, TLValue>());
        default:
            throw new IllegalArgumentException(
                    "Can't create value type: " + type);
        }
    }
    
}
