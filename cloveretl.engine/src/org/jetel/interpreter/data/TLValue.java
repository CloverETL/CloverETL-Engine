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

import org.jetel.data.DataField;
import org.jetel.data.DateDataField;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

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
        setValue(field);
    }
    
    public void setValue(Object _value) {
        this.value=_value;
    }
    
    public final void setValue(DataField field) {
        this.type=TLValueType.convertType(field.getMetadata());
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
    
    public final List getList() {
        if (type==TLValueType.LIST) {
            return (List)value;
        }
        throw new RuntimeException("not a "+TLValueType.LIST+" value");
    }
    
    
    public final boolean isNull() {
        return value==null;
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
        case DataFieldMetadata.STRING_FIELD:
            return new TLValue(TLValueType.STRING,field.getValueDuplicate());
        default:
            return new TLValue(TLValueType.STRING,field.toString());
        
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
                    ((Numeric) field).setValue((Numeric) value);
                    return;
                }
            case DataFieldMetadata.DATE_FIELD:
                if (type == TLValueType.DATE) {
                    ((DateDataField) field).setValue(value);
                    return;
                }
            }
        field.fromString((CharSequence) value.toString());
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
            newVal.value = new StringBuilder(((StringBuilder) value));
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
            throw new UnsupportedOperationException(
                    "Can't duplicate value type: " + type);
        }

        return newVal;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this.value == null )return false;
        return this.value.equals(obj);
    }
    
    @Override
    public int hashCode() {
        if (this.value!= null)
            return this.value.hashCode();
        return super.hashCode();
    }

    public int compareTo(TLValue o) {
        if (this.value instanceof Comparable) {
            return ((Comparable)this.value).compareTo(o);
        }
        return -1;
    }
}
