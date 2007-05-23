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
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

public class TLValue {
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
        this.type=convertType(field.getMetadata());
        this.value=field.getValueDuplicate();
    }
    
    public void setValue(Object _value) {
        this.value=_value;
    }
    
    public final void setValue(DataField field) {
        this.type=convertType(field.getMetadata());
        if (field.isNull())
            this.value=null;
        else
            this.value=field.getValueDuplicate();
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

    public final static TLValueType convertType(DataFieldMetadata fieldMeta) {
        switch (fieldMeta.getType()) {
        case DataFieldMetadata.INTEGER_FIELD:
            return TLValueType.INTEGER;
        case DataFieldMetadata.LONG_FIELD:
            return TLValueType.LONG;
        case DataFieldMetadata.NUMERIC_FIELD:
            return TLValueType.DOUBLE;
        case DataFieldMetadata.DECIMAL_FIELD:
            return TLValueType.DECIMAL;
        case DataFieldMetadata.DATE_FIELD:
            return TLValueType.DATE;
        case DataFieldMetadata.BYTE_FIELD:
        case DataFieldMetadata.STRING_FIELD:
            return TLValueType.STRING;
        default:
            return TLValueType.STRING;
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
        return value.toString();
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
}
