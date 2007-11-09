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

import java.util.Date;

import org.jetel.data.DataField;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

@SuppressWarnings("unchecked")
public abstract class TLValue implements Comparable<TLValue>{
   

	TLValueType type;
	
    public static final TLValue NULL_VAL=TLNullValue.getInstance();
    public static final TLValue TRUE_VAL=TLBooleanValue.TRUE;
    public static final TLValue FALSE_VAL=TLBooleanValue.FALSE;
    public static final TLValue NUM_ZERO_VAL = new TLNumericValue(TLValueType.INTEGER,new CloverInteger(0));
    public static final TLValue NUM_ONE_VAL = new TLNumericValue(TLValueType.INTEGER,new CloverInteger(1)); 
    public static final TLValue NUM_MINUS_ONE_VAL = new TLNumericValue(TLValueType.INTEGER,new CloverInteger(-1)); 
    public static final TLValue NUM_PI_VAL = new TLNumericValue(TLValueType.DOUBLE,new CloverDouble(Math.PI));
    public static final TLValue NUM_E_VAL = new TLNumericValue(TLValueType.DOUBLE,new CloverDouble(Math.E));
    
    public TLValue(TLValueType type){
    	this.type=type;
    }
    
    
    public abstract void setValue(Object _value);
    
    public abstract void setValue(DataField field);
    
    public abstract Object getValue();
    
    
    //public final boolean isNull();
    
    public TLValueType getType(){
    	return type;
    }

    public static  TLValue convertValue(DataField field) {
    	if (field.isNull())
    		return NULL_VAL;
        switch(field.getMetadata().getType()) {
        case DataFieldMetadata.INTEGER_FIELD:
            return new TLNumericValue<CloverInteger>(TLValueType.INTEGER,new CloverInteger((Numeric)field));
        case DataFieldMetadata.LONG_FIELD:
            return new TLNumericValue<CloverLong>(TLValueType.LONG,new CloverLong((Numeric)field));
        case DataFieldMetadata.NUMERIC_FIELD:
            return new TLNumericValue<CloverDouble>(TLValueType.DOUBLE,new CloverDouble((Numeric)field));
        case DataFieldMetadata.DECIMAL_FIELD:
            return new TLNumericValue<Decimal>(TLValueType.DECIMAL,((Decimal)field).createCopy());
        case DataFieldMetadata.DATE_FIELD:
           return new TLDateValue((Date)field.getValueDuplicate());
    /*    case DataFieldMetadata.BYTE_FIELD:
        	return new TLValue(TLValueType.BYTE,field.getValue());*/
        case DataFieldMetadata.STRING_FIELD:
            return new TLStringValue((String)field.getValueDuplicate());
        default:
            throw new IllegalArgumentException("Don't know how to convert "+field.getType());
        
        }
    }
    
    public abstract void copyToDataField(DataField field);    
    
    public abstract TLValue duplicate();
    
    public static TLValue create(TLValueType type) {
        switch (type) {
        case INTEGER:
        case DOUBLE:
        case LONG:
        case DECIMAL:
            return new TLNumericValue(type);
        case DATE:
            return new TLDateValue();
        case STRING:
            return new TLStringValue();
        case BYTE:
        	return null; //TLValue(type, new ByteArray());
        case BOOLEAN:
            return TLValue.FALSE_VAL;
        case LIST:
            return new TLListValue();
        case MAP:
            return new TLMapValue();
        default:
            throw new IllegalArgumentException(
                    "Can't create value type: " + type);
        }
    }


	public abstract int compareTo(TLValue arg0);
    
	public abstract String toString(); 
	
}
