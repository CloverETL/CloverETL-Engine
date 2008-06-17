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

import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DecimalDataField;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

@SuppressWarnings("unchecked")
public abstract class TLValue implements Comparable<TLValue>{
   

	public final TLValueType type;
	
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
    
    public abstract void setValue(TLValue _value);
    
    public abstract void setValue(DataField field);
    
    public abstract Object getValue();
    
    
    public Date getDate(){
    	throw new UnsupportedOperationException();
    }
    
    public Numeric getNumeric(){
    	throw new UnsupportedOperationException();
    }
    
    /*public final boolean isNull(){
    	return (this instanceof TLNullValue);
    }*/
    
    public TLValueType getType(){
    	return type;
    }

    public static  TLValue convertValue(DataField field) {
    	TLValue newval=null;;
    	if (field.isNull())
    		return NULL_VAL;
        switch(field.getMetadata().getType()) {
        case DataFieldMetadata.INTEGER_FIELD:
        	newval= new TLNumericValue<CloverInteger>(TLValueType.INTEGER,new CloverInteger((Numeric)field));
        	break;
        case DataFieldMetadata.LONG_FIELD:
        	newval = new TLNumericValue<CloverLong>(TLValueType.LONG,new CloverLong((Numeric)field));
        	break;
        case DataFieldMetadata.NUMERIC_FIELD:
        	newval = new TLNumericValue<CloverDouble>(TLValueType.DOUBLE,new CloverDouble((Numeric)field));
        	break;
        case DataFieldMetadata.DECIMAL_FIELD:
        	newval= new TLNumericValue<Decimal>(TLValueType.DECIMAL,(Decimal)((DecimalDataField)field).getValueDuplicate());
        	break;
        case DataFieldMetadata.DATE_FIELD:
        	newval= new TLDateValue((Date)field.getValueDuplicate());
        	break;
        case DataFieldMetadata.BOOLEAN_FIELD:
        	newval = TLBooleanValue.FALSE;
        	break;
       case DataFieldMetadata.BYTE_FIELD:
    	   	newval = new TLByteArrayValue(((ByteDataField)field).getByteArray());
        	break;
        case DataFieldMetadata.STRING_FIELD:
        	newval= new TLStringValue((StringBuilder)field.getValueDuplicate());
        	break;
        default:
            throw new IllegalArgumentException("Don't know how to convert "+field.getType());
        
        }
        newval.setValue(field);
        return newval;
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
        	return new TLByteArrayValue();
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
    
	@Override public abstract String toString(); 
	
}
