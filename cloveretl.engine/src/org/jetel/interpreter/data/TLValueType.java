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
package org.jetel.interpreter.data;

import org.jetel.metadata.DataFieldMetadata;

public enum TLValueType {
    NULL(0,"NULL",true,false,false),
    INTEGER(1,"INTEGER",true,true,false),
    LONG(2,"LONG",true,true,false),
    NUMBER(3,"NUMBER",true,true,false),
    DECIMAL(4,"DECIMAL",true,true,false),
    BYTE(5,"BYTE",true,false,true),
    STRING(6,"STRING",true,false,true),
    DATE(7,"DATE",true,false,false),
    BOOLEAN(8,"BOOLEAN",true,false,false),
    SYM_CONST(9,"SYMBOLIC_CONSTANT",true,true,false),
    LIST(20,"LIST",false,false,true),
    MAP(30,"MAP",false,false,true),
    OBJECT(40,"OBJECT",false,false,false),
    RECORD(50,"RECORD",false,false,true);
    
    private final int code;
    private final String name;
    private boolean primitive;
    private boolean numeric;
    private boolean array;
    
    TLValueType(int _code,String _name,boolean _primitive,boolean _numeric, boolean _array){
        code=_code;
        name=_name;
        primitive=_primitive;
        numeric=_numeric;
        array=_array;
    }
    
    public final int code(){return code;}
    public final String getName(){return name;}
    @Override
	public String toString(){return name;}
    public final boolean isPrimitive() {return primitive;}
    public final boolean isNumeric() {return numeric;}
    public final boolean isArray() {return array;}
    
    public boolean isCompatible(TLValueType _type) {
        if (_type == this || _type == NULL)
            return true;
        switch (this) {
        case INTEGER:
        case LONG:
        case NUMBER:
        case DECIMAL:
            if (_type.isNumeric())
                return true;
            break;
        case STRING:
            return true;
        case OBJECT:
            return true;
        case BYTE:
        	if (_type.isArray() || _type.isNumeric() || _type==STRING)
        		return true;
        	break;
        case BOOLEAN:
        	 break;
        case DATE:
        	if ( _type == LONG ) return true;
        	break;
        case MAP:
        case LIST:
        	if (_type.isArray()) return true;
        	break;
        case RECORD:
        	break; // record compatible only with record
        }
        return false;
    }

    public boolean isStrictlyCompatible(TLValueType _type) {
        if (_type == this || _type==NULL)
            return true;
        switch (this) {
        // not needed to be considered case INTEGER:
        case LONG:
            if (_type == INTEGER)
                return true;
            break;
        case NUMBER:
            if (_type == LONG || _type == INTEGER)
                return true;
            break;
        case DECIMAL:
            if (_type.isNumeric())
                return true;
            break;
        }
        return false;
    }

    public final static TLValueType convertType(DataFieldMetadata fieldMeta) {
        switch (fieldMeta.getType()) {
        case DataFieldMetadata.INTEGER_FIELD:
            return INTEGER;
        case DataFieldMetadata.LONG_FIELD:
            return LONG;
        case DataFieldMetadata.NUMERIC_FIELD:
            return NUMBER;
        case DataFieldMetadata.DECIMAL_FIELD:
            return DECIMAL;
        case DataFieldMetadata.DATE_FIELD:
            return DATE;
        case DataFieldMetadata.BYTE_FIELD:
            return BYTE;
        case DataFieldMetadata.STRING_FIELD:
            return STRING;
        case DataFieldMetadata.BOOLEAN_FIELD:
            return BOOLEAN;
        default:
            return STRING;
        }
    }

    public final static char convertType(TLValueType tlType){
    	switch (tlType) {
		case INTEGER:
			return DataFieldMetadata.INTEGER_FIELD;
		case LONG:
			return DataFieldMetadata.LONG_FIELD;
		case BOOLEAN:
			return DataFieldMetadata.BOOLEAN_FIELD;
		case BYTE:
			return DataFieldMetadata.BYTE_FIELD;
		case DATE:
			return DataFieldMetadata.DATE_FIELD;
		case DECIMAL:
			return DataFieldMetadata.DECIMAL_FIELD;
		case NUMBER: 
			return DataFieldMetadata.NUMERIC_FIELD;
		case STRING:
			return DataFieldMetadata.STRING_FIELD;
		case NULL:
			return DataFieldMetadata.NULL_FIELD;
		default:
			throw new IllegalArgumentException("Can't convert TL type " + tlType.getName() + " to clover type");
		}
    }
}
