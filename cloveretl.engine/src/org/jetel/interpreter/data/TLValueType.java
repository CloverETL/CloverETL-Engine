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

public enum TLValueType {
    NULL(0,"NULL",true,false),
    INTEGER(1,"INTEGER",true,true),
    LONG(2,"LONG",true,true),
    DOUBLE(3,"DOUBLE",true,true),
    DECIMAL(4,"DECIMAL",true,true),
    STRING(5,"STRING",true,false),
    DATE(6,"DATE",true,false),
    BOOLEAN(7,"BOOLEAN",true,false),
    LIST(20,"LIST",false,false),
    MAP(30,"MAP",false,false),
    OBJECT(40,"OBJECT",false,false),
    RECORD(50,"RECORD",false,false);
    
    private final int code;
    private final String name;
    private boolean primitive;
    private boolean numeric;
    
    TLValueType(int _code,String _name,boolean _primitive,boolean _numeric){
        code=_code;
        name=_name;
        primitive=_primitive;
        numeric=_numeric;
    }
    
    public int code(){return code;}
    public String getName(){return name;}
    public String toString(){return name;}
    public boolean isPrimitive() {return primitive;}
    public boolean isNumeric() {return numeric;}
    
    public boolean isCompatible(TLValueType _type) {
        if (_type == this)
            return true;
        switch (this) {
        case INTEGER:
        case LONG:
        case DOUBLE:
        case DECIMAL:
            if (_type!=NULL && _type.isNumeric())
                return true;
        case STRING:
            return true;

        case OBJECT:
            return true;
        }
        if (_type==NULL) return true;
        return false;
    }

    public boolean isStrictlyCompatible(TLValueType _type) {
        if (_type == this)
            return true;
        switch (this) {
        // not needed to be considered case INTEGER:
        case LONG:
            if (_type == INTEGER)
                return true;
        case DOUBLE:
            if (_type == LONG || _type == INTEGER)
                return true;
        case DECIMAL:
            if (_type!=NULL && _type.isNumeric())
                return true;
        }
        if (_type==NULL) return true;
        return false;
    }

}
