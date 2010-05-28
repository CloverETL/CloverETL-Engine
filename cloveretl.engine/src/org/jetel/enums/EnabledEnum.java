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
package org.jetel.enums;


/**
 * This enum expresses state of component - enabled, disabled or pass through component state.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 5.4.2007
 */
public enum EnabledEnum {
   
    ENABLED("enabled"),
    DISABLED("disabled"),
    PASS_THROUGH("passThrough");
    
    private String id;
    
    private EnabledEnum(String id) {
        this.id = id;
    }
    
    public static EnabledEnum fromString(String id) {
        return fromString(id, null);
    }
    
    public static EnabledEnum fromString(String id, EnabledEnum defaultValue) {
        if(id == null) return defaultValue;
        
        for(EnabledEnum item : values()) {
            if(id.equalsIgnoreCase(item.id)) {
                return item;
            }
        }
        
        return defaultValue;
    }
    
    public String toString() {
        return id;
    }
    
}
