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
 * This enumeration represents all possible values of 'enable' attribute of components.
 * Actually the enum has only two meaningful values true (ENABLED and TRUE)
 * and false (DISABLED, PASS_THROUGH and FALSE).
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 5.4.2007
 */
public enum EnabledEnum {
   
    ENABLED("enabled", true),
    DISABLED("disabled", false),
    PASS_THROUGH("passThrough", false), //deprecated - use DISABLED or FALSE
    TRUE("true", true),
    FALSE("false", false);
    
    private String id;
    
    private boolean enabled;
    
    private EnabledEnum(String id, boolean enabled) {
        this.id = id;
        this.enabled = enabled;
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
    
    /**
     * @return true if a component with this value is kept enabled, false 
     *  if a component with this value is removed from graph
     */
    public boolean isEnabled() {
    	return enabled;
    }
    
    @Override
	public String toString() {
        return id;
    }
    
}
