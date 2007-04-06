/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
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
