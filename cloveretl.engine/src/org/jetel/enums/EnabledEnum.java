/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.enums;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EnabledEnum {
    private String id;
    private static List elements = new ArrayList();
    
    private EnabledEnum(String id) {
        this.id = id;
        elements.add(this);
    }
    
    public String toString() {
        return id;
    }

    public static EnabledEnum fromString(String id) {
        for(Iterator it = elements.iterator(); it.hasNext();) {
            EnabledEnum ee = (EnabledEnum) it.next();
            if(ee.toString().equalsIgnoreCase(id)) return ee; 
        }
        return null;
    }
    
    public static final EnabledEnum ENABLED = new EnabledEnum("enabled");
    public static final EnabledEnum DISABLED = new EnabledEnum("disabled");
    public static final EnabledEnum PASS_THROUGH = new EnabledEnum("passThrough");
}
