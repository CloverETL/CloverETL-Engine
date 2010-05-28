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
package org.jetel.plugin;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents extension's parameter. It could be only string value, although now is possible
 * to assign to one key array of values.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 29.5.2007
 */
public class ExtensionParameter {

    private List<String> values;

    public ExtensionParameter() {
        this.values = new ArrayList<String>();
    }
    
    public ExtensionParameter(String value) {
        this.values = new ArrayList<String>();
        this.values.add(value);
    }
    
    public boolean isEmpty() {
        return values.isEmpty();
    }
    
    public void addValue(String value) {
        values.add(value);
    }
    
    public List<String> getValues() {
        return new ArrayList<String>(values);
    }
    
    public String getString() {
        if(values.size() > 0 ) {
            return values.get(0);
        } else {
            return null;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        
        for(String value : values) {
            ret.append(value);
            ret.append(',');
        }
        
        ret.setLength(ret.length() -1);
        return ret.toString();
    }
}
