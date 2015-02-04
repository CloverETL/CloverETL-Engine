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
package org.jetel.exception;

/**
 * This enum represents type of policy as reaction on BadDataFormatException in parses.
 * @see org.jetel.exception.IParserExceptionHandler
 * @see org.jetel.data.parser.Parser
 * 
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public enum PolicyType {

    STRICT, CONTROLLED, LENIENT;
    
    public static PolicyType valueOfIgnoreCase(String strPolicy) {
        if (strPolicy == null) {
            return STRICT; //default value
        }
        
        for (PolicyType pt : PolicyType.values()) {
            if (strPolicy.equalsIgnoreCase(pt.toString())) {
                return pt;
            }
        }
        
        throw new JetelRuntimeException("Unknown policy type: " + strPolicy);
    }
    
    public static boolean isPolicyType(String strPolicy) {
    	if (strPolicy == null) {
    		return true;
    	} else {
            for (PolicyType pt : PolicyType.values()) {
                if (strPolicy.equalsIgnoreCase(pt.toString())) {
                    return true;
                }
            }
            return false;
    	}
    }
    
}
