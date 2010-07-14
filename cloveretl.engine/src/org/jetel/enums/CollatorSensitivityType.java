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

import java.text.Collator;

/**
 * Used for collator sensitivity.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public enum CollatorSensitivityType {
	
	BASE_LETTER("base_letter_sensitivity", Collator.PRIMARY),
	ACCENT("accent_sensitivity", Collator.SECONDARY),
	CASE("case_sensitivity", Collator.TERTIARY),
	IDENTICAL("identical_sensitivity", Collator.IDENTICAL);
	
    private String id;
    private int collatorValue;
    
    private CollatorSensitivityType(String id, int collatorValue) {
        this.id = id;
        this.collatorValue = collatorValue;
    }

    public static CollatorSensitivityType fromString(String id, CollatorSensitivityType defaultValue) {
        if(id == null) return defaultValue;
        
        for(CollatorSensitivityType item : values()) {
            if(id.equalsIgnoreCase(item.id)) {
                return item;
            }
        }
        
        return defaultValue;
    }
    
    /**
     * Returns sensitivity name.
     * @return
     */
    public String getCollatorSensitivityName() {
    	return id;
    }
    
    /**
     * Returns collator sensitivity (Collator.PRIMARY / Collator.SECONDARY / Collator.TERTIARY / Collator.IDENTICAL).
     * @return
     */
    public int getCollatorSensitivityValue() {
    	return collatorValue;
    }
}
