package org.jetel.enums;

import java.text.Collator;

/**
 * Used for collator sensitivity.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) OpenSys (www.opensys.eu)
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
