package org.jetel.enums;

/**
 * Used for field reading.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) OpenSys (www.opensys.eu)
 */
public enum ProcessingType {
	SOURCE("source"),
	DISCRETE("discrete"),
	STREAM("stream");
	
    private String id;
    
    private ProcessingType(String id) {
        this.id = id;
    }

    public static ProcessingType fromString(String id, ProcessingType defaultValue) {
        if(id == null) return defaultValue;
        
        for(ProcessingType item : values()) {
            if(id.equalsIgnoreCase(item.id)) {
                return item;
            }
        }
        
        return defaultValue;
    }
}
