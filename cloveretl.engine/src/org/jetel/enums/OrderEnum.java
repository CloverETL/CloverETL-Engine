package org.jetel.enums;


/**
 * Order Enum.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 * (c) OpenSys (www.opensys.eu)
 */
public enum OrderEnum {
	ASC("a", "ascending"),	// ascending order
	DESC("d", "descending"),// descending order
	IGNORE("i", "ignore"),	// don't check order of records
	AUTO("r", "auto");	    // check the input to be either in ascending or descending order

	String shortCut;
	String description;

	OrderEnum(String shortCut, String description) {
		this.description = description;
		this.shortCut = shortCut;
	}

	public static OrderEnum fromString(String id) {
		return fromString(id, null);
	}
	
	public static OrderEnum fromString(String id, OrderEnum defaultValue) {
        if(id == null) return defaultValue;
        
        for(OrderEnum item : values()) {
            if(id.equalsIgnoreCase(item.shortCut)) {
                return item;
            }
        }
        return defaultValue;
	}

	public String getShortCut() {
		return shortCut;
	}
	
	public String getDescription() {
		return description;
	}
	
}
