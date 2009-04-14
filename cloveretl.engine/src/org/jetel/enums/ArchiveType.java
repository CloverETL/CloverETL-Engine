package org.jetel.enums;

/**
 * Used for file utils.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) OpenSys (www.opensys.eu)
 */
public enum ArchiveType {
	
	// archive types
	ZIP("zip"),
	TAR("tar"),
	GZIP("gzip");

	private String id;
    
    /**
     * Constructor.
     * @param id
     */
    private ArchiveType(String id) {
        this.id = id;
    }

    /**
     * Gets archive type.
     * @param id
     * @return
     */
    public static ArchiveType fromString(String id) {
        if(id == null) return null;
        
        for(ArchiveType item : values()) {
            if(id.equalsIgnoreCase(item.id)) {
                return item;
            }
        }
        
        return null;
    }
    
    public String getId() {
    	return id;
    }
}
