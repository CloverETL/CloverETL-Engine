package org.jetel.enums;


public enum EdgeTypeEnum {
	/**  Proxy represents Direct Edge */
	DIRECT("DIRECT"),
	/**  Proxy represents Direct Edge fast propagate */
	DIRECT_FAST_PROPAGATE("DIRECTFASTPROPAGATE"),
	/**  Proxy represents Buffered Edge */
	BUFFERED("BUFFERED"),
	/** Proxy represents Edge connecting two different phases */
	PHASE_CONNECTION("PHASECONNECTION");
	
	String name;
	
	EdgeTypeEnum(String name){
		this.name = name;
	}
	
	public static EdgeTypeEnum valueOfIgnoreCase(String edgeTypeName) {
        if(edgeTypeName == null) {
            return null;
        }
        for(EdgeTypeEnum et : EdgeTypeEnum.values()) {
            if(edgeTypeName.equalsIgnoreCase(et.name)) {
                return et;
            }
        }
        return null;
	}
}	

