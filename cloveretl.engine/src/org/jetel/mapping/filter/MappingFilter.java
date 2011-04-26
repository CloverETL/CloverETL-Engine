package org.jetel.mapping.filter;

import org.jetel.mapping.MappingAssignment;
import org.jetel.mapping.MappingSource;
import org.jetel.mapping.MappingTarget;


/**
 * The mapping filter interface can be used in Mapping.subMapping() method 
 * to filter out the unnecessary mapping assignments.
 * 
 * @author Martin Zatopek (martin.zatopek@opensys.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 7.8.2007
 */
public interface MappingFilter {

	/**
     * This method validates a mapping assignment. 
	 * @param assignment a checked mapping assignment
	 * @return false if the assignment should be filtred out, else true
	 */
	public boolean checkAssignment(MappingAssignment assignment);
	
	/**
     * This method validates a mapping source. 
	 * @param source a checked mapping source
     * @return false if the assignment with the given mapping source should be filtred out, else true
	 */
	public boolean checkSource(MappingSource source);
	
    /**
     * This method validates a mapping target. 
     * @param target a checked mapping target
     * @return false if the assignment with the given mapping target should be filtred out, else true
     */
	public boolean checkTarget(MappingTarget target);
	
}
