package org.jetel.mapping.filter;

import org.jetel.mapping.MappingAssignment;
import org.jetel.mapping.MappingSource;
import org.jetel.mapping.MappingTarget;

/**
 * A adapter implementation of MappingFilter interface.
 * This filter keeps all mapping assignments in the filtered mapping.
 * 
 * @author Martin Zatopek (martin.zatopek@opensys.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 7.8.2007
 */
public class MappingFilterAdapter implements MappingFilter {

	public boolean checkAssignment(MappingAssignment assignment) {
		return true;
	}

	public boolean checkSource(MappingSource source) {
		return true;
	}

	public boolean checkTarget(MappingTarget target) {
		return true;
	}

}
