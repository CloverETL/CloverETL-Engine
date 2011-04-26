package org.jetel.mapping.filter;

import org.jetel.mapping.MappingTarget;
import org.jetel.mapping.element.CloverMappingElement;


/**
 * Mapping filter. Only mapping assignment with template 'clover.field --> initiate.field' 
 * with the given port name remains.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class TargetCloverMappingFilter extends MappingFilterAdapter {

	/* (non-Javadoc)
	 * @see com.initiatesystems.etl.formatter.mapping.MappingFilterAdapter#checkSource(com.initiatesystems.etl.formatter.mapping.MappingSource)
	 */
	@Override
	public boolean checkTarget(MappingTarget target) {
		return target instanceof CloverMappingElement;
	}
}