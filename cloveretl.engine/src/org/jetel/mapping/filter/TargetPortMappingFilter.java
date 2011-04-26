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
public class TargetPortMappingFilter extends MappingFilterAdapter {
	private String portName;
	
	/**
	 * @param portName port name which is used to filter out the mapping source with different port names
	 */
	public TargetPortMappingFilter(String portName) {
		this.portName = portName;
	}
	
	/* (non-Javadoc)
	 * @see com.initiatesystems.etl.formatter.mapping.MappingFilterAdapter#checkSource(com.initiatesystems.etl.formatter.mapping.MappingSource)
	 */
	@Override
	public boolean checkTarget(MappingTarget target) {
		if(target instanceof CloverMappingElement) {
			CloverMappingElement cloverSource = (CloverMappingElement) target;
			return cloverSource.getPortName().equals(portName);
			
		} else {
			return false;
		}
	}
}