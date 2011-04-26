/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */
package org.jetel.mapping;


/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 12.3.2008
 */
public interface IMappingElementProvider {

	/**
	 * Creates source mapping element.
	 * 
	 * @param rawSource - source string
	 * @return source mapping element instance
	 * @throws MappingException
	 */
	public MappingSource createMappingSource(String rawSource) throws MappingException;

	/**
	 * Creates target mapping element.
	 * 
	 * @param rawTarget - target string
	 * @return target mapping element instance
	 * @throws MappingException
	 */
	public MappingTarget createMappingTarget(String rawTarget) throws MappingException;

}
