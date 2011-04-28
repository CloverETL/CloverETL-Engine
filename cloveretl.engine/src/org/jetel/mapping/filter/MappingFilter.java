/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
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
