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

import org.jetel.mapping.MappingSource;
import org.jetel.mapping.element.CloverMappingElement;
import org.jetel.mapping.filter.MappingFilterAdapter;


/**
 * Mapping filter. Only mapping assignment with template 'clover.field --> initiate.field' 
 * with the given port name remains.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class SourceCloverMappingFilter extends MappingFilterAdapter {

	/* (non-Javadoc)
	 * @see com.initiatesystems.etl.formatter.mapping.MappingFilterAdapter#checkSource(com.initiatesystems.etl.formatter.mapping.MappingSource)
	 */
	@Override
	public boolean checkSource(MappingSource source) {
		return source instanceof CloverMappingElement;
	}
}