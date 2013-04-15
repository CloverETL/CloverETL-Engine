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
package org.jetel.component.validator;

import java.util.List;

import org.jetel.data.lookup.LookupTable;
import org.jetel.util.property.PropertyRefResolver;

/**
 * Wrapper around graph to provide some of its parts.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 24.3.2013
 */
public interface GraphWrapper {

	public List<String> getLookupTables();
	
	public LookupTable getLookupTable(String name);
	
	public PropertyRefResolver getRefResolver();
	
	public void initParentTable(ValidationGroup root);
	
	public List<String> getNodePath(ValidationNode needle);
}
