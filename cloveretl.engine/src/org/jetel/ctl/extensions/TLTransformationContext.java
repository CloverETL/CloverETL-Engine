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
package org.jetel.ctl.extensions;

import java.util.HashMap;
import java.util.Map;

/**
 * Global transformation context shared
 * between all function calls from the same transformation.
 * 
 * Accessible via {@link TLFunctionCallContext#getTransformationContext()}.
 * 
 * Introduced to solve CLO-722;
 * used to store DataGenerator instances 
 * instead of the previously used static HashMap.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14. 11. 2013
 */
public class TLTransformationContext {
	
	private Map<Object, Object> cache = new HashMap<Object, Object>(5);
	
	public Object getCachedObject(Object key) {
		return cache.get(key);
	}
	
	public Object setCachedObject(Object key, Object value) {
		return cache.put(key, value);
	}

}
