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

import org.jetel.ctl.extensions.MappingLib.Mapping;

/**
 * 
 * @author david (david.krska@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24. 3. 2015
 */

public class TLMappingCache extends TLCache {

	private String cachedMappingCode = null;
	private Mapping cachedMapping = null;

	public TLMappingCache(TLFunctionCallContext context, int position) {
		if(context.getLiteralsSize() > position && context.isLiteral(position)){
			cachedMappingCode = (String) context.getParamValue(position);
			cachedMapping = new Mapping(cachedMappingCode);
		}
	}

	
	public Mapping getCachedMapping(TLFunctionCallContext context, String mappingCode) {

		if (context.isLiteral(0) || (cachedMappingCode != null && mappingCode.equals(cachedMappingCode))) {
			return cachedMapping; 
		} else {
			cachedMappingCode = mappingCode;
			cachedMapping = new Mapping(mappingCode);
			return cachedMapping;
		}
	}
	
}
