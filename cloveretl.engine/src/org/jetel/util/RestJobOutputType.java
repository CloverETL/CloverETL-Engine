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
package org.jetel.util;

import org.jetel.util.string.StringUtils;

/**
 * @author jedlickad (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 11. 2017
 */
public enum RestJobOutputType {

	CSV("FLAT_FILE_WRITER", false), JSON("JSON_WRITER", true), XML("EXT_XML_WRITER", true);
	
	private String writerComponent;
	private boolean mappingRequired;
	
	private RestJobOutputType(String writerComponent, boolean mappingRequired) {
		this.writerComponent = writerComponent;
		this.mappingRequired = mappingRequired;
	}
	
	public static RestJobOutputType fromString(String format) {
		if (!StringUtils.isEmpty(format)) {
			for (RestJobOutputType type : values()) {
				if(type.name().equals(format)) {
					return type;
				}
			}
		}
		return null;
	}
	
	public String getWriterComponent() {
		return this.writerComponent;
	}
	
	public boolean isMappingRequired() {
		return this.mappingRequired;
	}
}
