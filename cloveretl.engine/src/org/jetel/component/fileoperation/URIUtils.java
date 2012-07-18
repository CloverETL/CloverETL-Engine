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
package org.jetel.component.fileoperation;

import java.net.URI;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.3.2012
 */
public class URIUtils {
	
	public static final String PATH_SEPARATOR = "/"; //$NON-NLS-1$

	public static final String CURRENT_DIR_NAME = "."; //$NON-NLS-1$
	
	public static final String PARENT_DIR_NAME = ".."; //$NON-NLS-1$
	
	

	public static URI getChildURI(URI parentDir, String name) {
		String uriString = parentDir.toString();
		if (!uriString.endsWith(PATH_SEPARATOR)) {
			parentDir = URI.create(uriString + PATH_SEPARATOR);
		}
		return parentDir.resolve(name);
	}
	
	public static URI getParentURI(URI uri) {
		return uri.toString().endsWith(PATH_SEPARATOR) ? uri.resolve(PARENT_DIR_NAME) : uri.resolve(CURRENT_DIR_NAME);
	}
	
	public static String getFileName(URI uri) {
		String uriString = uri.toString();
		return uriString.substring(uriString.lastIndexOf('/') + 1);
	}

}
