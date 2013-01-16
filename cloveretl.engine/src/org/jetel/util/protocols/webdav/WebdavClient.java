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
package org.jetel.util.protocols.webdav;

import java.io.IOException;

import com.googlecode.sardine.DavResource;
import com.googlecode.sardine.Sardine;

/**
 * Extends the {@link Sardine} interface with some useful methods.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 10, 2013
 */
public interface WebdavClient extends Sardine {

	/**
	 * Execute PROPFIND with depth 0.
	 * 
	 * @param url
	 * @return
	 * @throws IOException
	 */
	public DavResource info(String url) throws IOException;
	
	/**
	 * Check whether remote directory exists.
	 * 
	 * @param url
	 *            Path to the directory.
	 * @return True if the directory exists.
	 * @throws IOException
	 */
	public boolean dirExists(String url) throws IOException;
}
