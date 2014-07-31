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
package org.jetel.plugin;

import java.net.URL;

/**
 * This class represent a location of a clover engine plugin. Internally is implemented by
 * a {@link URL} of plugin repository and optional class loader which should be used
 * as default class loader of all plugins placed in the repository.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.1.2011
 */
public class PluginLocation {

	private URL location;
	
	private ClassLoader classloader;
	
	public PluginLocation(URL location) {
		this(location, null);
	}

	public PluginLocation(URL location, ClassLoader classloader) {
		this.location = location;
		this.classloader = classloader;
	}

	public URL getLocation() {
		return location;
	}

	public ClassLoader getClassloader() {
		return classloader;
	}
	
	@Override
	public String toString() {
		return "PluginLocation<"+getLocation().toString()+">";
	}
}
