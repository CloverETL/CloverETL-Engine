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
package org.jetel.util.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.jetel.plugin.PluginClassLoader;

/**
 * Utility to create a "union" of class loaders.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.8.2012
 */
public class MultiParentClassLoader extends ClassLoader {

	private final ClassLoader parents[];
	
	/**
	 * Creates new multi-parent class loader. The class is searched on parents in the same
	 * order as provided to the constructor.
	 * @param parents
	 */
	public MultiParentClassLoader(ClassLoader ... parents) {
		if (parents == null) {
			throw new NullPointerException("parents");
		}
		this.parents = parents;
	}
	
	public URL[] getAllURLs() {
		
		Set<URL> urls = new LinkedHashSet<URL>();
		for (ClassLoader parent : parents) {
			if (parent instanceof PluginClassLoader) {
				PluginClassLoader pcl = (PluginClassLoader)parent;
				urls.addAll(Arrays.asList(pcl.getAllURLs()));
			} else if (parent instanceof URLClassLoader) {
				URLClassLoader ucl = (URLClassLoader)parent;
				urls.addAll(Arrays.asList(ucl.getURLs()));
			}
		}
		return urls.toArray(new URL[urls.size()]);
	}
	
	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		
		for (ClassLoader parent : parents) {
			try {
				return parent.loadClass(name);
			} catch (ClassNotFoundException e) {
				// ignore
			}
		}
		throw new ClassNotFoundException(name);
	}
}
