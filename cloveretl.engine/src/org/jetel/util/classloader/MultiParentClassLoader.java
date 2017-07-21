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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.jetel.plugin.PluginClassLoader;
import org.jetel.util.CompoundEnumeration;

/**
 * Utility to create a "union" of class loaders.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.8.2012
 */
public final class MultiParentClassLoader extends ClassLoader {

	public static final ClassLoader NULL_CLASS_LOADER = new ClassLoader(null) {
		
		@Override
		protected Class<?> loadClass(String className, boolean resolveClass) throws ClassNotFoundException {
			throw new ClassNotFoundException(className);
		}
		
		@Override
		public URL getResource(String resName) {
			return null;
		}
		
		@Override
		public Enumeration<URL> getResources(String resName) throws IOException {
			return Collections.emptyEnumeration();
		}
	};
	
	private final ClassLoader parents[];
	
	/**
	 * Creates new multi-parent class loader. The class is searched on parents in the same
	 * order as provided to the constructor.
	 * @param parents
	 */
	public MultiParentClassLoader(ClassLoader ... parents) {
		super(NULL_CLASS_LOADER);
		if (parents == null) {
			throw new NullPointerException("parents");
		}
		this.parents = parents; 
	}
	
	public ClassLoader[] getParents() {
		return parents.clone();
	}
	
	@SuppressWarnings("resource")
	public URL[] getAllURLs() {
		
		Set<URL> urls = new LinkedHashSet<URL>();
		for (ClassLoader parent : parents) {
			if (parent instanceof PluginClassLoader) {
				PluginClassLoader pcl = (PluginClassLoader)parent;
				urls.addAll(Arrays.asList(pcl.getAllURLs()));
			} else if (parent instanceof URLClassLoader) {
				URLClassLoader ucl = (URLClassLoader)parent;
				urls.addAll(Arrays.asList(ucl.getURLs()));
			} else {
				try {
					Method getAllURLs = parent.getClass().getMethod("getAllURLs");
					Object loaderUrls = getAllURLs.invoke(parent);
					if (loaderUrls instanceof URL[]) {
						urls.addAll(Arrays.asList((URL[])loaderUrls));
					}
				} catch (Exception e) {
					// ignore
				}
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
	
	@Override
	protected URL findResource(String resName) {
		for (ClassLoader parent : parents) {
			URL url = parent.getResource(resName);
			if (url != null) {
				return url;
			}
		}
		return null;
	}
	
	@Override
	protected Enumeration<URL> findResources(String resName) throws IOException {
		List<Enumeration<URL>> enums = new LinkedList<Enumeration<URL>>();
		for (ClassLoader parent : parents) {
			enums.add(parent.getResources(resName));
		}
		return new CompoundEnumeration<URL>(enums);
	}
	
}
