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

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * {@link ClassLoader} to allow filtering of parent delegated class loading.
 * By default all java.*, javax.* and sun.* packages are delegated.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.9.2014
 */
public class FilterClassLoader extends ClassLoader {
	
	private SortedMap<String, Boolean> filters = new TreeMap<>();
	
	public FilterClassLoader(ClassLoader parent) {
		super(parent);
		addIncludedPackages(getDefaultInclusions());
	}
	
	@Override
	protected Class<?> loadClass(String className, boolean resolveClass) throws ClassNotFoundException {
		if (proceed(className)) {
			return super.loadClass(className, resolveClass);
		} else {
			throw new ClassNotFoundException(className);
		}
	}
	
	/**
	 * Adds list of packages that will be not be loaded from parent class loader. 
	 * @param pkgs
	 */
	public void addExcludedPackages(String ... pkgs) {
		synchronized (filters) {
			for (String pkg : pkgs) {
				String pkgStart = pkg + '.';
				filters.put(pkgStart, Boolean.FALSE);
			}
		}
	}
	
	/**
	 * Adds list of packages that will be loaded from parent class loader.
	 * @param pkgs
	 */
	public void addIncludedPackages(String ... pkgs) {
		synchronized (filters) {
			for (String pkg : pkgs) {
				String pkgStart = pkg + '.';
				filters.put(pkgStart, Boolean.TRUE);
			}
		}
	}
	
	protected String[] getDefaultInclusions() {
		return new String[] {"java", "javax", "sun"};
	}
	
	protected boolean proceed(final String className) {
		
		synchronized (filters) {
			boolean load = false;
			for (Entry<String, Boolean> entry : filters.headMap(className).entrySet()) {
				if (!className.startsWith(entry.getKey())) { // XXX optimize me
					continue;
				}
				load = entry.getValue().booleanValue();
			}
			return load;
		}
	}
}
