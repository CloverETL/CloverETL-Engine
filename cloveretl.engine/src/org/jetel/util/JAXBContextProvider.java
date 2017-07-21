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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.jetel.exception.JetelRuntimeException;

/**
 * JAXB contexts cache to prevent class leaks caused by their repeated creation,
 * see https://issues.apache.org/jira/browse/CXF-2939
 * Anyway, JAXBContext is a heavy-weight object - it should be cached.
 * 
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.1.2013
 */
public class JAXBContextProvider {

	private static final JAXBContextProvider instance = new JAXBContextProvider();
	
	private Map<ContextKey, JAXBContext> cache = new HashMap<ContextKey, JAXBContext>();
	
	private JAXBContextProvider() {}
	
	public static JAXBContextProvider getInstance() {
		return instance;
	}
	
	/**
	 * Use this instead of {@link JAXBContext#newInstance(Class...)}
	 * @param types
	 * @return
	 * @throws JAXBException
	 */
	public JAXBContext getContext(Class<?> ... types) throws JAXBException {
		
		return getContext(new ContextKey(types));
	}
	
	/**
	 * Use this instead of {@link JAXBContext#newInstance(String, ClassLoader)}
	 * @param types
	 * @return
	 * @throws JAXBException
	 */
	public JAXBContext getContext(String contextPath, ClassLoader classLoader) throws JAXBException {
		
		return getContext(new ContextKey(contextPath, classLoader));
	}
	
	/**
	 * Use this instead of {@link JAXBContext#newInstance(String)}
	 * @param contextPath
	 * @return
	 * @throws JAXBException
	 */
	public JAXBContext getContext(String contextPath) throws JAXBException {
		
		return getContext(new ContextKey(contextPath, null));
	}
	
	private synchronized JAXBContext getContext(ContextKey key) throws JAXBException {
		
		JAXBContext ctx = cache.get(key);
		if (ctx == null) {
			ctx = key.createContext();
			cache.put(key, ctx);
		}
		return ctx;
	}
	
	private static class ContextKey {
		
		private Set<Class<?>> types;
		private String contextPath;
		private ClassLoader classLoader;
		
		public ContextKey(Class<?> ... types) {
			this.types = new HashSet<Class<?>>(Arrays.asList(types));
		}
		
		public ContextKey(String contextPath, ClassLoader classLoader) {
			this.contextPath = contextPath;
			this.classLoader = classLoader;
		}
		
		public JAXBContext createContext() throws JAXBException {
			if (types != null) {
				return JAXBContext.newInstance(types.toArray(new Class<?>[types.size()]));
			} if (contextPath != null) {
				if (classLoader != null) {
					return JAXBContext.newInstance(contextPath, classLoader);
				} else {
					return JAXBContext.newInstance(contextPath);
				}
			} else {
				throw new JetelRuntimeException("Invalid arguments for context creation.");
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((classLoader == null) ? 0 : classLoader.hashCode());
			result = prime * result + ((contextPath == null) ? 0 : contextPath.hashCode());
			result = prime * result + ((types == null) ? 0 : types.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ContextKey other = (ContextKey) obj;
			if (classLoader == null) {
				if (other.classLoader != null)
					return false;
			} else if (!classLoader.equals(other.classLoader))
				return false;
			if (contextPath == null) {
				if (other.contextPath != null)
					return false;
			} else if (!contextPath.equals(other.contextPath))
				return false;
			if (types == null) {
				if (other.types != null)
					return false;
			} else if (!types.equals(other.types))
				return false;
			return true;
		}
	}
}
