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
package org.jetel.connection.jdbc.driver;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import org.apache.commons.io.IOUtils;
import org.jetel.database.sql.JdbcDriver;
import org.jetel.util.classloader.ClassDefinitionFactory;

/**
 * This class is used to deregister drivers from 
 * {@link DriverManager}. Make sure this class is loaded
 * by the same classloader, which was used to load
 * the driver.
 * 
 * @see JdbcDriver#free()
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29.9.2011
 */
public class DriverUnregisterer {
	
	/* Cannot to reference log4j here, for it is not on the classpath of the classloader in most cases
	private static final Logger log = Logger.getLogger(DriverUnregisterer.class); 
	*/
	
	public static void unregisterDrivers(ClassLoader classLoader) {
		
		// unload each driver loaded by the given classLoader
		for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements();) {
			Driver driver = e.nextElement();
			if (driver.getClass().getClassLoader() == classLoader) {
				try {
					DriverManager.deregisterDriver(driver);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	public static void run(ClassLoader loader) throws Throwable {
		if (loader instanceof ClassDefinitionFactory) {
			ClassDefinitionFactory factory = (ClassDefinitionFactory) loader;
			final ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
			try {
				Thread.currentThread().setContextClassLoader(DriverUnregisterer.class.getClassLoader()); // prevents CLO-4787
				// get DriverUnregisterer's code, load it using driver's class loader and perform deregistration
				InputStream classData = DriverUnregisterer.class.getResourceAsStream(DriverUnregisterer.class.getSimpleName().concat(".class"));
				byte classBytes[] = IOUtils.toByteArray(classData);
				IOUtils.closeQuietly(classData);
				Class<?> unregisterer = factory.defineClass(DriverUnregisterer.class.getName(), classBytes);
				Method unregister = unregisterer.getMethod("unregisterDrivers", ClassLoader.class);
				unregister.invoke(null, loader);
			} finally {
				Thread.currentThread().setContextClassLoader(originalLoader);
			}
		}
	}
}