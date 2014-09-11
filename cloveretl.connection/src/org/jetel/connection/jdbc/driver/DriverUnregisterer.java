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

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import org.jetel.database.sql.JdbcDriver;

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
	
	public void unregisterDrivers(ClassLoader classLoader) {
		
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
}