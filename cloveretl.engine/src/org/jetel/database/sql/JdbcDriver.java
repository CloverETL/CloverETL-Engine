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
package org.jetel.database.sql;

import java.sql.Driver;
import java.util.Properties;

/**
 * This class represents a JDBC driver. Can be create based on JDBC driver description or by all
 * necessary attributes. Method getDriver() is root knowledge of this class.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 14.9.2007
 */
public interface JdbcDriver {

	/**
     * @return identifier of this jdbc driver. It is usually name of targeted database.
     */
    public String getDatabase();

    /**
     * @return human readable name of jdbc driver.
     */
    public String getName();

    /**
     * @return jdbc specific associated by default with this jdbc driver
     */
    public JdbcSpecific getJdbcSpecific();
    
    /**
     * @return custom connection properties
     */
    public Properties getProperties();
    
    /**
     * @return class loader, which is used to create java.sql.Driver instance, created in getDriver() method
     */
    public ClassLoader getClassLoader();
    
    /**
     * That is major method of this class. Returns a java.sql.Driver represented by this entity.
     */
    public Driver getDriver();

    /**
     * Explicitly removes drivers loaded by classLoader created by this instance.
     * Prevents from "java.lang.OutOfMemoryError: PermGen space"
     * 
     * If the driver loads any native libraries,
     * the libraries are unloaded when the driver's classloader
     * is garbage collected. The classloader can only be GCed
     * when classes loaded by it are no longer referenced.
     * 
     * @see DriverUnregisterer
     */
	public void free();
	
}
