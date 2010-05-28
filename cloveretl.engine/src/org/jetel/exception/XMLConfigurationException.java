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
package org.jetel.exception;

/**
 * Exception thrown when graph object deserialization from
 * XML fails for any reason.<br>
 * Mostly thrown from within <code>GraphElement.fromXML()</code> method.
 * 
 * @author david
 * @since  21.7.2006
 * 
 */
public class XMLConfigurationException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
     * 
     */
    public XMLConfigurationException() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     */
    public XMLConfigurationException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param message
     * @param cause
     */
    public XMLConfigurationException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param cause
     */
    public XMLConfigurationException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }

}
