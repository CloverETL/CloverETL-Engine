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

/** A class that represents exception indicating misconfigured Graph.<br>
 * Circular references between components, wrong phases assignment, etc.
 * 
 * @see Exception
 * @author D.Pavlis
 */
public class GraphConfigurationException extends Exception {
  
	private static final long serialVersionUID = -6907821049378514420L;
	
  // Attributes

  // Associations


// Operations
  public GraphConfigurationException(String message){
	  super(message);
  }

} /* end class GraphConfigurationException */
