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
package org.jetel.logger;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Factory for creating safe logger for given class.
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17.2.2012
 */
public class SafeLogFactory {
	/** Prefix of the safe logs */
	private static final String SENSITIVE_LOG_ROOT = "sensitive";
	
	/** Creates safe log for given class. 
	 * 
	 * @param c - class for which the log should be created
	 * @return safe log for given class.
	 */
	public static SafeLog getSafeLog(Class<?> c) {
		// create two logs for the class - one with the sensitive prefix and one without prefix
		Log sensitiveLog = LogFactory.getLog(SENSITIVE_LOG_ROOT + "." + c.getCanonicalName()); 
		Log standardLog = LogFactory.getLog(c); 
		
		return new SafeLog(standardLog, sensitiveLog);
	}

	
	private SafeLogFactory() {
	}
	
}