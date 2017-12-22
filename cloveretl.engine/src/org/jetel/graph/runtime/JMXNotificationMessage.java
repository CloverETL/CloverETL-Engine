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
package org.jetel.graph.runtime;

import java.io.Serializable;

/**
 * Container for runId and custom user data object, which can
 * be used in JMX notification.
 * 
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 12. 2017
 */
public class JMXNotificationMessage implements Serializable {
	
	private static final long serialVersionUID = 2445808779831669767L;
	
	private long runId;
	
	private Object userData;
	
	public JMXNotificationMessage(long runId, Object userData) {
		this.runId = runId;
		this.userData = userData;
	}
	
	public long getRunId() {
		return runId;
	}

	public Object getUserData() {
		return userData;
	}
	
}
