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
package org.jetel.graph.runtime.jmx;

import javax.management.Notification;
import javax.management.NotificationFilter;

import org.jetel.graph.runtime.JMXNotificationMessage;

/**
 * JMX notification filter which allows only notifications with user data
 * containing instance of {@link JMXNotificationMessage} with specified runId.
 * 
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 12. 2017
 */
public class RunIdNotificationFilter implements NotificationFilter {

	private static final long serialVersionUID = -7787802499929544438L;
	
	private long runId;
	
	public RunIdNotificationFilter(long runId) {
		this.runId = runId;
	}
	
	@Override
	public boolean isNotificationEnabled(Notification notification) {
		Object userData = notification.getUserData();
		if (userData instanceof JMXNotificationMessage) {
			JMXNotificationMessage message = (JMXNotificationMessage) userData;
			return runId == message.getRunId();
		} else {
			return false;
		}
	}
	
}
