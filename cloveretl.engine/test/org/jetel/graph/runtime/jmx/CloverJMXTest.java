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
import javax.management.NotificationListener;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.JMXNotificationMessage;
import org.jetel.graph.runtime.WatchDog;
import org.jetel.test.CloverTestCase;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 1. 2018
 */
public class CloverJMXTest extends CloverTestCase {

	public void testCleanObsoleteWatchDogs() throws InterruptedException, ComponentNotReadyException {
		CloverJMX.getInstance().addNotificationListener(createJMXNotificationListener(), null, null);
		
		//register new watchdog, but not execute
		registerWatchDog(123);

		//check the watchdog is registered
		assertNotNull(CloverJMX.getInstance().getGraphTracking(123));
		
		//check an unknown watchdog is not registered
		try {
			CloverJMX.getInstance().getGraphTracking(1234);
			fail();
		} catch (JetelRuntimeException e) {
			//OK
		}

		//check default obsolete timeout 
		assertEquals(10000, CloverJMX.getInstance().getObsoleteJobTimeout());
		
		//set new obsolete timeout for testing purpose
		CloverJMX.getInstance().setObsoleteJobTimeout(50);

		//check the new obsolete timeout
		assertEquals(50, CloverJMX.getInstance().getObsoleteJobTimeout());

		//check the watchdog is still registered
		assertNotNull(CloverJMX.getInstance().getGraphTracking(123));

		//sleep a while
		Thread.sleep(100);

		//register new watchdog
		registerWatchDog(124);

		//check registered watchdogs
		assertNotNull(CloverJMX.getInstance().getGraphTracking(123));
		assertNotNull(CloverJMX.getInstance().getGraphTracking(124));
		try {
			CloverJMX.getInstance().getGraphTracking(125);
			fail();
		} catch (JetelRuntimeException e) {
			//OK
		}

		//register new watchdog and execute the graph
		TransformationGraph graph = new TransformationGraph();
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setRunId(125);
		org.jetel.main.runGraph.executeGraph(graph, runtimeContext);
		
		//check registered watchdogs
		assertNotNull(CloverJMX.getInstance().getGraphTracking(123));
		assertNotNull(CloverJMX.getInstance().getGraphTracking(124));
		assertNotNull(CloverJMX.getInstance().getGraphTracking(125));

		Thread.sleep(1000); 

		//register one more watchdog to trigger obsolete jobs cleanup
		registerWatchDog(126);

		//the finished graph 125 should be automatically unregistered
		assertNotNull(CloverJMX.getInstance().getGraphTracking(123));
		assertNotNull(CloverJMX.getInstance().getGraphTracking(124));
		try {
			CloverJMX.getInstance().getGraphTracking(125);
			fail();
		} catch (JetelRuntimeException e) {
			//OK
		}
		assertNotNull(CloverJMX.getInstance().getGraphTracking(126));
	}
	
	private void registerWatchDog(long runId) {
		TransformationGraph graph = new TransformationGraph();
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setRunId(runId);
		WatchDog watchDog = new WatchDog(graph, runtimeContext);
		watchDog.init();
	}
	
	private NotificationListener createJMXNotificationListener() {
		return new NotificationListener() {
			@Override
			public void handleNotification(Notification notification, Object handback) {
				if (CloverJMX.GRAPH_FINISHED.equals(notification.getType())) {
					JMXNotificationMessage notificationMessage = (JMXNotificationMessage) notification.getUserData();					
					CloverJMX.getInstance().releaseJob(notificationMessage.getRunId());
				}
			}
		};
	}
}
