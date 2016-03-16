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
package org.jetel.ctl.debug;

import java.util.concurrent.ArrayBlockingQueue;
import org.jetel.ctl.debug.Thread;

public class CTLDebugThread {

	private Thread thread;
	private ArrayBlockingQueue<DebugStatus> statusQueue;
	private ArrayBlockingQueue<DebugCommand> commandQueue;
	
	/**
	 * 
	 */
	public CTLDebugThread(Thread thread, ArrayBlockingQueue<DebugCommand> commandQueue, ArrayBlockingQueue<DebugStatus> statusQueue) {
		this.thread = thread;
		this.commandQueue = commandQueue;
		this.statusQueue = statusQueue;
	}
	
	public Thread getThread() {
		return thread;
	}
	
	public void putCommand(DebugCommand command) {
		try {
			commandQueue.put(command);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public DebugStatus takeCommand() {
		try {
			return statusQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}