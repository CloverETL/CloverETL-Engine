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

/**
 * This interface describes a listener which listens to a CloverWorker and thus
 * can react on worker finishing, either gracefully or abnormally
 * 
 * @author Pavel Najvar (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.8.2010
 */
public interface CloverWorkerListener {

	
	/**
	 * An event class carries all information needed when an CloverWorker talks to its listeners
	 * 
	 * @author Pavel Najvar (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 4.8.2010
	 */
	public static class Event {
		
		CloverWorker source;
		Throwable exception;
		
		public Event(CloverWorker source) {
			this.source = source;
		}
		
		public Event(CloverWorker source, Throwable exception) {
			this.source = source;
			this.exception = exception;
		}
		
		public CloverWorker getSource() {
			return this.source;
		}
	
		public Throwable getException() {
			return this.exception;
		}
		
	}
	
	/**
	 * 
	 * Called when a worker finishes its job normally
	 * 
	 * @param w A just-finished worker
	 */
	void workerFinished(Event e);
	
	
	/**
	 * Called when a worker finishes abruptly - usually with a runtime Exception
	 * @param w
	 */
	void workerCrashed(Event e);
	
	
}
