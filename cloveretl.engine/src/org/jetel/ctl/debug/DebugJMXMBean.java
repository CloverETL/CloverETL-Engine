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

import org.jetel.ctl.debug.StackFrame;
import org.jetel.ctl.debug.Thread;

/**
 * @author Magdalena Malysz (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11. 3. 2016
 */
public interface DebugJMXMBean {

    public static final String THREAD_SUSPENDED = "clover.thread.suspend";
    public static final String THREAD_RESUMED = "clover.thread.resumed";

	public void info(long threadId);
	
    Thread[] listCtlThreads();
    
    StackFrame[] getStackFrames(long threadId);
    
    void resume(long threadId);
    
    void resumeAll();

    void addBreakpoints(Breakpoint breakpoints[]);
}
