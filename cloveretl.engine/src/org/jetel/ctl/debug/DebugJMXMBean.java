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

import java.util.List;

import org.jetel.ctl.debug.DebugCommand.CommandType;

/**
 * @author Magdalena Malysz (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11. 3. 2016
 */
public interface DebugJMXMBean {

	public static final String JOB_INIT = "clover.job.init";
    public static final String THREAD_SUSPENDED = "clover.thread.suspend";
    public static final String THREAD_RESUMED = "clover.thread.resumed";
    public static final String THREAD_STARTED = "clover.thread.started";
    public static final String THREAD_STOPPED = "clover.thread.stopped";
    public static final String BP_CONDITION_ERROR = "clover.bp_condition.error";
	
    Thread[] listCtlThreads(long runId);
    
    StackFrame[] getStackFrames(long runId, long threadId);
    
    void resume(long runId, long threadId);
    
    void resumeAll(long runId);

    void suspend(long runId, long threadId);

    void suspendAll(long runId);

    void addBreakpoints(long runId, List<Breakpoint> breakpoints);
    
    void removeBreakpoints(long runId, List<Breakpoint> breakpoints);
    
    void modifyBreakpoint(long runId, Breakpoint breakpoint);

    void setCtlBreakingEnabled(long runId, boolean enabled);
    
    void stepThread(long runId, long threadId, CommandType stepType);
    
    void runToLine(long runId, long threadId, RunToMark mark);
    
    ListVariableResult listVariables(long runId, long threadId, int stackFrameDepth, boolean includeGlobal);
    
    Object evaluateExpression(long runId, String expression, long threadId, int callStackIndex) throws Exception;
}
