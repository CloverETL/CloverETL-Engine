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

import java.io.Serializable;

import org.jetel.ctl.debug.DebugCommand.CommandType;

/**
 * This unit represents mark where "Run to Line" operation should
 * run up to. See {@link CommandType#RUN_TO_LINE}.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.4.2016
 */
public class RunToMark implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private boolean skipBreakpoints;
	private Breakpoint to;
	
	public RunToMark(Breakpoint to) {
		this.to = to;
	}
	
	public RunToMark(String source, int line) {
		this(new Breakpoint(source, line));
	}
	
	public Breakpoint getTo() {
		return to;
	}
	
	public boolean isSkipBreakpoints() {
		return skipBreakpoints;
	}
	
	public void setSkipBreakpoints(boolean skipBreakpoints) {
		this.skipBreakpoints = skipBreakpoints;
	}
}
