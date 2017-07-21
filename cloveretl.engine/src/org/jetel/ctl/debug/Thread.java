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

/**
 * This unit represents a thread in which a CTL code is being executed,
 * including its current debugging status.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14.3.2016
 */
public class Thread implements Serializable {

	private static final long serialVersionUID = 1L;

	private long id;
	private String name;
	private boolean stepping;
	private boolean suspended;
	private transient java.lang.Thread javaThread;
	
	/**
	 * Answers Java thread in which the CTL is being interpreted.
	 * @return
	 */
	public java.lang.Thread getJavaThread() {
		return javaThread;
	}
	
	public void setJavaThread(java.lang.Thread javaThread) {
		this.javaThread = javaThread;
		this.id = javaThread.getId();
		this.name = javaThread.getName();
	}
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public boolean isStepping() {
		return stepping;
	}
	public void setStepping(boolean stepping) {
		this.stepping = stepping;
	}
	public boolean isSuspended() {
		return suspended;
	}
	public void setSuspended(boolean suspended) {
		this.suspended = suspended;
	}
	
	@Override
	public String toString() {
		return "CTLThread[name=" + name + ", javaThread=" + javaThread + (javaThread != null ? (", state=" + javaThread.getState()) : "") + "]";
	}
}
