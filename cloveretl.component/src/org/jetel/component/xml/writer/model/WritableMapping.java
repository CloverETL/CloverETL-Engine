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
package org.jetel.component.xml.writer.model;

/**
 * @author LKREJCI (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18 Jan 2011
 */
public class WritableMapping {
	
	private final String version;
	
	private final WritableElement rootElement;
	private final WritableLoopElement partitionElement;
	
	private MappingWriteState state;
	
		public WritableMapping(String version, WritableElement rootElement,	WritableLoopElement partitionElement) {
		this.version = version;
		this.rootElement = rootElement;
		this.partitionElement = partitionElement;
	}

	public WritableElement getRootElement() {
		return rootElement;
	}

	public WritableLoopElement getPartitionElement() {
		return partitionElement;
	}

	public String getVersion() {
		return version;
	}

	public MappingWriteState getState() {
		return state;
	}

	public void setState(MappingWriteState state) {
		this.state = state;
	}

	public static enum MappingWriteState {
		HEADER, FOOTER, ALL, NOTHING
	}
	
}
