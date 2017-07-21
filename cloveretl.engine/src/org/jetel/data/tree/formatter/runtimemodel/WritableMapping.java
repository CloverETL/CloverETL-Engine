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
package org.jetel.data.tree.formatter.runtimemodel;

/**
 * XML writer engine mapping model.
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18 Jan 2011
 */
public class WritableMapping {
	
	private final WritableContainer rootElement;
	private final WritableContainer partitionElement;
	
	private MappingWriteState state;

	public WritableMapping(WritableContainer rootElement, WritableContainer partitionElement) {
		this.rootElement = rootElement;
		this.partitionElement = partitionElement;
	}

	public WritableContainer getRootElement() {
		return rootElement;
	}

	public WritableContainer getPartitionElement() {
		return partitionElement;
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
