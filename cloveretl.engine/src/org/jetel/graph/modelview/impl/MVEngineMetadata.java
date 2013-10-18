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
package org.jetel.graph.modelview.impl;

import org.jetel.graph.modelview.MVMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * General model wrapper for engine metadata ({@link DataRecordMetadata}).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 9. 2013
 */
public class MVEngineMetadata implements MVMetadata {

	private DataRecordMetadata metadata;
	
	private int priority;
	
	public MVEngineMetadata(DataRecordMetadata metadata) {
		this(metadata, LOW_PRIORITY);
	}
	
	public MVEngineMetadata(DataRecordMetadata metadata, int priority) {
		this.metadata = metadata;
		this.priority = priority;
	}

	@Override
	public DataRecordMetadata getMetadata() {
		return metadata;
	}
	
	@Override
	public int getPriority() {
		return priority;
	}
	
	@Override
	public void setId(String id) {
		metadata.setId(id);
	}
	
}
