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

import java.util.ArrayList;
import java.util.List;

import org.jetel.component.StaticMetadataProvider;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVGraphElement;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * General model wrapper for engine metadata ({@link DataRecordMetadata}).
 * 
 * This class has to be serializable, see {@link StaticMetadataProvider}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 9. 2013
 */
public class MVEngineMetadata extends MVEngineGraphElement implements MVMetadata {

	private static final long serialVersionUID = 1180088952413191562L;

	private DataRecordMetadata metadata;
	
	private int priority;
	
	private List<MVGraphElement> originPath;
	
	MVEngineMetadata(DataRecordMetadata metadata, MVGraph parentMVGraph) {
		this(metadata, null, DEFAULT_PRIORITY);
	}
	
	MVEngineMetadata(DataRecordMetadata metadata, MVGraph parentMVGraph, int priority) {
		super(metadata, parentMVGraph);
		this.metadata = metadata;
		this.priority = priority;
		originPath = new ArrayList<MVGraphElement>();
	}

	@Override
	public DataRecordMetadata getModel() {
		return metadata;
	}
	
	@Override
	public void reset() {
		//DO NOTHING
	}
	
	@Override
	public MVMetadata duplicate() {
		MVEngineMetadata result = new MVEngineMetadata(metadata, getParent(), priority);
		if (originPath != null) {
			result.originPath = new ArrayList<MVGraphElement>(originPath);
		}
		return result;
	}
	
	@Override
	public int getPriority() {
		return priority;
	}
	
	@Override
	public void setPriority(int priority) {
		this.priority = priority;
	}
	
	@Override
	public String getId() {
		return metadata.getId();
	}
	
	@Override
	public void addToOriginPath(MVGraphElement graphElement) {
		if (graphElement != null) { 
			originPath.add(0, graphElement);
		}
	}

	@Override
	public void addToOriginPath(List<MVGraphElement> originPath) {
		if (originPath != null) {
			this.originPath.addAll(0, originPath);
		}
	}

	@Override
	public List<MVGraphElement> getOriginPath() {
		return originPath;
	}
	
	@Override
	public MVGraph getParent() {
		return (MVGraph) super.getParent();
	}
	
}
