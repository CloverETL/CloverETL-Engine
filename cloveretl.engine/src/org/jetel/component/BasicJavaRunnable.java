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
package org.jetel.component;

import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.CloverPublicAPI;

@CloverPublicAPI
public abstract class BasicJavaRunnable extends AbstractDataTransform implements JavaRunnable {

	protected Properties parameters;

	public BasicJavaRunnable() {
		this(null);
	}
	
	public BasicJavaRunnable(TransformationGraph graph) {
		this.graph = graph;
	}
	
	@Override
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	@Override
	public boolean init(Properties parameters) throws ComponentNotReadyException {
		this.parameters = parameters;
		return init();
	}
	
	public boolean init() {
		return true;
	}

	@Override
	abstract public void run();
	
	/* (non-Javadoc)
	 * @see org.jetel.component.JavaRunnable#free()
	 */
	@Override
	public void free() {}
	
}
