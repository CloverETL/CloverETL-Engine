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
package org.jetel.graph.analyse;

import java.util.Iterator;

import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;

/**
 * This class is used by {@link GraphCycleInspector} class to provide all components of inspected graph.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.12.2012
 * @see GraphCycleInspector
 * @see BasicInspectedComponent
 */
public class SingleGraphProvider extends AbstractGraphProvider {

	private Iterator<Node> componentsIterator;
	
	/**
	 * @param graph
	 */
	public SingleGraphProvider(TransformationGraph graph) {
		componentsIterator = graph.getNodes().values().iterator();
	}

	@Override
	public BasicInspectedComponent getNextComponent() {
		if (componentsIterator.hasNext()) {
			return new BasicInspectedComponent(componentsIterator.next(), null);
		} else {
			return null;
		}
	}

}
