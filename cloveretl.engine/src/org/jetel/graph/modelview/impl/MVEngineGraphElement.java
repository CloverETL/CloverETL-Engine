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

import org.jetel.graph.IGraphElement;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVGraphElement;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4. 9. 2015
 */
public abstract class MVEngineGraphElement implements MVGraphElement {

	private static final long serialVersionUID = -909520506849532733L;

	private transient IGraphElement engineGraphElement;
	
	private MVGraphElement parent;
	
	private String id; //id is persisted in local variable, because model is not serialized
	
	public MVEngineGraphElement(IGraphElement engineGraphElement, MVGraphElement parent) {
		if (engineGraphElement == null) {
			throw new IllegalArgumentException("MVEngineGraphElement init failed");
		}
		
		this.id = engineGraphElement.getId();
		this.engineGraphElement = engineGraphElement;
		this.parent = parent;
	}
	
	@Override
	public MVGraphElement getParent() {
		return parent;
	}
	
	@Override
	public MVGraph getParentMVGraph() {
		MVGraphElement pointer = parent;
		while (!(pointer instanceof MVGraph)) {
			if (pointer.getParent() != null) {
				pointer = pointer.getParent();
			} else {
				return null;
			}
		}
		return (MVGraph) pointer;
	}
	
	@Override
	public IGraphElement getModel() {
		if (engineGraphElement != null) {
			return engineGraphElement;
		} else {
			throw new UnsupportedOperationException(); //model is not serialized
		}
	}

	@Override
	public boolean hasModel() {
		return engineGraphElement != null;
	}
	
	@Override
	public String getId() {
		return id;
	}

	@Override
	public List<String> getIdPath() {
		List<String> result = new ArrayList<>();
		MVGraphElement index = this;
		while (index != null) {
			result.add(index.getId());
			index = index.getParent();
		}
		return result;
	}

	@Override
	public int hashCode() {
		if (hasModel()) {
			return System.identityHashCode(getModel());
		} else {
			return getIdPath().hashCode();
		}
	}

	protected boolean equalsGraphElement(MVGraphElement obj) {
		if (hasModel()) {
			return getModel() == obj.getModel();
		} else {
			return getIdPath().equals(obj.getIdPath());
		}
	}
	
	@Override
	public String toString() {
		return hasModel() ? getModel().toString() : this.getClass().getName() + ":" + getId();
	}
	
}
