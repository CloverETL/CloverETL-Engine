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
package org.jetel.component.fileoperation.result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetel.component.fileoperation.SingleCloverURI;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.3.2012
 */
public class ResolveResult extends AbstractResult implements Iterable<SingleCloverURI> {
	
	private final List<SingleCloverURI> result = new ArrayList<SingleCloverURI>();
	
	private final List<List<SingleCloverURI>> parts = new ArrayList<List<SingleCloverURI>>(); 
	
	private final List<SingleCloverURI> uris = new ArrayList<SingleCloverURI>();
	
	public List<SingleCloverURI> getResult() {
		return result;
	}
	
	public void add(SingleCloverURI uri, List<SingleCloverURI> resolved) {
		addSuccess();
		result.addAll(resolved);
		parts.add(resolved);
		uris.add(uri);
	}
	
	public void addFailure(SingleCloverURI uri, Exception failure) {
		addFailure(failure);
		parts.add(null);
		uris.add(uri);
	}
	
	@Override
	public ResolveResult setFatalError(Exception exception) {
		return (ResolveResult) super.setFatalError(exception);
	}

	@Override
	public int successCount() {
		return result.size();
	}
	
	public boolean isEmpty() {
		return result.isEmpty();
	}
	
	@Override
	public int totalCount() {
		return uris.size();
	}
	
	public List<SingleCloverURI> getResult(int i) {
		return parts.get(i);
	}
	
	public SingleCloverURI getURI(int i) {
		return uris.get(i);
	}
	
	@Override
	public Iterator<SingleCloverURI> iterator() {
		return result.iterator();
	}

	@Override
	public String toString() {
		return result.toString();
	}
	
	
	
}
