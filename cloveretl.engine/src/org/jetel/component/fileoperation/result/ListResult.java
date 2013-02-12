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

import org.jetel.component.fileoperation.Info;
import org.jetel.component.fileoperation.SingleCloverURI;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.3.2012
 */
public class ListResult extends AbstractResult implements Iterable<Info> {
	
	private final List<Info> result = new ArrayList<Info>();
	
	private final List<List<Info>> parts = new ArrayList<List<Info>>(); 
	
	private final List<SingleCloverURI> uris = new ArrayList<SingleCloverURI>();
	
	public ListResult() {
	}

	public ListResult(Exception fatalError) {
		super(fatalError);
	}

	public List<Info> getResult() {
		return result;
	}
	
	public void add(SingleCloverURI uri, List<Info> infos) {
		addSuccess();
		uris.add(uri);
		result.addAll(infos);
		parts.add(infos);
	}
	
	public void addFailure(SingleCloverURI uri, Exception failure) {
		addFailure(failure);
		uris.add(uri);
		parts.add(null);
	}
	
	@Override
	public ListResult setFatalError(Exception exception) {
		return (ListResult) super.setFatalError(exception);
	}

	public boolean isEmpty() {
		return result.isEmpty();
	}
	
	@Override
	public int successCount() {
		return result.size();
	}
	
	@Override
	public int totalCount() {
		return uris.size();
	}
	
	public List<Info> getResult(int i) {
		return parts.get(i);
	}
	
	public SingleCloverURI getURI(int i) {
		return uris.get(i);
	}
	
	@Override
	public Iterator<Info> iterator() {
		return result.iterator();
	}
	
}
