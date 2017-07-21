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
public class CreateResult extends AbstractResult implements Iterable<SingleCloverURI> {
	
	private final List<SingleCloverURI> result = new ArrayList<SingleCloverURI>(); 
	
	private final List<SingleCloverURI> uris = new ArrayList<SingleCloverURI>();
	
	private final List<SingleCloverURI> newFiles = new ArrayList<SingleCloverURI>(); 
	
	public CreateResult() {
	}

	public CreateResult(Exception fatalError) {
		super(fatalError);
	}

	@Override
	public CreateResult setFatalError(Exception exception) {
		return (CreateResult) super.setFatalError(exception);
	}

	public void add(SingleCloverURI uri, SingleCloverURI target) {
		addSuccess();
		uris.add(uri);
		result.add(target);
		newFiles.add(target);
	}
	
	public void addFailure(SingleCloverURI source, Exception failure) {
		addFailure(failure);
		uris.add(source);
		newFiles.add(null);
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
	
	public SingleCloverURI getResult(int i) {
		return newFiles.get(i);
	}
	
	public SingleCloverURI getURI(int i) {
		return uris.get(i);
	}
	
	@Override
	public Iterator<SingleCloverURI> iterator() {
		return result.iterator();
	}
	
}
