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
public class MoveResult extends AbstractResult implements Iterable<SingleCloverURI> {
	
	private final List<SingleCloverURI> aggregatedResult = new ArrayList<SingleCloverURI>(); 
	
	private final List<SingleCloverURI> sources = new ArrayList<SingleCloverURI>();
	
	private final List<SingleCloverURI> targets = new ArrayList<SingleCloverURI>(); 
	
	private final List<SingleCloverURI> results = new ArrayList<SingleCloverURI>(); 

	public MoveResult setException(Exception exception) {
		return (MoveResult) super.setException(exception);
	}

	public void add(SingleCloverURI source, SingleCloverURI target, SingleCloverURI result) {
		addSuccess();
		this.sources.add(source);
		this.targets.add(target);
		this.results.add(result);
		this.aggregatedResult.add(result);
	}
	
	public void addError(SingleCloverURI source, SingleCloverURI target, String error) {
		addError(error);
		this.sources.add(source);
		this.targets.add(target);
		this.results.add(null);
	}
	
	public int successCount() {
		return aggregatedResult.size();
	}
	
	public int totalCount() {
		return sources.size();
	}
	
	@Override
	public boolean success(int i) {
		return (getException() == null) && (getResult(i) != null);
	}

	public SingleCloverURI getSource(int i) {
		return sources.get(i);
	}
	
	public SingleCloverURI getTarget(int i) {
		return targets.get(i);
	}
	
	public SingleCloverURI getResult(int i) {
		return results.get(i);
	}
	
	@Override
	public Iterator<SingleCloverURI> iterator() {
		return aggregatedResult.iterator();
	}
	
}
