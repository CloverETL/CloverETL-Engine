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

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
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
public class InfoResult extends AbstractResult implements Iterable<Info> {
	
	private final List<Info> parts = new ArrayList<Info>(); 
	
	private final List<SingleCloverURI> uris = new ArrayList<SingleCloverURI>();
	
	public List<Info> getResult() {
		return parts;
	}
	
	public void add(SingleCloverURI uri, Info info) {
		addSuccess();
		uris.add(uri);
		parts.add(info);
	}
	
	public void addError(SingleCloverURI uri, String error) {
		addError(error);
		uris.add(uri);
		parts.add(null);
	}
	
	@Override
	public InfoResult setException(Exception exception) {
		return (InfoResult) super.setException(exception);
	}

	public boolean isEmpty() {
		return totalCount() == 0;
	}
	
	@Override
	public int totalCount() {
		return uris.size();
	}
	
	public Info getResult(int i) {
		return parts.get(i);
	}
	
	public SingleCloverURI getURI(int i) {
		return uris.get(i);
	}
	
	@Override
	public Iterator<Info> iterator() {
		return parts.iterator();
	}
	
	public Info getInfo() {
		int size = successCount();
		if (size == 0) {
			return null;
		}
		if (size > 1) {
			throw new IllegalStateException("Not a single URI");
		}
		return parts.get(0);
	}

	public String getName() {
		return getInfo().getName();
	}

	public URI getURI() {
		return getInfo().getURI();
	}

	public URI getParentDir() {
		return getInfo().getParentDir();
	}

	public boolean exists() {
		return getInfo() != null;
	}

	public boolean isDirectory() {
		Info first = getInfo();
		return (first != null) && first.isDirectory();
	}

	public boolean isFile() {
		Info first = getInfo();
		return (first != null) && first.isFile();
	}

	public Info.Type getType() {
		return getInfo().getType();
	}

	public Date getLastModified() {
		return getInfo().getLastModified();
	}

	public Date getCreated() {
		return getInfo().getCreated();
	}

	public Date getLastAccessed() {
		return getInfo().getLastAccessed();
	}

	public Long getSize() {
		return getInfo().getSize();
	}

	@Override
	public String toString() {
		return parts.toString();
	}
	
	
	
}
