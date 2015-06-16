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
package org.jetel.component.fileoperation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.3.2012
 */
public class WildcardResolver extends BaseOperationHandler {
	
	private final String[] schemes;
	
	private FileManager manager = FileManager.getInstance();
	
	public WildcardResolver(String... schemas) {
		String[] tmp = Arrays.copyOf(schemas, schemas.length);
		Arrays.sort(tmp);
		this.schemes = tmp;
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) {
		try {
			return manager.defaultResolve(uri);
		} catch (Exception ex) {
			return new ArrayList<SingleCloverURI>(0);
		}
	}

	@Override
	public int getPriority(Operation operation) {
		return Integer.MIN_VALUE;
	}

	@Override
	public boolean canPerform(Operation operation) {
		if (operation.kind == OperationKind.RESOLVE) {
			return Arrays.binarySearch(schemes, operation.scheme()) >= 0;
		}
		return false;
	}

}
