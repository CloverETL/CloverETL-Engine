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
import java.util.List;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 13, 2012
 */
public abstract class AbstractResult implements Result {

	private int successCount = 0;
	private int errorCount = 0;

	private final List<String> errors = new ArrayList<String>();

	private Exception exception = null;

	@Override
	public int errorCount() {
		return errorCount;
	}

	@Override
	public boolean success() {
		return (getException() == null) && (errorCount() == 0);
	}

	@Override
	public boolean success(int i) {
		return (getException() == null) && (getError(i) == null);
	}

	@Override
	public String getError(int i) {
		return errors.get(i);
	}

	@Override
	public Exception getException() {
		return exception;
	}
	
	public AbstractResult setException(Exception exception) {
		this.exception = exception;
		return this;
	}
	
	protected void addSuccess() {
		errors.add(null);
		successCount++;
	}

	protected void addError(String error) {
		errors.add(error);
		errorCount++;
	}
	
	@Override
	public int successCount() {
		return successCount;
	}

	@Override
	public String getFirstErrorMessage() {
		if (exception != null) {
			return exception.getMessage();
		}
		for (int i = 0; i < totalCount(); i++) {
			if (getError(i) != null) {
				return getError(i);
			}
		}
		return null;
	}

}
