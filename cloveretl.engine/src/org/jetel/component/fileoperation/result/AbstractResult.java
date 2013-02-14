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

import org.jetel.util.ExceptionUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 13, 2012
 */
public abstract class AbstractResult implements Result {

	private int successCount = 0;
	private int failCount = 0;

	private final List<Exception> failures = new ArrayList<Exception>();

	private Exception fatalError = null;

	public AbstractResult() {
	}

	public AbstractResult(Exception fatalError) {
		this.fatalError = fatalError;
	}

	@Override
	public int failCount() {
		return failCount;
	}

	@Override
	public boolean success() {
		return (getFatalError() == null) && (failCount() == 0);
	}

	@Override
	public boolean success(int i) {
		return (getFatalError() == null) && (getFailure(i) == null);
	}

	@Override
	public Exception getFailure(int i) {
		return failures.get(i);
	}

	@Override
	public Exception getFatalError() {
		return fatalError;
	}
	
	public AbstractResult setFatalError(Exception fatalError) {
		this.fatalError = fatalError;
		return this;
	}
	
	protected void addSuccess() {
		failures.add(null);
		successCount++;
	}

	protected void addFailure(Exception failure) {
		failures.add(failure);
		failCount++;
	}
	
	@Override
	public int successCount() {
		return successCount;
	}

	@Override
	public Exception getFirstError() {
		if (fatalError != null) {
			return fatalError;
		}
		for (int i = 0; i < totalCount(); i++) {
			if (getFailure(i) != null) {
				return getFailure(i);
			}
		}
		return null;
	}

	@Override
	public String getFirstErrorMessage() {
		Exception ex = getFirstError();
		return ex != null ? ExceptionUtils.exceptionChainToMessage(null, ex) : null;
	}

}
