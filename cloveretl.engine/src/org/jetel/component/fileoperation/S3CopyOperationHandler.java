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

import java.io.IOException;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.util.file.SandboxUrlUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 3. 2015
 */
public class S3CopyOperationHandler extends S3OperationHandler {
	
	public S3CopyOperationHandler() {
		super(new PrimitiveS3CopyOperationHandler());
	}

	@Override
	public int getPriority(Operation operation) {
		return TOP_PRIORITY;
	}

	@Override
	public boolean canPerform(Operation operation) {
		switch (operation.kind) {
		case COPY:
		case MOVE:
			return (operation.scheme(0).equalsIgnoreCase(LocalOperationHandler.FILE_SCHEME) 
						|| operation.scheme(0).equals(SandboxUrlUtils.SANDBOX_PROTOCOL))
					&& operation.scheme(1).equalsIgnoreCase(S3_SCHEME);
		default:
			return false;
		}
	}

	/*
	 * Overridden to disable authority check
	 */
	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException {
		return super.copy(source.toURI(), target.toURI(), params);
	}

	/*
	 * Overridden to disable authority check
	 */
	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException {
		return super.move(source.toURI(), target.toURI(), params);
	}

	@Override
	public String toString() {
		return "S3CopyOperationHandler";
	}

}
