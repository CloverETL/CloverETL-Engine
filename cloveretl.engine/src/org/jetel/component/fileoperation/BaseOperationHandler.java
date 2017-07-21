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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.SimpleParameters.FileParameters;
import org.jetel.component.fileoperation.SimpleParameters.InfoParameters;
import org.jetel.component.fileoperation.SimpleParameters.ListParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ReadParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.SimpleParameters.WriteParameters;

/**
 * Convenience base class for operation handler creation.
 * All methods throw {@link UnsupportedOperationException}.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7. 5. 2015
 */
public abstract class BaseOperationHandler implements IOperationHandler {

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public List<Info> list(SingleCloverURI parent, ListParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		throw new UnsupportedOperationException("Not implemented");
	}

}
