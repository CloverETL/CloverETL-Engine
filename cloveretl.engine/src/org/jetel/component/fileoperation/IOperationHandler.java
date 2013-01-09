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
 * The FileManager and related classes
 * are considered internal and may change in the future.
 */
public interface IOperationHandler {
	
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException;

	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException;

	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) throws IOException;

	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) throws IOException;
	
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException;

	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException;
	
	public List<Info> list(SingleCloverURI parent, ListParameters params) throws IOException;
	
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException;
	
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException;
	
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException;
	
	public int getPriority(Operation operation);
	
	public boolean canPerform(Operation operation);
	
	public static final int TOP_PRIORITY = Integer.MAX_VALUE - 100; // keep some reserve
	
}
