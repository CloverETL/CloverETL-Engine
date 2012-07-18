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

public class ObservableHandler implements IOperationHandler {
	
	private IOperationHandler parent;
	
	public interface IObserver {
		public void observe(IOperationHandler handler, Operation operation);
	}
	
	private static class Logger implements IObserver {

		@Override
		public synchronized void observe(IOperationHandler handler, Operation operation) {
			System.out.printf("%s performs %s%n", handler, operation);
		}
		
	}
	
	private static IObserver observer = new Logger();
	
	public ObservableHandler(IOperationHandler parent) {
		this.parent = parent;
	}

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException {
		observer.observe(parent, Operation.copy(source.getScheme(), target.getScheme()));
		return parent.copy(source, target, params);
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException {
		observer.observe(parent, Operation.move(source.getScheme(), target.getScheme()));
		return parent.move(source, target, params);
	}

	@Override
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) throws IOException {
		observer.observe(parent, Operation.read(source.getScheme()));
		return parent.getInput(source, params);
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) throws IOException {
		observer.observe(parent, Operation.write(target.getScheme()));
		return parent.getOutput(target, params);
	}

	@Override
	public boolean delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		observer.observe(parent, Operation.delete(target.getScheme()));
		return parent.delete(target, params);
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI target, ResolveParameters params) throws IOException {
		observer.observe(parent, Operation.resolve(target.getScheme()));
		return parent.resolve(target, params);
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		observer.observe(parent, Operation.info(target.getScheme()));
		return parent.info(target, params);
	}

	@Override
	public List<Info> list(SingleCloverURI target, ListParameters params) throws IOException {
		observer.observe(parent, Operation.list(target.getScheme()));
		return parent.list(target, params);
	}
	
	@Override
	public boolean create(SingleCloverURI target, CreateParameters params) throws IOException {
		observer.observe(parent, Operation.create(target.getScheme()));
		return parent.create(target, params);
	}

	@Override
	public File getFile(SingleCloverURI target, FileParameters params) throws IOException {
		observer.observe(parent, Operation.create(target.getScheme()));
		return parent.getFile(target, params);
	}

	@Override
	public int getPriority(Operation operation) {
		return parent.getPriority(operation);
	}

	@Override
	public boolean canPerform(Operation operation) {
		return parent.canPerform(operation);
	}

	@Override
	public String toString() {
		return "ObservableHandler: " + parent.toString();
	}

}
