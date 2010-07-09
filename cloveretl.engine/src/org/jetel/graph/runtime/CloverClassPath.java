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
package org.jetel.graph.runtime;

import java.net.URL;

/**
 * This class is only simple container for two list of classpath URLs.
 * 1) Runtime classpath URLs should be used as an extra list of places where to search classes given direct in graph.
 * 2) Compile classpath URLs should be used as an extra list of libraries which should be part of -classpath
 * in compile time for java source given direct in graph. @see DynamicCompiler
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.7.2010
 */
public class CloverClassPath {

	private URL[] runtimeClassPath;

	private URL[] compileClassPath;

	public CloverClassPath() {
		this(null, null);
	}
	
	public CloverClassPath(URL[] runtimeClassPath, URL[] compileClassPath) {
		this.runtimeClassPath = runtimeClassPath;
		this.compileClassPath = compileClassPath;
	}

	public URL[] getRuntimeClassPath() {
		return runtimeClassPath;
	}

	public void setRuntimeClassPath(URL[] runtimeClassPath) {
		this.runtimeClassPath = runtimeClassPath;
	}

	public URL[] getCompileClassPath() {
		return compileClassPath;
	}

	public void setCompileClassPath(URL[] compileClassPath) {
		this.compileClassPath = compileClassPath;
	}

}
