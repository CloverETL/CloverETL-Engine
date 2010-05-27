/*
 * jETeL/Clover - Java based ETL application framework.
 * Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
package org.jetel.util.compile;

/**
 * Checked exception thrown by {@link DynamicCompiler} if compilation fails.
 *
 * @version 14th May 2010
 * @created 14th May 2010
 *
 * @see DynamicCompiler
 */
public final class CompilationException extends Exception {

	/** The revision of this class. */
	private static final long serialVersionUID = 1954989402031394706L;

	/** The compiler output generated during compilation. */
	private final String compilerOutput;

	/**
	 * Constructs a <code>CompilationException</code> instance.
	 *
	 * @param message the detail message
	 * @param cause the cause of this exception
	 */
	protected CompilationException(String message, Throwable cause) {
		super(message, cause);

		this.compilerOutput = null;
	}

	/**
	 * Constructs a <code>CompilationException</code> instance.
	 *
	 * @param message the detail message
	 * @param compilerOutput the compiler output generated during compilation
	 */
	protected CompilationException(String message, String compilerOutput) {
		super(message);

		this.compilerOutput = compilerOutput;
	}

	/**
	 * @return compiler output generated during compilation, or <code>null</code> if no output was generated
	 */
	public String getCompilerOutput() {
		return compilerOutput;
	}

}
