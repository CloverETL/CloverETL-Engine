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
package org.jetel.component;

import org.apache.log4j.Logger;
import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.TransformLangExecutor;

/**
 * This class aggregates all necessary information for a transformation interface
 * (transformation code loaded by various components RecordTransform, RecordRollup, JavaRunnable, ...)
 * to be instantiated by {@link TransformFactory}. Supported languages are java, CTL1 and CTL2.
 * 
 * @see TransformFactory
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.8.2012
 */
public interface TransformDescriptor<T> {

	/**
	 * @return the class of interface which is instantiated by {@link TransformFactory}, usually it is 
	 * a descendant of {@link Transform} interface
	 */
	public Class<T> getTransformClass();

	/**
	 * Creates instance of {@link #getTransformClass()} class based on the CTL1 transformation code. 
	 * @param transformCode CTL1 transformation code to be converted to dedicated class
	 * @param logger a logger necessary for CTL1 compiler
	 * @return instance of transformation interface defined by CTL1 transformation code 
	 */
	public T createCTL1Transform(String transformCode, Logger logger);
	
	/**
	 * @return class which is used for compiled mode of CTL2 compiler
	 * to be overridden by generated java code    
	 */
	public Class<? extends CTLAbstractTransform> getCompiledCTL2TransformClass();

	/**
	 * Creates instance of transformation interface based on CTL2 interpreted executor.
	 * @param executor CTL2 executor of transformation code
	 * @param logger
	 * @return instance of transformation interface based on CTL2 interpreted executor
	 */
	public T createInterpretedCTL2Transform(TransformLangExecutor executor, Logger logger);
	
}
