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
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6. 1. 2015
 */
public class GenericTransformDescriptor implements TransformDescriptor<GenericTransform> {

	public static GenericTransformDescriptor newInstance() {
		return new GenericTransformDescriptor();
	}
	
	private GenericTransformDescriptor() {
	}
	
	@Override
	public Class<GenericTransform> getTransformClass() {
		return GenericTransform.class;
	}

	@Override
	public GenericTransform createCTL1Transform(String transformCode, Logger logger) {
		throw new UnsupportedOperationException("CTL1 is not supported in '" + GenericTransform.class.getName() + "'.");
		//return new GenericTransformTL(transformCode, logger);
	}

	@Override
	public Class<? extends CTLAbstractTransform> getCompiledCTL2TransformClass() {
		return CTLGenericTransform.class;
	}

	@Override
	public CTLGenericTransformAdapter createInterpretedCTL2Transform(TransformLangExecutor executor, Logger logger) {
		return new CTLGenericTransformAdapter(executor, logger);
	}

}
