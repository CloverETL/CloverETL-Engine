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
package org.jetel.component.rollup;

import org.apache.log4j.Logger;
import org.jetel.component.TransformDescriptor;
import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.TransformLangExecutor;

/**
 * Factorisation descriptor for {@link RecordRollup} class.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.9.2012
 */
public class RecordRollupDescriptor implements TransformDescriptor<RecordRollup> {

	public static RecordRollupDescriptor newInstance() {
		return new RecordRollupDescriptor();
	}
	
	private RecordRollupDescriptor() {
	}
	
	@Override
	public Class<RecordRollup> getTransformClass() {
		return RecordRollup.class;
	}

	@Override
	public RecordRollup createCTL1Transform(String transformCode, Logger logger) {
		return new RecordRollupTL(transformCode, logger);
	}

	@Override
	public Class<? extends CTLAbstractTransform> getCompiledCTL2TransformClass() {
		return CTLRecordRollup.class;
	}

	@Override
	public RecordRollup createInterpretedCTL2Transform(TransformLangExecutor executor, Logger logger) {
		return new CTLRecordRollupAdapter(executor, logger);
	}

}
