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
package org.jetel.component.normalize;

import org.apache.log4j.Logger;
import org.jetel.component.TransformDescriptor;
import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.TransformLangExecutor;

/**
 * Factorisation descriptor for {@link RecordNormalize} class.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.9.2012
 */
public class RecordNormalizeDescriptor implements TransformDescriptor<RecordNormalize> {

	public static RecordNormalizeDescriptor newInstance() {
		return new RecordNormalizeDescriptor();
	}
	
	private RecordNormalizeDescriptor() {
	}

	@Override
	public Class<RecordNormalize> getTransformClass() {
		return RecordNormalize.class;
	}

	@Override
	public RecordNormalize createCTL1Transform(String transformCode, Logger logger) {
		return new RecordNormalizeTL(logger, transformCode);
	}

	@Override
	public Class<? extends CTLAbstractTransform> getCompiledCTL2TransformClass() {
		return CTLRecordNormalize.class;
	}

	@Override
	public RecordNormalize createInterpretedCTL2Transform(TransformLangExecutor executor, Logger logger) {
		return new CTLRecordNormalizeAdapter(executor, logger);
	}

}
