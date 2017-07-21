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
package org.jetel.component.denormalize;

import org.apache.log4j.Logger;
import org.jetel.component.TransformDescriptor;
import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.TransformLangExecutor;

/**
 * Factorisation descriptor for {@link RecordDenormalize} class.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.9.2012
 */
public class RecordDenormalizeDescriptor implements TransformDescriptor<RecordDenormalize> {

	public static RecordDenormalizeDescriptor newInstance() {
		return new RecordDenormalizeDescriptor();
	}
	
	private RecordDenormalizeDescriptor() {
	}

	@Override
	public Class<RecordDenormalize> getTransformClass() {
		return RecordDenormalize.class;
	}

	@Override
	public RecordDenormalize createCTL1Transform(String transformCode, Logger logger) {
		return new RecordDenormalizeTL(logger, transformCode);
	}

	@Override
	public Class<? extends CTLAbstractTransform> getCompiledCTL2TransformClass() {
		return CTLRecordDenormalize.class;
	}

	@Override
	public RecordDenormalize createInterpretedCTL2Transform(TransformLangExecutor executor, Logger logger) {
		return new CTLRecordDenormalizeAdapter(executor, logger);
	}

}
