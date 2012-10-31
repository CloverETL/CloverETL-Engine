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
 * Factorisation descriptor for {@link RecordGenerate} class.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.9.2012
 */
public class RecordGenerateDescriptor implements TransformDescriptor<RecordGenerate> {

	public static RecordGenerateDescriptor newInstance() {
		return new RecordGenerateDescriptor();
	}
	
	private RecordGenerateDescriptor() {
	}
	
	@Override
	public Class<RecordGenerate> getTransformClass() {
		return RecordGenerate.class;
	}

	@Override
	public RecordGenerate createCTL1Transform(String transformCode, Logger logger) {
		return new RecordGenerateTL(transformCode, logger);
	}

	@Override
	public Class<? extends CTLAbstractTransform> getCompiledCTL2TransformClass() {
		return CTLRecordGenerate.class;
	}

	@Override
	public RecordGenerate createInterpretedCTL2Transform(TransformLangExecutor executor, Logger logger) {
		return new CTLRecordGenerateAdapter(executor, logger);
	}

}
