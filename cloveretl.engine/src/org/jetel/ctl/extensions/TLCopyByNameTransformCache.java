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
package org.jetel.ctl.extensions;

import org.jetel.component.CustomizedRecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This class is used in Integral.copyByName() CTL2 function for caching
 * of CustomizedRecordTransform instance.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.8.2010
 */
public class TLCopyByNameTransformCache extends TLCache {

	private boolean isInitialized;
	
	private CustomizedRecordTransform transform;
	
	private DataRecord[] tempSources = new DataRecord[1];
	private DataRecord[] tempTargets = new DataRecord[1];
	
	public TLCopyByNameTransformCache() {
		transform = new CustomizedRecordTransform(null);
		transform.addFieldToFieldRule("*.*", "*.*");
		isInitialized = false;
	}

	public void init(DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata) throws ComponentNotReadyException {
		if (!isInitialized) {
			transform.init(null, new DataRecordMetadata[] { sourceMetadata }, new DataRecordMetadata[] { targetMetadata });
			isInitialized = true;
		}
	}
	
	public void transform(DataRecord source, DataRecord target) throws TransformException {
		tempSources[0] = source;
		tempTargets[0] = target;
		transform.transform(tempSources, tempTargets);
	}
	
}
