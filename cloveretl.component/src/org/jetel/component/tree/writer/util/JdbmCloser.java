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
package org.jetel.component.tree.writer.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.jetel.exception.JetelRuntimeException;

import jdbm.recman.BaseRecordManager;


public class JdbmCloser {

	/**
	 * Closes JDBM {@link BaseRecordManager} using package-private <code>forceClose</code> method of its
	 * RecordFile-s. While this method is fast because there is no synchronization with the disk storage,
	 * JDBM's files are closed unsynchronized, likely corrupted, and should be deleted once this method has completed.
	 * @param manager
	 */
	public static void fastCloseUsingReflection(BaseRecordManager manager) {
		
		try {
			String fileFieldNames[] = new String[] {"_physFile", "_logicFile", "_physFileFree", "_logicFileFree"};
			
			Class<?> recordFileType = Class.forName("jdbm.recman.RecordFile", true, manager.getClass().getClassLoader());
			Method forceCloseMethod = recordFileType.getDeclaredMethod("forceClose", (Class[])null);
			forceCloseMethod.setAccessible(true);
			
			for (String fieldName : fileFieldNames) {
				Field fileField = BaseRecordManager.class.getDeclaredField(fieldName);
				fileField.setAccessible(true);
				Object recordFile = fileField.get(manager);
				forceCloseMethod.invoke(recordFile, (Object[])null);
			}
			
		} catch (Exception e) {
			throw new JetelRuntimeException(e);
		}
	}
}