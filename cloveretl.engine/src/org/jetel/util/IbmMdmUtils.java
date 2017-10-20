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
package org.jetel.util;

import java.util.Arrays;
import java.util.List;

/**
 * @author adamekl (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 10. 2017
 */
public class IbmMdmUtils {
	
	public static final String IBM_MEM_DICTIONARY_READER = "MEM_DICTIONARY_READER";
	public static final String IBM_MEMGET_DATA_READER = "MEMGET_DATA_READER";
	public static final String IBM_MEMSEARCH = "MEMSEARCH";
	public static final String IBM_TSKGET_DATA_READER = "TSKGET_DATA_READER";
	public static final String IBM_TSKSEARCH = "TSKSEARCH";
	
	public static final List<String> IBM_MDM_READERS = Arrays.asList(
			IBM_MEM_DICTIONARY_READER,
			IBM_MEMGET_DATA_READER,
			IBM_MEMSEARCH,
			IBM_TSKGET_DATA_READER,
			IBM_TSKSEARCH);
	
	public static boolean isIbmMdmReader(String componentType) {
		return IBM_MDM_READERS.contains(componentType);
	}
	
	public static List<String> getIbmMdmReaders() {
		return IBM_MDM_READERS;
	}

}
