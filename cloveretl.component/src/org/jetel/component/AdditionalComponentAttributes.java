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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An auxiliary class used as a dirty fix solving non-visibility of commercial components from
 * a package containing FileComponentFactory 
 * 
 * @author Pavel Simecek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.2.2012
 */
public enum AdditionalComponentAttributes {
	SPREADSHEET_DATA_READER,
	SPREADSHEET_DATA_WRITER;
	
	static public enum ComponentOperation {
		READER, WRITER;
	}
	
	private static final String SPREADSHEET_DATA_READER_COMPONENT_TYPE = "SPREADSHEET_READER";
	private static final String SPREADSHEET_DATA_WRITER_COMPONENT_TYPE = "SPREADSHEET_WRITER";
	
	private static final String [] SPREADSHEET_DATA_READER_EXTENSIONS = { "xls", "xlsx"};
	private static final String [] SPREADSHEET_DATA_WRITER_EXTENSIONS = { "xls", "xlsx"};
	
	private static Map<String, String []> componentTypeToExtensions;
	private static Map<String, AdditionalComponentAttributes> extensionToReader;
	private static Map<String, AdditionalComponentAttributes> extensionToWriter;
	
	static {
		componentTypeToExtensions = new HashMap<String, String []>();
		extensionToReader = new HashMap<String, AdditionalComponentAttributes>();
		extensionToWriter = new HashMap<String, AdditionalComponentAttributes>();
		for (AdditionalComponentAttributes additionalComponentAttributes : AdditionalComponentAttributes.values()) {
			componentTypeToExtensions.put(getComponentType(additionalComponentAttributes), getFileExtensions(additionalComponentAttributes));
			if (isReader(additionalComponentAttributes)) {
				for (String extension : getFileExtensions(additionalComponentAttributes)) {
					extensionToReader.put(extension, additionalComponentAttributes);
				}
			}
			if (isWriter(additionalComponentAttributes)) {
				for (String extension : getFileExtensions(additionalComponentAttributes)) {
					extensionToWriter.put(extension, additionalComponentAttributes);
				}
			}
		}
	}
	
	public static String getComponentType(AdditionalComponentAttributes additionalComponentAttributes) {
		if (additionalComponentAttributes == SPREADSHEET_DATA_READER) {
			return SPREADSHEET_DATA_READER_COMPONENT_TYPE;
		} else if (additionalComponentAttributes == SPREADSHEET_DATA_WRITER){
			return  SPREADSHEET_DATA_WRITER_COMPONENT_TYPE;
		} else {
			return null;
		}
	}
	
	public String getComponentType() {
		return getComponentType(this);
	}
	
	public static String [] getFileExtensions(AdditionalComponentAttributes additionalComponentAttributes) {
		if (additionalComponentAttributes == SPREADSHEET_DATA_READER) {
			return SPREADSHEET_DATA_READER_EXTENSIONS;
		} else if (additionalComponentAttributes == SPREADSHEET_DATA_WRITER){
			return  SPREADSHEET_DATA_WRITER_EXTENSIONS;
		} else {
			return null;
		}
	}
	
	public String [] getFileExtensions() {
		return getFileExtensions(this);
	}
	
	public static boolean isReader(AdditionalComponentAttributes additionalComponentAttributes) {
		if (additionalComponentAttributes == SPREADSHEET_DATA_READER) {
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isReader() {
		return isReader(this);
	}
	
	public static boolean isWriter(AdditionalComponentAttributes additionalComponentAttributes) {
		if (additionalComponentAttributes == SPREADSHEET_DATA_WRITER) {
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isWriter() {
		return isWriter(this);
	}
	
	public static String [] getFileExtensionsByComponentType(String componentType) {
		return componentTypeToExtensions.get(componentType);
	}
	
	public static AdditionalComponentAttributes getReaderByFileExtension(String extension) {
		return extensionToReader.get(extension);
	}
	
	public static AdditionalComponentAttributes getWriterByFileExtension(String extension) {
		return extensionToWriter.get(extension);
	}
	
	public static AdditionalComponentAttributes getComponentByFileExtension(String extension, ComponentOperation componentOperation) {
		if (componentOperation == ComponentOperation.READER) {
			return getReaderByFileExtension(extension);
		} else {
			return getWriterByFileExtension(extension);
		}
	}
	
	public static Set<String> getSupportedReaderExtensions() {
		return extensionToReader.keySet();
	}
	
	public static Set<String> getSupportedWriterExtensions() {
		return extensionToWriter.keySet();
	}
	
	public static Set<String> getSupportedExtensions(ComponentOperation componentOperation) {
		if (componentOperation == ComponentOperation.READER) {
			return getSupportedReaderExtensions();
		} else {
			return getSupportedWriterExtensions();
		}
	}
}
