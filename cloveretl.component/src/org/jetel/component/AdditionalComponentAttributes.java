/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
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
