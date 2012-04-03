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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An auxiliary class used as a dirty fix solving non-visibility of commercial components from
 * a package containing FileComponentFactory 
 * 
 * See issues CLD-3314, CLD-3435 
 * 
 * @author Pavel Simecek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.2.2012
 */
public enum AdditionalComponentAttributes {
	
	// Components are listed with descending priority (for common file extension(s))
	SPREADSHEET_DATA_READER("SPREADSHEET_READER", true, "xls", "xlsx"),
	SPREADSHEET_DATA_WRITER("SPREADSHEET_WRITER", false, "xls", "xlsx"),
	XLS_READER("XLS_READER", true, "xls", "xlsx"),
	XLS_WRITER("XLS_WRITER", false, "xls", "xlsx"),
	JSON_READER("JSON_READER", true, "json");
	
	static public enum ComponentOperation {
		READER, WRITER;
	}
	
	private final String componentType;
	private boolean reader;
	private final String[] fileExtentions;

	private AdditionalComponentAttributes(String componentType, boolean reader, String... fileExtentions) {
		this.componentType = componentType;
		this.reader = reader;
		this.fileExtentions = fileExtentions;
	}

	private static Map<String, String []> componentTypeToExtensions;
	private static Map<String, List<AdditionalComponentAttributes>> extensionToReader;
	private static Map<String, List<AdditionalComponentAttributes>> extensionToWriter;
	
	static {
		componentTypeToExtensions = new HashMap<String, String []>();
		extensionToReader = new HashMap<String, List<AdditionalComponentAttributes>>();
		extensionToWriter = new HashMap<String, List<AdditionalComponentAttributes>>();
		for (AdditionalComponentAttributes additionalComponentAttributes : AdditionalComponentAttributes.values()) {
			componentTypeToExtensions.put(getComponentType(additionalComponentAttributes), getFileExtensions(additionalComponentAttributes));
			
			if (isReader(additionalComponentAttributes)) {
				registerComonentAttribute(additionalComponentAttributes, extensionToReader);
			}
			if (isWriter(additionalComponentAttributes)) {
				registerComonentAttribute(additionalComponentAttributes, extensionToWriter);
			}
		}
	}

	private static void registerComonentAttribute(AdditionalComponentAttributes additionalComponentAttributes, Map<String, List<AdditionalComponentAttributes>> register) {
		for (String extension : getFileExtensions(additionalComponentAttributes)) {
			List<AdditionalComponentAttributes> componentAttributes = register.get(extension);
			if (componentAttributes == null) {
				componentAttributes = new LinkedList<AdditionalComponentAttributes>();
				register.put(extension, componentAttributes);
			}
			componentAttributes.add(additionalComponentAttributes);
		}
	}
	
	public static String getComponentType(AdditionalComponentAttributes additionalComponentAttributes) {
		return additionalComponentAttributes.getComponentType();
	}
	
	public String getComponentType() {
		return componentType;
	}
	
	public static String [] getFileExtensions(AdditionalComponentAttributes additionalComponentAttributes) {
		return additionalComponentAttributes.getFileExtensions();
	}
	
	public String [] getFileExtensions() {
		return fileExtentions;
	}
	
	public static boolean isReader(AdditionalComponentAttributes additionalComponentAttributes) {
		return additionalComponentAttributes.isReader();
	}
	
	public boolean isReader() {
		return reader;
	}
	
	public static boolean isWriter(AdditionalComponentAttributes additionalComponentAttributes) {
		return additionalComponentAttributes.isWriter();
	}
	
	public boolean isWriter() {
		return !reader;
	}
	
	public static String [] getFileExtensionsByComponentType(String componentType) {
		return componentTypeToExtensions.get(componentType);
	}
	
	public static List<AdditionalComponentAttributes> getReaderByFileExtension(String extension) {
		return extensionToReader.get(extension);
	}
	
	public static List<AdditionalComponentAttributes> getWriterByFileExtension(String extension) {
		return extensionToWriter.get(extension);
	}
	
	public static List<AdditionalComponentAttributes> getComponentByFileExtension(String extension, ComponentOperation componentOperation) {
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
