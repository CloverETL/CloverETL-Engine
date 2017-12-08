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
package org.jetel.component.fileoperation;

import java.io.IOException;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jetel.exception.JetelRuntimeException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleParameters implements Serializable, Parameters, Cloneable {
	
	private static final long serialVersionUID = -1597353175731941561L;
	
	// so far, no subclass supports more than 3 parameters, use initial size of 5
	private Map<String, Object> map = new HashMap<String, Object>(5);
	
	@Override
	public Object get(String key) {
		return map.get(key);
	}

	@Override
	public Parameters set(String key, Object value) {
		map.put(key, value);
		return this;
	}
	
	@Override
	public Set<String> keys() {
		return Collections.unmodifiableSet(map.keySet());
	}
	
	@Override
	public SimpleParameters clone() {
		try {
			SimpleParameters clone = (SimpleParameters) super.clone();
			clone.map = new HashMap<String, Object>();
			clone.map.putAll(this.map);
			return clone;
		} catch (CloneNotSupportedException ex) {
			throw new RuntimeException(FileOperationMessages.getString("SimpleParameters.clone_failed"), ex); //$NON-NLS-1$
		}
	}

	@Override
	public String toString() {
		return map.toString();
	}

	public static class ListParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 7287530644468245893L;
		
		public static final String RECURSIVE = "recursive"; //$NON-NLS-1$
		public static final String LIST_DIRECTORY_CONTENTS = "listDirectoryContents"; //$NON-NLS-1$
		
		public ListParameters() {
			set(RECURSIVE, false);
			set(LIST_DIRECTORY_CONTENTS, true);
		}

		public ListParameters setRecursive(Boolean recursive) {
			return (ListParameters) set(RECURSIVE, recursive);
		}
		
		public boolean isRecursive() {
			return (Boolean) get(RECURSIVE);
		}

		public ListParameters setListDirectoryContents(Boolean listDirectoryContents) {
			return (ListParameters) set(LIST_DIRECTORY_CONTENTS, listDirectoryContents);
		}
		
		public boolean isListDirectoryContents() {
			return (Boolean) get(LIST_DIRECTORY_CONTENTS);
		}
	}

	public static class DeleteParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = -8115947854087985478L;
		
		public static final String RECURSIVE = "recursive"; //$NON-NLS-1$
		
		public DeleteParameters() {
			set(RECURSIVE, false);
		}

		public DeleteParameters setRecursive(Boolean recursive) {
			return (DeleteParameters) set(RECURSIVE, recursive);
		}
		
		public Boolean isRecursive() {
			return (Boolean) get(RECURSIVE);
		}
	}

	public static class MoveParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 3337277685618520468L;
		
		public static final String OVERWRITE = "overwrite"; //$NON-NLS-1$
		public static final String MAKE_PARENT_DIRS = "makeParentDirs"; //$NON-NLS-1$
		
		public MoveParameters() {
			set(OVERWRITE, OverwriteMode.ALWAYS);
		}
		
		@Override
		public MoveParameters set(String key, Object value) {
			return (MoveParameters) super.set(key, value);
		}

		public OverwriteMode getOverwriteMode() {
			return (OverwriteMode) get(OVERWRITE);
		}
		
		public MoveParameters setOverwriteMode(OverwriteMode mode) {
			return set(OVERWRITE, mode);
		}
		
		public boolean isUpdate() {
			return OverwriteMode.UPDATE.equals(getOverwriteMode());
		}

		public boolean isNoOverwrite() {
			return OverwriteMode.NEVER.equals(getOverwriteMode());
		}

		public MoveParameters setUpdate() {
			return setOverwriteMode(OverwriteMode.UPDATE);
		}
		
		public MoveParameters setNoOverwrite() {
			return setOverwriteMode(OverwriteMode.NEVER);
		}

		public MoveParameters setMakeParents(Boolean directory) {
			return (MoveParameters) set(MAKE_PARENT_DIRS, directory);
		}
		
		public Boolean isMakeParents() {
			return (Boolean) get(MAKE_PARENT_DIRS);
		}
	}
	
	public static enum OverwriteMode {
		ALWAYS, // always overwrite
		UPDATE, // overwrite if newer
		NEVER; // never overwrite
		
		public static OverwriteMode fromStringIgnoreCase(String string) {
			for (OverwriteMode mode: values()) {
				if (mode.toString().equalsIgnoreCase(string)) {
					return mode;
				}
			}
			
			throw new IllegalArgumentException(MessageFormat.format(FileOperationMessages.getString("SimpleParameters.unknown_overwrite_mode"), string)); //$NON-NLS-1$
		}
	}

	public static class CopyParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 1L;
		
		public static final String RECURSIVE = "recursive"; //$NON-NLS-1$
		public static final String OVERWRITE = "overwrite"; //$NON-NLS-1$
		public static final String MAKE_PARENT_DIRS = "makeParentDirs"; //$NON-NLS-1$
		
		public CopyParameters() {
			set(RECURSIVE, false);
			set(OVERWRITE, OverwriteMode.ALWAYS);
		}
		
		@Override
		public CopyParameters set(String key, Object value) {
			return (CopyParameters) super.set(key, value);
		}

		@Override
		public CopyParameters clone() {
			return (CopyParameters) super.clone();
		}

		public CopyParameters setRecursive(Boolean recursive) {
			return set(RECURSIVE, recursive);
		}
		
		public Boolean isRecursive() {
			return (Boolean) get(RECURSIVE);
		}

		public OverwriteMode getOverwriteMode() {
			return (OverwriteMode) get(OVERWRITE);
		}
		
		public CopyParameters setOverwriteMode(OverwriteMode mode) {
			return set(OVERWRITE, mode);
		}
		
		public boolean isUpdate() {
			return OverwriteMode.UPDATE.equals(getOverwriteMode());
		}

		public boolean isNoOverwrite() {
			return OverwriteMode.NEVER.equals(getOverwriteMode());
		}

		public CopyParameters setUpdate() {
			return setOverwriteMode(OverwriteMode.UPDATE);
		}
		
		public CopyParameters setNoOverwrite() {
			return setOverwriteMode(OverwriteMode.NEVER);
		}
		
		public CopyParameters setMakeParents(Boolean directory) {
			return (CopyParameters) set(MAKE_PARENT_DIRS, directory);
		}
		
		public Boolean isMakeParents() {
			return (Boolean) get(MAKE_PARENT_DIRS);
		}
	}

	public static class CreateParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public CreateParameters clone() {
			return (CreateParameters) super.clone();
		}

		public static final String IS_DIRECTORY = "isDirectory"; //$NON-NLS-1$
		public static final String LAST_MODIFIED = "lastModified"; //$NON-NLS-1$
		public static final String MAKE_PARENT_DIRS = "makeParentDirs"; //$NON-NLS-1$
		
		public CreateParameters() {
			set(IS_DIRECTORY, false);
			set(MAKE_PARENT_DIRS, false);
		}
		
		public CreateParameters setDirectory(Boolean directory) {
			return (CreateParameters) set(IS_DIRECTORY, directory);
		}
		
		public Boolean isDirectory() {
			return (Boolean) get(IS_DIRECTORY);
		}
		
		public CreateParameters setMakeParents(Boolean directory) {
			return (CreateParameters) set(MAKE_PARENT_DIRS, directory);
		}
		
		public Boolean isMakeParents() {
			return (Boolean) get(MAKE_PARENT_DIRS);
		}
		
		public CreateParameters setLastModified(Date lastModified) {
			return (CreateParameters) set(LAST_MODIFIED, lastModified);
		}
		
		public Date getLastModified() {
			return (Date) get(LAST_MODIFIED);
		}
	}

	public static class InfoParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 228106520703846942L;

		@Override
		public InfoParameters clone() {
			return (InfoParameters) super.clone();
		}

	}

	public static class ResolveParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 361471721261204946L;

		@Override
		public ResolveParameters clone() {
			return (ResolveParameters) super.clone();
		}

	}

	public static class ReadParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = -1499640681705748118L;

		@Override
		public ReadParameters clone() {
			return (ReadParameters) super.clone();
		}

	}

	public static class WriteParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 5978094017352252524L;

		@Override
		public WriteParameters clone() {
			return (WriteParameters) super.clone();
		}

	}

	public static class FileParameters extends SimpleParameters implements Serializable {
		private static final long serialVersionUID = 1257715659014888845L;

		@Override
		public WriteParameters clone() {
			return (WriteParameters) super.clone();
		}

	}
}
