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
package org.jetel.data.formatter.provider;

import java.io.InputStream;
import java.net.URL;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.SpreadsheetFormatter;
import org.jetel.data.parser.XLSMapping;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.SpreadsheetUtils.SpreadsheetAttitude;
import org.jetel.util.SpreadsheetUtils.SpreadsheetFormat;
import org.jetel.util.file.FileUtils;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6 Sep 2011
 */
public class SpreadsheetFormatterProvider implements FormatterProvider {
	
	private SpreadsheetAttitude attitude;
	private SpreadsheetFormat formatterType;
	
	private Workbook templateWorkbook;
	private String sheet;
	private XLSMapping mapping;
	
	private boolean append;
	private boolean insert;
	private boolean removeSheets;

	@Override
	public Formatter getNewFormatter() {
		SpreadsheetFormatter formatter = new SpreadsheetFormatter();
		formatter.setAttitude(attitude);
		formatter.setFormatterType(formatterType);
		formatter.setTemplateWorkbook(templateWorkbook);
		formatter.setSheet(sheet);
		formatter.setMapping(mapping);
		formatter.setAppend(append);
		formatter.setInsert(insert);
		formatter.setRemoveSheets(removeSheets);
		return formatter;
	}
	
	public void setAttitude(SpreadsheetAttitude attitude) {
		this.attitude = attitude;
	}
	
	public void setFormatterType(SpreadsheetFormat formatterType) {
		this.formatterType = formatterType;
	}
	
	public void setTemplateFile(URL contextURL, String templateFileURL) throws ComponentNotReadyException {
		try {
			InputStream stream = FileUtils.getInputStream(contextURL, templateFileURL);
			this.templateWorkbook = WorkbookFactory.create(stream);
		} catch (Exception e) {
			throw new ComponentNotReadyException("Failed to prepare template!", e);
		}
	}

	public void setSheet(String sheet) {
		this.sheet = sheet;
	}

	public void setMapping(XLSMapping mapping) {
		this.mapping = mapping;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public void setInsert(boolean insert) {
		this.insert = insert;
	}

	public void setRemoveSheets(boolean removeSheets) {
		this.removeSheets = removeSheets;
	}
}
