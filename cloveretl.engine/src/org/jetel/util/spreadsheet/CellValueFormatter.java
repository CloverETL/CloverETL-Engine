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
package org.jetel.util.spreadsheet;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Feb 10, 2012
 */
public class CellValueFormatter {

	private Map<String, DataFormatter> formatters = new HashMap<String, DataFormatter>();
	
	public CellValueFormatter() {
		DataFormatter defaultLocaleFormatter = new OurDataFormatter();
		defaultLocaleFormatter.addFormat("General", new DecimalFormat("#.############")); // take 2 more decimal places than there are in default
		formatters.put(null, defaultLocaleFormatter);
	}
	
	public String formatRawCellContents(double cellValue, int formatIndex, String formatString, String localeString) {
		DataFormatter formatter = getLocalizedDataFormater(localeString);
		return formatter.formatRawCellContents(cellValue, formatIndex, formatString);
	}

	private DataFormatter getLocalizedDataFormater(String localeString) {
		DataFormatter formatter = formatters.get(localeString);
		if (formatter == null) {
			String[] localParts = localeString.split("\\.");
			Locale locale;
			if (localParts.length > 1) {
				locale = new Locale(localParts[0], localParts[1]);
			} else {
				locale = new Locale(localParts[0]);
			}
			formatter = new OurDataFormatter(locale);
			formatters.put(localeString, formatter);
		}
		return formatter;
	}
	
	public String formatCellValue(Cell cell, FormulaEvaluator formulaEvaluator, String locale) {
		DataFormatter formatter = getLocalizedDataFormater(locale);
		return formatter.formatCellValue(cell, formulaEvaluator);
	}
	
	public static class OurDataFormatter extends DataFormatter {
		
		private static Log logger = LogFactory.getLog(OurDataFormatter.class);
		
	    private static final Pattern FRAC_PATTERN = Pattern.compile("\\?+/[\\d+|\\?+]");
	    
	    public OurDataFormatter() {
	    	super();
	    }
	    
		public OurDataFormatter(Locale locale) {
			super(locale);
		}
	    
		@Override
		public String formatRawCellContents(double value, int formatIndex, String formatString) {  // TODO boolean use1904Windowing
			if (formatString != null) {
				Matcher fracMatcher = FRAC_PATTERN.matcher(formatString);
				if (fracMatcher.find()) {
					// convert fractions to standard double
					formatString = "0.00";
				}
			}
			else {
				logger.warn("Null format obtained, using default double 0.00 format instead");
				formatString = "0.00";
			}
			return super.formatRawCellContents(value, formatIndex, formatString).replaceFirst("\\* ", "");
		}

		@Override
	    public String formatCellValue(Cell cell, FormulaEvaluator evaluator) {

	        if (cell == null) {
	            return "";
	        }

	        int cellType = cell.getCellType();
	        if (cellType == Cell.CELL_TYPE_FORMULA) {
	            if (evaluator == null) {
	                return cell.getCellFormula();
	            }
	            cellType = evaluator.evaluateFormulaCell(cell);
	        }
	        switch (cellType) {
	            case Cell.CELL_TYPE_NUMERIC :
	                return getFormattedNumberString(cell);
	            case Cell.CELL_TYPE_STRING :
	                return cell.getRichStringCellValue().getString();
	            case Cell.CELL_TYPE_BOOLEAN :
	                return String.valueOf(cell.getBooleanCellValue());
	            case Cell.CELL_TYPE_BLANK :
	                return "";
	        }
	        throw new RuntimeException("Unexpected celltype (" + cellType + ")");
	    }
		
	    private String getFormattedNumberString(Cell cell) {
	    	CellStyle cellStyle = cell.getCellStyle();
	    	return formatRawCellContents(cell.getNumericCellValue(), cellStyle.getDataFormat(), cellStyle.getDataFormatString());
	    }
	    
	}

}
