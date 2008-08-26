/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.hsqldb.xls;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import jxl.BooleanCell;
import jxl.Cell;
import jxl.CellType;
import jxl.DateCell;
import jxl.JXLException;
import jxl.LabelCell;
import jxl.NumberCell;
import jxl.write.Blank;
import jxl.write.DateTime;
import jxl.write.Label;
import jxl.write.WritableCell;
import jxl.write.WritableSheet;

/**
 * @author vitek
 */
public class CellHelper {

    public static final String ERROR_STR = "#NA";
    public static final Date ERROR_DATE = new Date(-1L);
    
    //private static final String dateFormat = "yyyyMMdd";

    /**
     * 
     */
    public static void setCell(WritableSheet sheet, int col, int row,
            String value) throws JXLException {
        if (sheet == null) {
            return;
        }
        if (value == null) {
            sheet.addCell(new Blank(col, row));
            return;
        }
        
        WritableCell cell = sheet.getWritableCell(col, row);
        
        CellType cellType = (cell != null) ? cell.getType() : null;
        if (cellType == CellType.LABEL) {
            ((Label) cell).setString(value);
        } else {
            cell = new Label(col, row, value);
            sheet.addCell(cell);
        }
    }
    
    /**
     * 
     * @throws JXLException
     */
    public static void setCellNull(WritableSheet sheet, int col, int row)
    		throws JXLException {
        WritableCell cell = (WritableCell) sheet.getCell(col, row);
        if (cell != null && cell.getType() == CellType.EMPTY) {
            return;
        }
        sheet.addCell(new Blank(col, row));
    }
    
    /**
     * 
     */
    public static void setCell(WritableSheet sheet, int col, int row,
            Boolean value) throws JXLException {
        if (sheet == null) {
            return;
        }
        if (value == null) {
            sheet.addCell(new Blank(col, row));
            return;
        }

        WritableCell cell = sheet.getWritableCell(col, row);
        
        CellType cellType = (cell != null) ? cell.getType() : null;
        if (cellType == CellType.BOOLEAN) {
            ((jxl.write.Boolean) cell).setValue(value.booleanValue());
        } else {
            cell = new jxl.write.Boolean(col, row, value.booleanValue());
            sheet.addCell(cell);
        }
    }

    /**
     * 
     */
    public static void setCell(WritableSheet sheet, int col, int row,
            java.util.Date value) throws JXLException {
        if (sheet == null) {
            return;
        }
        if (value == null) {
            sheet.addCell(new Blank(col, row));
            return;
        }

        WritableCell cell = sheet.getWritableCell(col, row);
        
        CellType cellType = (cell != null) ? cell.getType() : null;
        if (cellType == CellType.DATE) {
            ((DateTime) cell).setDate(value);
        } else {
            cell = new DateTime(col, row, value);
            sheet.addCell(cell);
        }
    }
    
    /**
     * 
     */
    public static void setCell(WritableSheet sheet, int col, int row,
            java.lang.Number value) throws JXLException {
        if (sheet == null) {
            return;
        }
        if (value == null) {
            sheet.addCell(new Blank(col, row));
            return;
        }

        WritableCell cell = sheet.getWritableCell(col, row);
        
        CellType cellType = (cell != null) ? cell.getType() : null;
        if (cellType == CellType.NUMBER) {
            ((jxl.write.Number) cell).setValue(value.doubleValue());
        } else {
            cell = new jxl.write.Number(col, row, value.doubleValue());
            sheet.addCell(cell);
        }
    }
    
    /**
     * 
     * @param cell
     * @return
     */
    public static String stringValue(Cell cell) {
        if (cell == null) return null;
        
        CellType cellType = cell.getType(); 
        if (cellType == CellType.EMPTY) {
            return null;
        } else if (cellType == CellType.BOOLEAN) {
            return String.valueOf(((BooleanCell) cell).getValue());
        } else if (cellType == CellType.DATE) {
            return String.valueOf(((DateCell) cell).getDate());
    	} else if (cellType == CellType.ERROR) {
            return ERROR_STR;
    	} else if (cellType == CellType.FORMULA_ERROR
    			|| cellType == CellType.BOOLEAN_FORMULA
				|| cellType == CellType.NUMBER_FORMULA
				|| cellType == CellType.DATE_FORMULA
				|| cellType == CellType.STRING_FORMULA) {
            return ERROR_STR;
    	} else if (cellType == CellType.NUMBER) {
            return String.valueOf(((NumberCell) cell).getValue());
    	} else if (cellType == CellType.LABEL) {
            return ((LabelCell) cell).getString();
    	} else {
    		throw new IllegalStateException();
        }
    }

    private static final Double ZERO = new Double(0D);
    private static final Double ONE = new Double(1D);
    private static final Double ERROR_NUMBER = new Double(Double.MAX_VALUE);
    /**
     * 
     * @param cell
     * @return
     */
    public static Double numberValue(Cell cell) {
        if (cell == null) return null;
        
        CellType cellType = cell.getType(); 
        if (cellType == CellType.EMPTY) {
            return null;
        } else if (cellType == CellType.BOOLEAN) {
            return ((BooleanCell) cell).getValue() ? ONE : ZERO;
        } else if (cellType == CellType.DATE) {
            return ERROR_NUMBER;
    	} else if (cellType == CellType.ERROR) {
            return ERROR_NUMBER;
    	} else if (cellType == CellType.FORMULA_ERROR
    			|| cellType == CellType.BOOLEAN_FORMULA
				|| cellType == CellType.NUMBER_FORMULA
				|| cellType == CellType.DATE_FORMULA
				|| cellType == CellType.STRING_FORMULA) {
            return ERROR_NUMBER;
    	} else if (cellType == CellType.NUMBER) {
            return new Double(((NumberCell) cell).getValue());
    	} else if (cellType == CellType.LABEL) {
            try {
                return new Double(((LabelCell) cell).getString());
            } catch (NumberFormatException e) {
                return ERROR_NUMBER;
            }
    	} else {
    		throw new IllegalStateException();
        }
    }

    /**
     * 
     * @param cell
     * @return
     */
    public static Date dateValue(Cell cell, String format) {
        if (cell == null) return null;
        
        CellType cellType = cell.getType(); 
        if (cellType == CellType.EMPTY) {
            return null;
        } else if (cellType == CellType.BOOLEAN) {
            return ERROR_DATE;
        } else if (cellType == CellType.DATE) {
        	return new Date(((DateCell) cell).getDate().getTime());
		} else if (cellType == CellType.ERROR) {
            return ERROR_DATE;
    	} else if (cellType == CellType.FORMULA_ERROR
    			|| cellType == CellType.BOOLEAN_FORMULA
				|| cellType == CellType.NUMBER_FORMULA
				|| cellType == CellType.DATE_FORMULA
				|| cellType == CellType.STRING_FORMULA) {
            return ERROR_DATE;
    	} else if (cellType == CellType.NUMBER) {
    		return ERROR_DATE;
    	} else if (cellType == CellType.LABEL) {
            try {
                SimpleDateFormat sdf = (format != null)
                		? new SimpleDateFormat(format) : new SimpleDateFormat("yyMMdd");
                return new Date(sdf.parse(((LabelCell) cell).getString()).getTime());
            } catch (ParseException e) {
                return ERROR_DATE;
            }
    	} else {
    		throw new IllegalStateException();
        }
    }

    private static final Boolean ERROR_BOOL = null;
    /**
     * 
     * @param cell
     * @return
     */
    public static Boolean boolValue(Cell cell) {
        if (cell == null) return null;
        
        CellType cellType = cell.getType(); 
        if (cellType == CellType.EMPTY) {
            return null;
        } else if (cellType == CellType.BOOLEAN) {
            return new Boolean(((BooleanCell) cell).getValue());
        } else if (cellType == CellType.DATE) {
            return ERROR_BOOL;
    	} else if (cellType == CellType.ERROR) {
            return ERROR_BOOL;
    	} else if (cellType == CellType.FORMULA_ERROR
    			|| cellType == CellType.BOOLEAN_FORMULA
				|| cellType == CellType.NUMBER_FORMULA
				|| cellType == CellType.DATE_FORMULA
				|| cellType == CellType.STRING_FORMULA) {
            return ERROR_BOOL;
    	} else if (cellType == CellType.NUMBER) {
            return new Boolean(((NumberCell) cell).getValue() != 0D);
    	} else if (cellType == CellType.LABEL) {
            return parseBoolean(((LabelCell) cell).getString());
    	} else {
    		throw new IllegalStateException();
        }
    }
    
    private static Boolean parseBoolean(String s) {
        
        if (s == null) return null;
        
        if (s.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (s.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        
        if (s.equalsIgnoreCase("yes")) {
            return Boolean.TRUE;
        }
        if (s.equalsIgnoreCase("no")) {
            return Boolean.FALSE;
        }
        
        try {
            double d = Double.parseDouble(s);
            if (d == 1D) {
                return Boolean.TRUE;
            }
            if (d == 0D) {
                return Boolean.FALSE;
            }
        } catch (NumberFormatException e) {
            // empty
        }

        return null;
    }

}
