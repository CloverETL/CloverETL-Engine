/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package cz.opentech.jdbc.xlsdriver.db.util;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import jxl.BooleanCell;
import jxl.Cell;
import jxl.CellType;
import jxl.DateCell;
import jxl.LabelCell;
import jxl.NumberCell;

/**
 * @author vitek
 */
public class CellHelper {

    public static final String ERROR_STR = "#NA";
    public static final Date ERROR_DATE = new Date(-1L);

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
