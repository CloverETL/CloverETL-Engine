
package org.jetel.data.formatter;

public interface XLSFormatter extends Formatter {
	
	public void prepareSheet();

	public void setSheetName(String sheetName);

	public void setSheetNumber(int sheetNumber);
	
	public void setFirstRow(int firstRow);

	public void setFirstColumn(String firstColumn);

	public int getFirstColumn();

	public void setNamesRow(int namesRow);

	public boolean isAppend();

	public int getFirstRow() ;

	public int getNamesRow();

	public String getSheetName();

	public int getSheetNumber();
}
