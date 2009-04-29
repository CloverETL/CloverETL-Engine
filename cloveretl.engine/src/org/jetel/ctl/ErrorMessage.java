package org.jetel.ctl;

import java.io.Serializable;

public class ErrorMessage implements Serializable {

	private static final long serialVersionUID = 2385458414727702852L;

	public enum ErrorLevel {
		WARN("Warning"),
		ERROR("Error");
		
		private String name;
		
		private ErrorLevel(String levelName) {
			this.name = levelName;
		}
		
		public String getName() {
			return name;
		}
	}
	
	private String fileURL;
	private ErrorLevel level;
	private int beginLine;
	private int beginColumn;
	private int endLine;
	private int endColumn;
	private String errorMessage;
	private String hint;

	public ErrorMessage(String fileURL, ErrorLevel level, int line, int column, int endLine, int endColumn, String errorMessage, String hint) {
		this.fileURL = fileURL;
		this.level = level;
		this.beginLine = line;
		this.beginColumn = column;
		this.endLine = endLine;
		this.endColumn = endColumn;
		this.errorMessage = errorMessage;
		this.hint = hint;
	}
	
	
	@Override
	public String toString() {
		return (
				level.getName()
				+ (fileURL != null ? " in '" + fileURL + "'" : "" ) 
				+ ": Line " + beginLine
				+ " column " + beginColumn
				+ " - Line " + endLine
				+ " column " + endColumn
				+ ": " + errorMessage + ". " 
				+ ( hint != null ? "[ " + hint  + ". ]" : "")
			   );  
	}
	
	public ErrorLevel getErrorLevel() {
		return level;
	}
	
	public String getFileURL() {
		return fileURL;
	}
	
	public int getBeginLine() {
		return beginLine;
	}
	
	public int getBeginColumn() {
		return beginColumn;
	}
	
	public int getEndLine() {
		return endLine;
	}

	public int getEndColumn() {
		return endColumn;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}
	
	public String getHint() {
		return hint;
	}
	
}
