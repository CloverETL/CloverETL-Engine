package org.jetel.ctl;

import java.util.LinkedList;
import java.util.List;

import org.jetel.ctl.ErrorMessage.ErrorLevel;

/**
 * Collector of diagnostic messages. Server also for passing the information
 * about error occurrence between CTL phases.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class ProblemReporter {

	/** List of diagnostic messages collected by different phases of the compiler */
	private List<ErrorMessage> diagnosticMessages = new LinkedList<ErrorMessage>();
	/** URL of import file (being parsed) for which the error messages apply */
	private String importFileURL;
	
	private int errorCount = 0;
	private int warningCount = 0;
	
    public void warn(int beginLine, int beginColumn, int endLine, int endColumn, String error, String hint) {
        createMessage(ErrorLevel.WARN,beginLine,beginColumn,endLine,endColumn,error,hint);
    }
    
    public void warn(SyntacticPosition begin, SyntacticPosition end, String error, String hint) {
    	createMessage(ErrorLevel.WARN,begin.getLine(),begin.getColumn(),
    			end.getLine(),end.getColumn(),error,hint);
    }
    
    public void error(int beginLine, int beginColumn, int endLine, int endColumn, String error, String hint) {
    	createMessage(ErrorLevel.ERROR,beginLine,beginColumn,endLine,endColumn,error,hint);
    }

    public void error(SyntacticPosition begin, SyntacticPosition end, String error, String hint) {
    	createMessage(ErrorLevel.ERROR,begin.getLine(),begin.getColumn(),
    			end.getLine(),end.getColumn(),error,hint);
    }
    
    public int errorCount() {
    	return errorCount;
    }
    
    public int warningCount() {
		return warningCount;
	}
    
    public List<ErrorMessage> getDiagnosticMessages() {
		return diagnosticMessages;
	}
    
    private void createMessage(ErrorLevel level, int beginLine, int beginColumn, int endLine, int endColumn, String error, String hint) {
    	if (level == ErrorLevel.ERROR) {
    		errorCount++;
    	} else {
    		warningCount++;
    	}
    	diagnosticMessages.add(new ErrorMessage(importFileURL,level,beginLine,beginColumn,endLine, endColumn, error,hint));
    }
    
    
    
    /**
     * Sets the URL of import file being currently processed.
     * All errors reported will be related to this file.
     * Set to <code>null</code> for 'current' file
     * 
     * @param importFileURL URL of import file being processed or <code>null</code> for current file
     */
    public void setImport(String importFileURL) {
		this.importFileURL = importFileURL;
	}

    
    public void reset() {
    	diagnosticMessages.clear();
    	errorCount = 0;
    	warningCount = 0;
    }
}
