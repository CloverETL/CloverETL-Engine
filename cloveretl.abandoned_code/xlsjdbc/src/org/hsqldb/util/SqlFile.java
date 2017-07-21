/* Copyright (c) 2001-2004, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.util;

import java.io.IOException;
import java.io.PrintStream;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;

/* $Id$ */

/**
 * Encapsulation of a sql text file like 'myscript.sql'.
 * The ultimate goal is to run the execute() method to feed the SQL
 * commands within the file to a jdbc connection.
 *
 * Some implementation comments and variable names use keywords based
 * on the following definitions.  <UL>
 * <LI> COMMAND = Statement || SpecialCommand || BufferCommand
 * Statement = SQL statement like "SQL Statement;"
 * SpecialCommand =  Special Command like "\x arg..."
 * BufferCommand =  Editing/buffer command like ":s/this/that/"
 *
 * When entering SQL statements, you are always "appending" to the
 * "current" command (not the "buffer", which is a different thing).
 * All you can do to the current command is append new lines to it,
 * execute it, or save it to buffer.
 *
 * In general, the special commands mirror those of Postgresql's psql,
 * but SqlFile handles command editing much different from Postgresql
 * because of Java's lack of support for raw tty I/O.
 * The \p special command, in particular, is very different from psql's.
 * Also, to keep the code simpler, we're sticking to only single-char
 * special commands until we really need more.
 *
 * Buffer commands are uniqueue to SQLFile.  The ":" commands allow
 * you to edit the buffer and to execute the buffer.
 *
 * The command history consists only of SQL Statements (i.e., special
 * commands and editing commands are not stored for later viewing or
 * editing).
 *
 * Most of the Special Commands and all of the Editing Commands are for
 * interactive use only.
 *
 * @version $Revision$
 * @author Blaine Simpson
 */
public class SqlFile {

    private static final int DEFAULT_HISTORY_SIZE = 20;
    private File             file;
    private boolean          interactive;
    private String           primaryPrompt    = "sql> ";
    private String           contPrompt       = "  +> ";
    private Connection       curConn          = null;
    private boolean          htmlMode         = false;
    private HashMap          userVars         = null;
    private String[]         statementHistory = null;

    /**
     * Private class to "share" a variable among a family of SqlFile
     * instances.
     */
    private static class BooleanBucket {

        private boolean bPriv = false;
        public void set(boolean bIn) {
            bPriv = bIn;
        }

        public boolean get() {
            return bPriv;
        }
    }

    // This is an imperfect solution since when user runs SQL they could
    // be running DDL or a commit or rollback statement.  All we know is,
    // they MAY run some DML that needs to be committed.
    BooleanBucket possiblyUncommitteds = new BooleanBucket();

    // Ascii field separator blanks
    private final static int SEP_LEN = 2;
    private final static String DIVIDER =
        "-----------------------------------------------------------------"
        + "-----------------------------------------------------------------";
    private final static String SPACES =
        "                                                                 "
        + "                                                                 ";
    private static String revnum = null;
    static {
        revnum = "$Revision$".substring("$Revision: ".length(),
                                               "$Revision$".length()
                                               - 2);
    }
    private static String BANNER =
        "(SqlFile processor v. " + revnum + ")\n"
        + "Distribution is permitted under the terms of the HSQLDB license.\n"
        + "(c) 2004 Blaine Simpson and the HSQLDB Development Group.\n\n"
        + "    \\q    to Quit.\n" + "    \\?    lists Special Commands.\n"
        + "    :?    lists Buffer/Editing commands.\n"
        + "    * ?   lists PL commands (including alias commands).\n\n"
        + "SPECIAL Commands begin with '\\' and execute when you hit ENTER.\n"
        + "BUFFER Commands begin with ':' and execute when you hit ENTER.\n"
        + "COMMENTS begin with '/*' and end with the very next '*/'.\n"
        + "PROCEDURAL LANGUAGE commands begin with '* ' and end when you hit ENTER.\n"
        + "All other lines comprise SQL Statements.\n"
        + "  SQL Statements are terminated by either a blank line (which moves the\n"
        + "  statement into the buffer without executing) or a line ending with ';'\n"
        + "  (which executes the statement).\n";
    private final static String BUFFER_HELP_TEXT =
        "BUFFER Commands (only available for interactive use).\n\n"
        + "    :?                Help\n"
        + "    :;                Execute current buffer as an SQL Statement\n"
        + "    :a[text]          Enter append mode with a copy of the buffer\n"
        + "    :l                List current contents of buffer\n"
        + "    :s/from/to        Substitute \"to\" for first occurrence of \"from\"\n"
        + "    :s/from/to/[i;g2] Substitute \"to\" for occurrence(s) of \"from\"\n"
        + "                from:  '$'s represent line breaks\n"
        + "                to:    If empty, from's will be deleted (e.g. \":s/x//\").\n"
        + "                       '$'s represent line breaks\n"
        + "                       You can't use ';' in order to execute the SQL (use\n"
        + "                       the ';' switch for this purpose, as explained below).\n"
        + "                /:     Can actually be any character which occurs in\n"
        + "                       neither \"to\" string nor \"from\" string.\n"
        + "                SUBSTITUTION MODE SWITCHES:\n"
        + "                       i:  case Insensitive\n"
        + "                       ;:  execute immediately after substitution\n"
        + "                       g:  Global (substitute ALL occurrences of \"from\" string)\n"
        + "                       2:  Narrows substitution to specified buffer line number\n"
        + "                           (Use any line number in place of '2').\n"
    ;
    private final static String HELP_TEXT = "SPECIAL Commands.\n"
        + "* commands only available for interactive use.\n"
        + "In place of \"3\" below, you can use nothing for the previous command, or\n"
        + "an integer \"X\" to indicate the Xth previous command.\n\n"
        + "    \\?                   Help\n"
        + "    \\p [line to print]   Print string to stdout\n"
        + "    \\w file/path.sql     Append current buffer to file\n"
        + "    \\i file/path.sql     Include/execute commands from external file\n"
        + "    \\d{tv*sa} [substr]   List names of Tbls/Views/all/System Tbls/Aliases\n"
        + "    \\d OBJECTNAME        Describe table or view\n"
        + "    \\o [file/path.html]  Tee (or stop teeing) query output to specified file\n"
        + "    \\H                   Toggle HTML output mode\n"
        + "    \\! COMMAND ARGS      Execute external program (no support for stdin)\n"
        + "    \\* [true|false]      Continue upon errors (a.o.t. abort upon error)\n"
        + "    \\a [true|false]      Auto-commit JDBC DML commands\n"
        + "    \\s                   * Show previous commands (i.e. SQL command history)\n"
        + "    \\-[3]                * reload a command to buffer (for : commands)\n"
        + "    \\-[3];               * reload command and execute (via \":;\")\n"
        + "    \\q [abort message]   Quit (alternatively, end input like Ctrl-Z or Ctrl-D)\n"
    ;
    private final static String PL_HELP_TEXT =
        "PROCEDURAL LANGUAGE Commands.  MUST have white space after '*'.\n"
        + "    * ?                           Help\n"
        + "    *                             Expand PL variables from now on.\n"
        + "                                  (this is also implied by all the following).\n"
        + "    * VARNAME = Variable value    Set variable value (note spaces around =)\n"
        + "    * VARNAME =                   Unset variable (note space before =)\n"
        + "    * VARNAME ~                   Set variable value to the value of the very\n"
        + "                                  next SQL statement executed (see details\n"
        + "                                  at the bottom of this listing).\n"
        + "    * list [VARNAME1...]          List values of variable(s) (defaults to all)\n"
        + "    * foreach VARNAME ([val1...]) Repeat the following PL block with the\n"
        + "                                  variable set to each value in turn.\n"
        + "    * if (logical expr)           Execute following PL block only if expr true\n"
        + "    * while (logical expr)        Repeat following PL block while expr true\n"
        + "    * end foreach|if|while        Ends a PL block\n"
        + "    * break [foreach|if|while|file] Exits a PL block or file early\n"
        + "    * continue [foreach|while]    Exits a PL block iteration early\n\n"
        + "Use PL variables (which you have set) like: *{VARNAME}.\n"
        + "You may omit the {}'s iff *VARNAME is the first word of a SQL command.\n"
        + "Use PL variables in logical expressions like: *VARNAME.\n\n"
        + "* VARNAME ~ sets the variable value according to the very next SQL statement:\n"
        + "    Query:  The value of the first field of the first row returned\n"
        + "    other:  Return status of the command (for updates this will be\n"
        + "            the number of rows updated).\n"
    ;

    /**
     * Interpret lines of input file as SQL Statements, Comments,
     * Special Commands, and Buffer Commands.
     * Most Special Commands and many Buffer commands are only for
     * interactive use.
     *
     * @param inFile  inFile of null means to read stdin.
     * @param inInteractive  If true, prompts are printed, the interactive
     *                       Special commands are enabled, and
     *                       continueOnError defaults to true.
     */
    public SqlFile(File inFile, boolean inInteractive,
                   HashMap inVars) throws IOException {

        file        = inFile;
        interactive = inInteractive;
        userVars    = inVars;
        if (interactive) {
            String tmpStr = System.getProperty("sqltool.historyLength");
            try {
                statementHistory =
                    new String[Integer.parseInt(System.getProperty("sqltool.historyLength"))];
            } catch (Throwable t) {
                statementHistory = null;
            }
            if (statementHistory == null) {
                statementHistory = new String[DEFAULT_HISTORY_SIZE];
            }
        }
        if (file != null &&!file.canRead()) {
            throw new IOException("Can't read SQL file '" + file + "'");
        }
    }

    /**
     * Constructor for reading stdin instead of a file for commands.
     *
     * @see #SqlFile(File,boolean)
     */
    public SqlFile(boolean inInteractive, HashMap inVars) throws IOException {
        this(null, inInteractive, inVars);
    }

    /**
     * Process all the commands on stdin.
     *
     * @param conn The JDBC connection to use for SQL Commands.
     * @see #execute(Connection,PrintStream,PrintStream,boolean)
     */
    public void execute(Connection conn,
                        Boolean coeOverride)
                        throws IOException, SqlToolError, SQLException {
        execute(conn, System.out, System.err, coeOverride);
    }

    /**
     * Process all the commands on stdin.
     *
     * @param conn The JDBC connection to use for SQL Commands.
     * @see #execute(Connection,PrintStream,PrintStream,boolean)
     */
    public void execute(Connection conn,
                        boolean coeOverride)
                        throws IOException, SqlToolError, SQLException {
        execute(conn, System.out, System.err, new Boolean(coeOverride));
    }

    // So we can tell how to handle quit and break commands.
    public boolean      recursed     = false;
    private String      curCommand   = null;
    private int         curLinenum   = -1;
    private int         curHist      = -1;
    private PrintStream psStd        = null;
    private PrintStream psErr        = null;
    private PrintWriter pwQuery      = null;
    StringBuffer        stringBuffer = new StringBuffer();
    /*
     * This is reset upon each execute() invocation (to true if interactive,
     * false otherwise).
     */
    private boolean             continueOnError = false;
    private static final String DEFAULT_CHARSET = "US-ASCII";
    private BufferedReader      br              = null;
    private String              charset         = null;

    /**
     * Process all the commands in the file (or stdin) associated with
     * "this" object.
     * Run SQL in the file through the given database connection.
     *
     * This is synchronized so that I can use object variables to keep
     * track of current line number, command, connection, i/o streams, etc.
     *
     * Sets encoding character set to that specified with System Property
     * 'sqlfile.charset'.  Defaults to "US-ASCII".
     *
     * @param conn The JDBC connection to use for SQL Commands.
     */
    public synchronized void execute(Connection conn, PrintStream stdIn,
                                     PrintStream errIn,
                                     Boolean coeOverride)
                                     throws IOException, SqlToolError,
                                         SQLException {

        psStd      = stdIn;
        psErr      = errIn;
        curConn    = conn;
        curLinenum = -1;
        String  inputLine;
        String  trimmedCommand;
        String  trimmedInput;
        String  deTerminated;
        boolean inComment = false;    // Globbling up a comment
        int     postCommentIndex;
        boolean gracefulExit = false;
        continueOnError = (coeOverride == null) ? interactive
                                                : coeOverride.booleanValue();
        if (userVars != null && userVars.size() > 0) {
            plMode = true;
        }
        String specifiedCharSet = System.getProperty("sqlfile.charset");
        charset = ((specifiedCharSet == null) ? DEFAULT_CHARSET
                                              : specifiedCharSet);
        try {
            br = new BufferedReader(new InputStreamReader((file == null)
                    ? System.in
                    : new FileInputStream(file), charset));
            curLinenum = 0;
            if (interactive) {
                stdprintln(BANNER);
            }
            while (true) {
                if (interactive) {
                    psStd.print((stringBuffer.length() == 0) ? primaryPrompt
                                                             : contPrompt);
                }
                inputLine = br.readLine();
                if (inputLine == null) {
                    /*
                     * This is because interactive EOD on some OSes doesn't
                     * send a line-break, resulting in no linebreak at all
                     * after the SqlFile prompt or whatever happens to be
                     * on their screen.
                     */
                    if (interactive) {
                        psStd.println();
                    }
                    break;
                }
                curLinenum++;
                if (inComment) {
                    postCommentIndex = inputLine.indexOf("*/") + 2;
                    if (postCommentIndex > 1) {

                        // I see no reason to leave comments in history.
                        inputLine = inputLine.substring(postCommentIndex);

                        // Empty the buffer.  The non-comment remainder of
                        // this line is either the beginning of a new SQL
                        // or Special command, or an empty line.
                        stringBuffer.setLength(0);
                        inComment = false;
                    } else {

                        // Just completely ignore the input line.
                        continue;
                    }
                }
                trimmedInput = inputLine.trim();
                try {
                    // This is the try for SQLException.  SQLExceptions are
                    // normally thrown below in Statement processing, but
                    // could be called up above if a Special processing
                    // executes a SQL command from history.
                    if (stringBuffer.length() == 0) {
                        if (trimmedInput.startsWith("/*")) {
                            postCommentIndex = trimmedInput.indexOf("*/", 2)
                                               + 2;
                            if (postCommentIndex > 1) {

                                // I see no reason to leave comments in
                                // history.
                                inputLine = inputLine.substring(
                                    postCommentIndex + inputLine.length()
                                    - trimmedInput.length());
                                trimmedInput = inputLine.trim();
                            } else {

                                // Just so we get continuation lines:
                                stringBuffer.append("COMMENT");
                                inComment = true;
                                continue;
                            }
                        }

                        // This is just to filter out useless newlines at
                        // beginning of commands.
                        if (trimmedInput.length() == 0) {
                            continue;
                        }
                        if (trimmedInput.startsWith("* ")
                                || trimmedInput.equals("*")) {
                            try {
                                processPL((trimmedInput.length() == 1) ? ""
                                                                       : trimmedInput
                                                                       .substring(
                                                                           2));
                            } catch (BadSpecial bs) {
                                errprintln("Error at '"
                                           + ((file == null) ? "stdin"
                                                             : file.toString()) + "' line "
                                                             + curLinenum
                                                             + ":\n\""
                                                             + inputLine
                                                             + "\"\n"
                                                             + bs.getMessage());
                                if (!continueOnError) {
                                    throw new SqlToolError(bs);
                                }
                            }
                            continue;
                        }
                        if (trimmedInput.charAt(0) == '\\') {
                            try {
                                processSpecial(trimmedInput.substring(1));
                            } catch (BadSpecial bs) {
                                errprintln("Error at '"
                                           + ((file == null) ? "stdin"
                                                             : file.toString()) + "' line "
                                                             + curLinenum
                                                             + ":\n\""
                                                             + inputLine
                                                             + "\"\n"
                                                             + bs.getMessage());
                                if (!continueOnError) {
                                    throw new SqlToolError(bs);
                                }
                            }
                            continue;
                        }
                        if (interactive && trimmedInput.charAt(0) == ':') {
                            try {
                                processBuffer(trimmedInput.substring(1));
                            } catch (BadSpecial bs) {
                                errprintln("Error at '"
                                           + ((file == null) ? "stdin"
                                                             : file.toString()) + "' line "
                                                             + curLinenum
                                                             + ":\n\""
                                                             + inputLine
                                                             + "\"\n"
                                                             + bs.getMessage());
                                if (!continueOnError) {
                                    throw new SqlToolError(bs);
                                }
                            }
                            continue;
                        }
                    }
                    if (trimmedInput.length() == 0) {
                        if (interactive &&!inComment) {
                            setBuf(stringBuffer.toString());
                            stringBuffer.setLength(0);
                            stdprintln("Current input moved into buffer.");
                        }
                        continue;
                    }
                    deTerminated = deTerminated(inputLine);

                    // A null terminal line (i.e., /\s*;\s*$/) is never useful.
                    if (!trimmedInput.equals(";")) {
                        if (stringBuffer.length() > 0) {
                            stringBuffer.append('\n');
                        }
                        stringBuffer.append((deTerminated == null) ? inputLine
                                                                   : deTerminated);
                    }
                    if (deTerminated == null) {
                        continue;
                    }

                    // If we reach here, then stringBuffer contains a complete
                    // SQL command.
                    curCommand     = stringBuffer.toString();
                    trimmedCommand = curCommand.trim();
                    if (trimmedCommand.length() == 0) {
                        throw new SQLException("Empty SQL Statement");
                    }
                    if (interactive) {
                        setBuf(curCommand);
                    }
                    processSQL();
                } catch (SQLException se) {
                    errprintln("SQL Error at '" + ((file == null) ? "stdin"
                                                                  : file.toString()) + "' line "
                                                                  + curLinenum
                                                                      + ":\n\""
                                                                          + curCommand
                                                                              + "\"\n"
                                                                                  + se
                                                                                  .getMessage());
                    if (!continueOnError) {
                        throw se;
                    }
                } catch (BreakException be) {
                    String msg = be.getMessage();
                    if ((!recursed) && (msg != null &&!msg.equals("file"))) {
                        errprintln("Unsatisfied break statement"
                                   + ((msg == null) ? ""
                                                    : (" (type " + msg
                                                       + ')')) + '.');
                    } else {
                        gracefulExit = true;
                    }
                    if (recursed ||!continueOnError) {
                        throw be;
                    }
                } catch (ContinueException ce) {
                    String msg = ce.getMessage();
                    if (!recursed) {
                        errprintln("Unsatisfied continue statement"
                                   + ((msg == null) ? ""
                                                    : (" (type " + msg
                                                       + ')')) + '.');
                    } else {
                        gracefulExit = true;
                    }
                    if (recursed ||!continueOnError) {
                        throw ce;
                    }
                } catch (QuitNow qn) {
                    throw qn;
                } catch (SqlToolError ste) {
                    if (!continueOnError) {
                        throw ste;
                    }
                }
                stringBuffer.setLength(0);
            }
            if (inComment || stringBuffer.length() != 0) {
                errprintln("Unterminated input:  [" + stringBuffer + ']');
                throw new SqlToolError("Unterminated input:  ["
                                       + stringBuffer + ']');
            }
            gracefulExit = true;
        } catch (QuitNow qn) {
            gracefulExit = qn.getMessage() == null;
            if ((!recursed) &&!gracefulExit) {
                errprintln("Aborting: " + qn.getMessage());
            }
            if (recursed ||!gracefulExit) {
                throw qn;
            }
            return;
        } finally {
            closeQueryOutputStream();
            if (fetchingVar != null) {
                errprintln("PL variable setting incomplete:  " + fetchingVar);
                gracefulExit = false;
            }
            if (br != null) {
                br.close();
            }
            if ((!gracefulExit) && possiblyUncommitteds.get()) {
                errprintln("Rolling back SQL transaction.");
                curConn.rollback();
                possiblyUncommitteds.set(false);
            }
        }
    }

    /**
     * Returns a copy of given string without a terminating semicolon.
     * If there is no terminating semicolon, null is returned.
     *
     * @param inString Base String, which will not be modified (because
     *                 a "copy" will be returned).
     */
    private static String deTerminated(String inString) {

        int index = inString.lastIndexOf(';');
        if (index < 0) {
            return null;
        }
        for (int i = index + 1; i < inString.length(); i++) {
            if (!Character.isWhitespace(inString.charAt(i))) {
                return null;
            }
        }
        return inString.substring(0, index);
    }

    /**
     * Utility nested Exception class for internal use.
     */
    private class BadSpecial extends Exception {

        private BadSpecial(String s) {
            super(s);
        }
    }

    /**
     * Utility nested Exception class for internal use.
     * This must extend SqlToolError because it has to percolate up from
     * recursions of SqlTool.execute(), yet SqlTool.execute() is public
     * and external users should not declare (or expect!) QuitNows to be
     * thrown.
     * SqlTool.execute() on throws a QuitNow if it is in a recursive call.
     */
    private class QuitNow extends SqlToolError {

        public QuitNow(String s) {
            super(s);
        }

        public QuitNow() {
            super();
        }
    }

    /**
     * Utility nested Exception class for internal use.
     * Very similar to QuitNow.
     */
    private class BreakException extends SqlToolError {

        public BreakException() {
            super();
        }

        public BreakException(String s) {
            super(s);
        }
    }

    /**
     * Utility nested Exception class for internal use.
     * Very similar to QuitNow.
     */
    private class ContinueException extends SqlToolError {

        public ContinueException() {
            super();
        }

        public ContinueException(String s) {
            super(s);
        }
    }

    /**
     * Utility nested Exception class for internal use.
     */
    private class BadSwitch extends Exception {

        private BadSwitch(int i) {
            super(Integer.toString(i));
        }
    }

    /**
     * Process a Buffer/Edit Command.
     *
     * Due to the nature of the goal here, we don't trim() "other" like
     * we do for other kinds of commands.
     *
     * @param inString Complete command, less the leading ':' character.
     * @throws SQLException Passed through from processSQL()
     * @throws BadSpecial Runtime error()
     */
    private void processBuffer(String inString)
    throws BadSpecial, SQLException {

        int    index = 0;
        int    special;
        char   commandChar = 'i';
        String other       = null;
        if (inString.length() > 0) {
            commandChar = inString.charAt(0);
            other       = inString.substring(1);
            if (other.trim().length() == 0) {
                other = null;
            }
        }
        switch (commandChar) {

            case ';' :
                curCommand = commandFromHistory(0);
                stdprintln("Executing command from buffer:\n" + curCommand
                           + '\n');
                processSQL();
                return;

            case 'a' :
            case 'A' :
                stringBuffer.append(commandFromHistory(0));
                if (other != null) {
                    String deTerminated = deTerminated(other);
                    if (!other.equals(";")) {
                        stringBuffer.append(((deTerminated == null) ? other
                                                                    : deTerminated));
                    }
                    if (deTerminated != null) {

                        // If we reach here, then stringBuffer contains a
                        // complete SQL command.
                        curCommand = stringBuffer.toString();
                        setBuf(curCommand);
                        stdprintln("Executing:\n" + curCommand + '\n');
                        processSQL();
                        stringBuffer.setLength(0);
                        return;
                    }
                }
                stdprintln("Appending to:\n" + stringBuffer);
                return;

            case 'l' :
            case 'L' :
                stdprintln("Current Buffer:\n" + commandFromHistory(0));
                return;

            case 's' :
            case 'S' :

                // For now, I'm only keeping the "modified" SQL command in
                // history.  This is because a user could make 10 modifications
                // to a command before it is usable, and we don't want those
                // intermediate commands cluttering up the history.
                // Note that this behavior is inconsistent with that of :a.
                // Should probably refactor this.
                boolean modeIC      = false;
                boolean modeGlobal  = false;
                boolean modeExecute = false;
                int     modeLine    = 0;
                try {
                    String       fromHist = commandFromHistory(0);
                    StringBuffer sb       = new StringBuffer(fromHist);
                    if (other == null) {
                        throw new BadSwitch(0);
                    }
                    String delim = other.substring(0, 1);
                    StringTokenizer toker = new StringTokenizer(other, delim,
                        true);
                    if (toker.countTokens() < 4
                            ||!toker.nextToken().equals(delim)) {
                        throw new BadSwitch(1);
                    }
                    String from = toker.nextToken().replace('$', '\n');
                    if (!toker.nextToken().equals(delim)) {
                        throw new BadSwitch(2);
                    }
                    String to = toker.nextToken().replace('$', '\n');
                    if (to.equals(delim)) {
                        to = "";
                    } else {
                        if (toker.countTokens() > 0
                                &&!toker.nextToken().equals(delim)) {
                            throw new BadSwitch(3);
                        }
                    }
                    if (toker.countTokens() > 0) {
                        String opts = toker.nextToken("");
                        for (int j = 0; j < opts.length(); j++) {
                            switch (opts.charAt(j)) {

                                case 'i' :
                                    modeIC = true;
                                    break;

                                case ';' :
                                    modeExecute = true;
                                    break;

                                case 'g' :
                                    modeGlobal = true;
                                    break;

                                case '1' :
                                case '2' :
                                case '3' :
                                case '4' :
                                case '5' :
                                case '6' :
                                case '7' :
                                case '8' :
                                case '9' :
                                    modeLine = Character.digit(opts.charAt(j),
                                                               10);
                                    break;

                                default :
                                    throw new BadSpecial(
                                        "Unknown Substitution option: "
                                        + opts.charAt(j));
                            }
                        }
                    }
                    if (modeIC) {
                        fromHist = fromHist.toUpperCase();
                        from     = from.toUpperCase();
                    }

                    // lineStart will be either 0 or char FOLLOWING a \n.
                    int lineStart = 0;

                    // lineStop is the \n AFTER what we consider.
                    int lineStop = -1;
                    if (modeLine > 0) {
                        for (int j = 1; j < modeLine; j++) {
                            lineStart = fromHist.indexOf('\n', lineStart) + 1;
                            if (lineStart < 1) {
                                throw new BadSpecial(
                                    "There are not " + modeLine
                                    + " lines in the buffer.");
                            }
                        }
                        lineStop = fromHist.indexOf('\n', lineStart);
                    }
                    if (lineStop < 0) {
                        lineStop = fromHist.length();
                    }

                    // System.err.println("["
                    // + fromHist.substring(lineStart, lineStop) + ']');
                    int i;
                    if (modeGlobal) {
                        i = lineStop;
                        while ((i = fromHist.lastIndexOf(from, i - 1))
                                >= lineStart) {
                            sb.replace(i, i + from.length(), to);
                        }
                    } else if ((i = fromHist.indexOf(from, lineStart)) > -1
                               && i < lineStop) {
                        sb.replace(i, i + from.length(), to);
                    }
                    statementHistory[curHist] = sb.toString();
                    stdprintln((modeExecute ? "Executing"
                                            : "Current Buffer") + ":\n"
                                            + commandFromHistory(0));
                    if (modeExecute) {
                        stdprintln();
                    }
                } catch (BadSwitch badswitch) {
                    throw new BadSpecial(
                        "Substitution syntax:  \":s/from this/to that/i;g2\".  "
                        + "Use '$' for line separations.  ["
                        + badswitch.getMessage() + ']');
                }
                if (modeExecute) {
                    curCommand = commandFromHistory(0);
                    processSQL();
                }
                return;

            case '?' :
                stdprintln(BUFFER_HELP_TEXT);
                return;
        }
        throw new BadSpecial("Unknown Buffer Command");
    }

    /**
     * Process a Special Command.
     *
     * @param inString Complete command, less the leading '\' character.
     * @throws SQLException Passed through from processSQL()
     * @throws BadSpecial Runtime error()
     * @throws QuitNot Command execution (but not the JVM!) should stop
     */
    private void processSpecial(String inString)
    throws BadSpecial, QuitNow, SQLException, SqlToolError {

        int    index = 0;
        int    special;
        String arg1,
               other = null;
        if (inString.length() < 1) {
            throw new BadSpecial("Null special command");
        }
        if (plMode) {
            inString = dereference(inString, false);
        }
        StringTokenizer toker = new StringTokenizer(inString);
        arg1 = toker.nextToken();
        if (toker.hasMoreTokens()) {
            other = toker.nextToken("").trim();
        }
        switch (arg1.charAt(0)) {

            case 'q' :
                if (other != null) {
                    throw new QuitNow(other);
                }
                throw new QuitNow();
            case 'H' :
                htmlMode = !htmlMode;
                stdprintln("HTML Mode is now set to: " + htmlMode);
                return;

            case 'd' :
                if (arg1.length() == 2) {
                    listTables(arg1.charAt(1), other);
                    return;
                }
                if (arg1.length() == 1 && other != null) {
                    describe(other);
                    return;
                }
                throw new BadSpecial("Describe commands must be like "
                                     + "'\\dX' or like '\\d OBJECTNAME'.");
            case 'o' :
                if (other == null) {
                    if (pwQuery == null) {
                        throw new BadSpecial(
                            "There is no query output file to close");
                    }
                    closeQueryOutputStream();
                    return;
                }
                if (pwQuery != null) {
                    stdprintln(
                        "Closing current query output file and opening "
                        + "new one");
                    closeQueryOutputStream();
                }
                try {
                    pwQuery = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(other, true), charset));
                    /* Opening in append mode, so it's possible that we will
                     * be adding superfluous <HTML> and <BODY> tages.
                     * I think that browsers can handle that */
                    pwQuery.println((htmlMode ? "<HTML>\n<!--"
                                              : "#") + " "
                                                     + (new java.util.Date())
                                                     + ".  Query output from "
                                                     + getClass().getName()
                                                     + (htmlMode
                                                        ? ". -->\n\n<BODY>"
                                                        : ".\n"));
                    pwQuery.flush();
                } catch (Exception e) {
                    throw new BadSpecial("Failed to write to file '" + other
                                         + "':  " + e);
                }
                return;

            case 'w' :
                if (other == null) {
                    throw new BadSpecial(
                        "You must supply a destination file name");
                }
                if (commandFromHistory(0).length() == 0) {
                    throw new BadSpecial("Empty command in buffer");
                }
                try {
                    PrintWriter pw = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(other, true), charset));
                    pw.println(commandFromHistory(0) + ';');
                    pw.flush();
                    pw.close();
                } catch (Exception e) {
                    throw new BadSpecial("Failed to append to file '" + other
                                         + "':  " + e);
                }
                return;

            case 'i' :
                if (other == null) {
                    throw new BadSpecial("You must supply an SQL file name");
                }
                try {
                    SqlFile sf = new SqlFile(new File(other), false,
                                             userVars);
                    sf.recursed = true;

                    // Share the possiblyUncommitted state
                    sf.possiblyUncommitteds = possiblyUncommitteds;
                    sf.plMode               = plMode;
                    sf.execute(curConn, continueOnError);
                } catch (ContinueException ce) {
                    throw ce;
                } catch (BreakException be) {
                    String beMessage = be.getMessage();
                    if (beMessage != null &&!beMessage.equals("file")) {
                        throw be;
                    }
                } catch (QuitNow qe) {
                    throw qe;
                } catch (Exception e) {
                    throw new BadSpecial("Failed to execute SQL from file '"
                                         + other + "':  " + e.getMessage());
                }
                return;

            case 'p' :
                if (other == null) {
                    stdprintln(true);
                } else {
                    stdprintln(other, true);
                }
                return;

            case 'a' :
                if (other != null) {
                    curConn.setAutoCommit(
                        Boolean.valueOf(other).booleanValue());
                }
                stdprintln("Auto-commit is set to: "
                           + curConn.getAutoCommit());
                return;

            case '*' :
                if (other != null) {

                    // But remember that we have to abort on some I/O errors.
                    continueOnError = Boolean.valueOf(other).booleanValue();
                }
                stdprintln("Continue-on-error is set to: " + continueOnError);
                return;

            case 's' :
                showHistory();
                return;

            case '-' :
                int     commandsAgo = 0;
                String  numStr;
                boolean executeMode = arg1.charAt(arg1.length() - 1) == ';';
                if (executeMode) {

                    // Trim off terminating ';'
                    arg1 = arg1.substring(0, arg1.length() - 1);
                }
                numStr = (arg1.length() == 1) ? null
                                              : arg1.substring(1,
                                              arg1.length());
                if (numStr == null) {
                    commandsAgo = 0;
                } else {
                    try {
                        commandsAgo = Integer.parseInt(numStr);
                    } catch (NumberFormatException nfe) {
                        throw new BadSpecial("Malformatted command number");
                    }
                }
                setBuf(commandFromHistory(commandsAgo));
                if (executeMode) {
                    processBuffer(";");
                } else {
                    stdprintln(
                        "RESTORED following command to buffer.  Enter \":?\" "
                        + "to see buffer commands:\n"
                        + commandFromHistory(0));
                }
                return;

            case '?' :
                stdprintln(HELP_TEXT);
                return;

            case '!' :
                InputStream stream;
                byte[]      ba         = new byte[1024];
                String      extCommand = ((arg1.length() == 1) ? ""
                                                               : arg1.substring(1)) + ((arg1.length() > 1 && other != null)
                                                                   ? " "
                                                                   : "") + ((other == null)
                                                                       ? ""
                                                                       : other);
                try {
                    Process proc = Runtime.getRuntime().exec(extCommand);
                    proc.getOutputStream().close();
                    int i;
                    stream = proc.getInputStream();
                    while ((i = stream.read(ba)) > 0) {
                        stdprint(new String(ba, 0, i));
                    }
                    stream.close();
                    stream = proc.getErrorStream();
                    while ((i = stream.read(ba)) > 0) {
                        errprint(new String(ba, 0, i));
                    }
                    stream.close();
                    if (proc.waitFor() != 0) {
                        throw new BadSpecial("External command failed: '"
                                             + extCommand + "'");
                    }
                } catch (Exception e) {
                    throw new BadSpecial("Failed to execute command '"
                                         + extCommand + "':  " + e);
                }
                return;
        }
        throw new BadSpecial("Unknown Special Command");
    }

    /**
     * Deference PL variables.
     *
     * @throws SQLException  This is really an inappropriate exception
     * type.  Only using it because I don't have time to do things properly.
     */
    private String dereference(String inString,
                               boolean permitAlias) throws SQLException {

        String       varName, varValue;
        StringBuffer expandBuffer = new StringBuffer(inString);
        int          b, e;    // begin and end
        if (permitAlias && inString.charAt(0) == '*') {
            Iterator it = userVars.keySet().iterator();
            while (it.hasNext()) {
                varName = (String) it.next();
                if (inString.equals("*" + varName)) {
                    return (String) userVars.get(varName);
                }
                if (inString.startsWith("*" + varName + ' ')
                        || inString.startsWith("*" + varName + '\t')) {
                    expandBuffer.replace(0, varName.length() + 1,
                                         (String) userVars.get(varName));
                    return expandBuffer.toString();
                }
            }
            return inString;
        }
        String s;
        while (true) {
            s = expandBuffer.toString();
            if ((b = s.indexOf("*{")) < 0
                    || ((e = s.indexOf('}', b + 2)) < 0)) {
                break;
            }
            varName = s.substring(b + 2, e);
            if (!userVars.containsKey(varName)) {
                throw new SQLException("Use of unset PL variable: "
                                       + varName);
            }
            expandBuffer.replace(b, e + 1, (String) userVars.get(varName));
        }
        return expandBuffer.toString();
    }

    public boolean plMode = false;

    //  PL variable name currently awaiting query output.
    private String fetchingVar = null;

    /**
     * Process a Process Language Command.
     * Nesting not supported yet.
     *
     * @param inString Complete command, less the leading '\' character.
     * @throws BadSpecial Runtime error()
     */
    private void processPL(String inString)
    throws BadSpecial, SqlToolError, SQLException {

        if (inString.length() < 1) {
            plMode = true;
            stdprintln("PL variable expansion mode is now on");
            return;
        }
        if (inString.charAt(0) == '?') {
            stdprintln(PL_HELP_TEXT);
            return;
        }
        if (plMode) {
            inString = dereference(inString, false);
        }
        StringTokenizer toker      = new StringTokenizer(inString);
        String          arg1       = toker.nextToken();
        String[]        tokenArray = null;

        // If user runs any PL command, we turn PL mode on.
        plMode = true;
        if (userVars == null) {
            userVars = new HashMap();
        }
        if (arg1.equals("end")) {
            throw new BadSpecial("PL end statements may only occur inside of "
                                 + "a PL block");
        }
        if (arg1.equals("continue")) {
            if (toker.hasMoreTokens()) {
                String s = toker.nextToken("").trim();
                if (s.equals("foreach") || s.equals("while")) {
                    throw new ContinueException(s);
                } else {
                    throw new BadSpecial(
                        "Bad continue statement."
                        + "You may use no argument or one of 'foreach', "
                        + "'while'");
                }
            }
            throw new ContinueException();
        }
        if (arg1.equals("break")) {
            if (toker.hasMoreTokens()) {
                String s = toker.nextToken("").trim();
                if (s.equals("foreach") || s.equals("if")
                        || s.equals("while") || s.equals("file")) {
                    throw new BreakException(s);
                } else {
                    throw new BadSpecial(
                        "Bad break statement."
                        + "You may use no argument or one of 'foreach', "
                        + "'if', 'while', 'file'");
                }
            }
            throw new BreakException();
        }
        if (arg1.equals("list")) {
            if (toker.countTokens() == 0) {
                stdprint(formatNicely(userVars));
            } else {
                tokenArray = getTokenArray(toker.nextToken(""));
                for (int i = 0; i < tokenArray.length; i++) {
                    stdprintln("    " + tokenArray[i] + ": ("
                               + userVars.get(tokenArray[i]) + ')');
                }
            }
            return;
        }
        if (arg1.equals("foreach")) {
            if (toker.countTokens() < 2) {
                throw new BadSpecial("Malformatted PL foreach command (1)");
            }
            String varName   = toker.nextToken();
            String parenExpr = toker.nextToken("").trim();
            if (parenExpr.length() < 2 || parenExpr.charAt(0) != '('
                    || parenExpr.charAt(parenExpr.length() - 1) != ')') {
                throw new BadSpecial("Malformatted PL foreach command (2)");
            }
            String[] values = getTokenArray(parenExpr.substring(1,
                parenExpr.length() - 1));
            File   tmpFile = null;
            String varVal;
            try {
                tmpFile = plBlockFile("foreach");
            } catch (IOException ioe) {
                throw new BadSpecial(
                    "Failed to write given PL block temp file: " + ioe);
            }
            String origval = (String) userVars.get(varName);
            try {
                SqlFile sf;
                for (int i = 0; i < values.length; i++) {
                    try {
                        varVal = values[i];
                        userVars.put(varName, varVal);
                        sf          = new SqlFile(tmpFile, false, userVars);
                        sf.plMode   = true;
                        sf.recursed = true;

                        // Share the possiblyUncommitted state
                        sf.possiblyUncommitteds = possiblyUncommitteds;
                        sf.execute(curConn, continueOnError);
                    } catch (ContinueException ce) {
                        String ceMessage = ce.getMessage();
                        if (ceMessage != null
                                &&!ceMessage.equals("foreach")) {
                            throw ce;
                        }
                    }
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();
                if (beMessage != null &&!beMessage.equals("foreach")) {
                    throw be;
                }
            } catch (QuitNow qe) {
                throw qe;
            } catch (Exception e) {
                throw new BadSpecial("Failed to execute SQL from PL block.  "
                                     + e.getMessage());
            }
            if (origval == null) {
                userVars.remove(varName);
            } else {
                userVars.put(varName, origval);
            }
            if (tmpFile != null &&!tmpFile.delete()) {
                throw new BadSpecial(
                    "Error occurred while trying to remove temp file '"
                    + tmpFile + "'");
            }
            return;
        }
        if (arg1.equals("if")) {
            if (toker.countTokens() < 1) {
                throw new BadSpecial("Malformatted PL if command (1)");
            }
            String parenExpr = toker.nextToken("").trim();
            if (parenExpr.length() < 2 || parenExpr.charAt(0) != '('
                    || parenExpr.charAt(parenExpr.length() - 1) != ')') {
                throw new BadSpecial("Malformatted PL if command (2)");
            }
            String[] values = getTokenArray(parenExpr.substring(1,
                parenExpr.length() - 1));
            File tmpFile = null;
            try {
                tmpFile = plBlockFile("if");
            } catch (IOException ioe) {
                throw new BadSpecial(
                    "Failed to write given PL block temp file: " + ioe);
            }
            try {
                if (eval(values)) {
                    SqlFile sf = new SqlFile(tmpFile, false, userVars);
                    sf.plMode   = true;
                    sf.recursed = true;

                    // Share the possiblyUncommitted state
                    sf.possiblyUncommitteds = possiblyUncommitteds;
                    sf.execute(curConn, continueOnError);
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();
                if (beMessage == null ||!beMessage.equals("if")) {
                    throw be;
                }
            } catch (ContinueException ce) {
                throw ce;
            } catch (QuitNow qe) {
                throw qe;
            } catch (BadSpecial bs) {
                throw new BadSpecial("Malformatted PL if command (3): " + bs);
            } catch (Exception e) {
                throw new BadSpecial("Failed to execute SQL from PL block.  "
                                     + e.getMessage());
            }
            if (tmpFile != null &&!tmpFile.delete()) {
                throw new BadSpecial(
                    "Error occurred while trying to remove temp file '"
                    + tmpFile + "'");
            }
            return;
        }
        if (arg1.equals("while")) {
            if (toker.countTokens() < 1) {
                throw new BadSpecial("Malformatted PL while command (1)");
            }
            String parenExpr = toker.nextToken("").trim();
            if (parenExpr.length() < 2 || parenExpr.charAt(0) != '('
                    || parenExpr.charAt(parenExpr.length() - 1) != ')') {
                throw new BadSpecial("Malformatted PL while command (2)");
            }
            String[] values = getTokenArray(parenExpr.substring(1,
                parenExpr.length() - 1));
            File tmpFile = null;
            try {
                tmpFile = plBlockFile("while");
            } catch (IOException ioe) {
                throw new BadSpecial(
                    "Failed to write given PL block temp file: " + ioe);
            }
            try {
                SqlFile sf;
                while (eval(values)) {
                    try {
                        sf          = new SqlFile(tmpFile, false, userVars);
                        sf.recursed = true;

                        // Share the possiblyUncommitted state
                        sf.possiblyUncommitteds = possiblyUncommitteds;
                        sf.plMode               = true;
                        sf.execute(curConn, continueOnError);
                    } catch (ContinueException ce) {
                        String ceMessage = ce.getMessage();
                        if (ceMessage != null &&!ceMessage.equals("while")) {
                            throw ce;
                        }
                    }
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();
                if (beMessage != null &&!beMessage.equals("while")) {
                    throw be;
                }
            } catch (QuitNow qe) {
                throw qe;
            } catch (BadSpecial bs) {
                throw new BadSpecial("Malformatted PL while command (3): "
                                     + bs);
            } catch (Exception e) {
                throw new BadSpecial("Failed to execute SQL from PL block.  "
                                     + e.getMessage());
            }
            if (tmpFile != null &&!tmpFile.delete()) {
                throw new BadSpecial(
                    "Error occurred while trying to remove temp file '"
                    + tmpFile + "'");
            }
            return;
        }
        if (toker.countTokens() < 1) {
            throw new BadSpecial("Unknown PL command (1)");
        }
        String operator = toker.nextToken();
        if (operator.length() != 1) {
            throw new BadSpecial("Unknown PL command (2)");
        }
        switch (operator.charAt(0)) {

            case '~' :
                if (toker.countTokens() > 0) {
                    throw new BadSpecial(
                        "PL ~ set command takes no other args");
                }
                userVars.remove(arg1);
                fetchingVar = arg1;
                return;

            case '=' :
                if (toker.countTokens() == 0) {
                    userVars.remove(arg1);
                } else {
                    userVars.put(arg1, toker.nextToken("").trim());
                }
                return;
        }
        throw new BadSpecial("Unknown PL command (3)");
    }

    /*
     * Read a PL block into a new temp file.
     *
     * WARNING!!! foreach blocks are not yet smart about comments
     * and strings.  We just look for a line beginning with "* end"
     * without worrying about comments or quotes (for now).
     *
     * WARNING!!! This is very rudimentary.
     * Users give up all editing and feedback capabilities for while
     * in the foreach loop.
     * A better solution would be to pass current input stream to a
     * new SqlFile.execute() with a mode whereby commands are written
     * to a separate history but not executed.
     */
    private File plBlockFile(String type) throws IOException, SqlToolError {

        String          s;
        StringTokenizer toker;

        // Have already read the if/while/foreach statement, so we are already
        // at nest level 1.  When we reach nestlevel 1 (read 1 net "end"
        // statement), we're at level 0 and return.
        int    nestlevel = 1;
        String curPlCommand;
        if (type == null
                || ((!type.equals("foreach")) && (!type.equals("if"))
                    && (!type.equals("while")))) {
            throw new RuntimeException(
                "Assertion failed.  Unsupported PL block type:  " + type);
        }
        File tmpFile = File.createTempFile("sqltool-", ".sql");
        PrintWriter pw = new PrintWriter(
            new OutputStreamWriter(new FileOutputStream(tmpFile), charset));
        pw.println("/* " + (new java.util.Date()) + ". "
                   + getClass().getName() + " PL block. */\n");
        while (true) {
            s = br.readLine();
            if (s == null) {
                errprintln("Unterminated '" + type + "' PL block");
                throw new SqlToolError("Unterminated '" + type
                                       + "' PL block");
            }
            curLinenum++;
            toker = new StringTokenizer(s);
            if (toker.countTokens() > 1 && toker.nextToken().equals("*")) {
                curPlCommand = toker.nextToken();

                // PL COMMAND of some sort.
                if (curPlCommand.equals(type)) {
                    nestlevel++;
                } else if (curPlCommand.equals("end")) {
                    if (toker.countTokens() < 1) {
                        errprintln("PL end statement requires arg of "
                                   + "'foreach' or 'if' or 'while' (1)");
                        throw new SqlToolError(
                            "PL end statement requires arg "
                            + " of 'foreach' or 'if' or 'while' (1)");
                    }
                    String inType = toker.nextToken();
                    if (inType.equals(type)) {
                        nestlevel--;
                        if (nestlevel < 1) {
                            break;
                        }
                    }
                    if ((!inType.equals("foreach")) && (!inType.equals("if"))
                            && (!inType.equals("while"))) {
                        errprintln("PL end statement requires arg of "
                                   + "'foreach' or 'if' or 'while' (2)");
                        throw new SqlToolError(
                            "PL end statement requires arg of "
                            + "'foreach' or 'if' or 'while' (2)");
                    }
                }
            }
            pw.println(s);
        }
        pw.flush();
        pw.close();
        return tmpFile;
    }

    /**
     * Wrapper methods so don't need to call x(..., false) in most cases.
     */
    private void stdprintln() {
        stdprintln(false);
    }

    private void stdprint(String s) {
        stdprint(s, false);
    }

    private void stdprintln(String s) {
        stdprintln(s, false);
    }

    /**
     * Encapsulates normal output.
     *
     * Conditionally HTML-ifies output.
     */
    private void stdprintln(boolean queryOutput) {

        if (htmlMode) {
            psStd.println("<BR>");
        } else {
            psStd.println();
        }
        if (queryOutput && pwQuery != null) {
            if (htmlMode) {
                pwQuery.println("<BR>");
            } else {
                pwQuery.println();
            }
            pwQuery.flush();
        }
    }

    /**
     * Encapsulates error output.
     *
     * Conditionally HTML-ifies error output.
     */
    private void errprint(String s) {

        psErr.print(htmlMode
                    ? ("<DIV style='color:white; background: red; "
                       + "font-weight: bold'>" + s + "</DIV>")
                    : s);
    }

    /**
     * Encapsulates error output.
     *
     * Conditionally HTML-ifies error output.
     */
    private void errprintln(String s) {

        psErr.println(htmlMode
                      ? ("<DIV style='color:white; background: red; "
                         + "font-weight: bold'>" + s + "</DIV>")
                      : s);
    }

    /**
     * Encapsulates normal output.
     *
     * Conditionally HTML-ifies output.
     */
    private void stdprint(String s, boolean queryOutput) {

        psStd.print(htmlMode ? ("<P>" + s + "</P>")
                             : s);
        if (queryOutput && pwQuery != null) {
            pwQuery.print(htmlMode ? ("<P>" + s + "</P>")
                                   : s);
            pwQuery.flush();
        }
    }

    /**
     * Encapsulates normal output.
     *
     * Conditionally HTML-ifies output.
     */
    private void stdprintln(String s, boolean queryOutput) {

        psStd.println(htmlMode ? ("<P>" + s + "</P>")
                               : s);
        if (queryOutput && pwQuery != null) {
            pwQuery.println(htmlMode ? ("<P>" + s + "</P>")
                                     : s);
            pwQuery.flush();
        }
    }

    private static final int DEFAULT_ELEMENT = 0,
                             HSQLDB_ELEMENT  = 1,
                             ORACLE_ELEMENT  = 2
    ;

    /** Column numbering starting at 1. */
    private static final int[][] listMDTableCols = {
        {
            2, 3
        },        // Default
        { 3 },    // HSQLDB
        {
            2, 3
        },        // Oracle
    };

    /**
     * Lists available database tables.
     * This method needs work.  See the implementation comments.
     */
    private void listTables(char c,
                            String filter) throws SQLException, BadSpecial {

        int[]                     listSet       = null;
        String[]                  types         = null;
        java.sql.DatabaseMetaData md            = curConn.getMetaData();
        String                    dbProductName = md.getDatabaseProductName();

        //System.err.println("DB NAME = (" + dbProductName + ')');
        // Database-specific table filtering.
        String excludePrefix = null;
        if (c != '*') {
            types = new String[1];
            switch (c) {

                case 's' :
                    types[0] = "SYSTEM TABLE";
                    break;

                case 'a' :
                    types[0] = "ALIAS";
                    break;

                case 't' :
                    types[0] = "TABLE";
                    break;

                case 'v' :
                    types[0] = "VIEW";
                    break;

                default :
                    throw new BadSpecial("Unknown describe option: '" + c
                                         + "'");
            }
        }
        if (dbProductName.indexOf("HSQL") > -1) {
            listSet = listMDTableCols[HSQLDB_ELEMENT];
        } else if (dbProductName.indexOf("Oracle") > -1) {
            listSet = listMDTableCols[ORACLE_ELEMENT];
        } else {
            listSet = listMDTableCols[DEFAULT_ELEMENT];
        }
        displayResultSet(null, md.getTables(null, null, null, types),
                         listSet, filter);
    }

    /**
     * Process the current command as an SQL Statement
     */
    private void processSQL() throws SQLException {

        Statement statement = curConn.createStatement();

        // Really don't know whether to take the network latency hit here
        // in order to check autoCommit in order to set
        // possiblyUncommitteds more accurately.
        // I'm going with "NO" for now, since autoCommit will usually be off.
        // If we do ever check autocommit, we have to keep track of the
        // autocommit state when every SQL statement is run, since I may
        // be able to have uncommitted DML, turn autocommit on, then run
        // other DDL with autocommit on.  As a result, I could be running
        // SQL commands with autotommit on but still have uncommitted mods.
        possiblyUncommitteds.set(true);
        statement.execute(plMode ? dereference(curCommand, true)
                                 : curCommand);
        displayResultSet(statement, statement.getResultSet(), null, null);
    }

    /**
     * Display the given result set for user.
     * The last 3 params are to narrow down records and columns where
     * that can not be done with a where clause (like in metadata queries).
     *
     * @param statement The SQL Statement that the result set is for.
     *                  (I think that this is just for reporting purposes.
     * @param r         The ResultSet to display.
     * @param incCols   Optional list of which columns to include (i.e., if
     *                  given, then other columns will be skipped).
     * @param incFilter Optional case-insensitive substring.
     *                  Rows are skipped which to not contain this substring.
     */
    private void displayResultSet(Statement statement, ResultSet r,
                                  int[] incCols,
                                  String inFilter) throws SQLException {

        String filter      = ((inFilter == null) ? null
                                                 : inFilter.toUpperCase());
        int    updateCount = (statement == null) ? -1
                                                 : statement.getUpdateCount();
        switch (updateCount) {

            case -1 :
                if (r == null) {
                    stdprintln("No result", true);
                    break;
                }
                ResultSetMetaData m        = r.getMetaData();
                int               cols     = m.getColumnCount();
                int               incCount = (incCols == null) ? cols
                                                               : incCols
                                                                   .length;
                String            val;
                ArrayList         rows        = new ArrayList();
                String[]          headerArray = null;
                String[]          fieldArray;
                int[]             maxWidth = new int[incCount];
                int               insi;
                boolean           skip;
                String            dataType;
                boolean           ok;

                // STEP 1: GATHER DATA
                if (!htmlMode) {
                    for (int i = 0; i < maxWidth.length; i++) {
                        maxWidth[i] = 0;
                    }
                }
                boolean[] rightJust = new boolean[incCount];
                if (incCount > 1) {
                    insi        = -1;
                    headerArray = new String[incCount];
                    for (int i = 1; i <= cols; i++) {
                        if (incCols != null) {
                            skip = true;
                            for (int j = 0; j < incCols.length; j++) {
                                if (i == incCols[j]) {
                                    skip = false;
                                }
                            }
                            if (skip) {
                                continue;
                            }
                        }
                        headerArray[++insi] = m.getColumnLabel(i);
                        dataType            = m.getColumnTypeName(i);
                        rightJust[insi] = dataType.equals("INTEGER")
                                          || dataType.equals("NUMBER");
                        if (htmlMode) {
                            continue;
                        }
                        if (headerArray[insi].length() > maxWidth[insi]) {
                            maxWidth[insi] = headerArray[insi].length();
                        }
                    }
                }
                boolean filteredOut;
                EACH_ROW:
                while (r.next()) {
                    fieldArray  = new String[incCount];
                    insi        = -1;
                    filteredOut = filter != null;
                    for (int i = 1; i <= cols; i++) {
                        val = r.getString(i);
                        if (fetchingVar != null) {
                            userVars.put(fetchingVar, val);
                            fetchingVar = null;
                        }
                        if (incCols != null) {
                            skip = true;
                            for (int j = 0; j < incCols.length; j++) {
                                if (i == incCols[j]) {
                                    skip = false;
                                }
                            }
                            if (skip) {
                                continue;
                            }
                        }
                        if (val == null &&!r.wasNull()) {
                            val = "NON-CONVERTIBLE TYPE!";
                        }
                        if (filter != null
                                && val.toUpperCase().indexOf(filter) > -1) {
                            filteredOut = false;
                        }
                        fieldArray[++insi] = (val == null)
                                             ? (htmlMode ? "<I>null</I>"
                                                         : "null")
                                             : val;
                        if (htmlMode) {
                            continue;
                        }
                        if (fieldArray[insi].length() > maxWidth[insi]) {
                            maxWidth[insi] = fieldArray[insi].length();
                        }
                    }
                    if (!filteredOut) {
                        rows.add(fieldArray);
                    }
                }

                // STEP 2: DISPLAY DATA
                condlPrintln("<TABLE border='1'>", true);
                if (headerArray != null) {
                    condlPrint(htmlRow(COL_HEAD) + '\n' + PRE_TD, true);
                    for (int i = 0; i < headerArray.length; i++) {
                        condlPrint("<TD>" + headerArray[i] + "</TD>", true);
                        condlPrint(((i > 0) ? spaces(2)
                                            : "") + pad(
                                                headerArray[i], maxWidth[i],
                                                rightJust[i],
                                                (i < headerArray.length - 1
                                                 || rightJust[i])), false);
                    }
                    condlPrintln("\n" + PRE_TR + "</TR>", true);
                    condlPrintln("", false);
                    if (!htmlMode) {
                        for (int i = 0; i < headerArray.length; i++) {
                            condlPrint(((i > 0) ? spaces(2)
                                                : "") + divider(
                                                    maxWidth[i]), false);
                        }
                        condlPrintln("", false);
                    }
                }
                for (int i = 0; i < rows.size(); i++) {
                    condlPrint(htmlRow(((i % 2) == 0) ? COL_EVEN
                                                      : COL_ODD) + '\n'
                                                      + PRE_TD, true);
                    fieldArray = (String[]) rows.get(i);
                    for (int j = 0; j < fieldArray.length; j++) {
                        condlPrint("<TD>" + fieldArray[j] + "</TD>", true);
                        condlPrint(((j > 0) ? spaces(2)
                                            : "") + pad(
                                                fieldArray[j], maxWidth[j],
                                                rightJust[j],
                                                (j < fieldArray.length - 1
                                                 || rightJust[j])), false);
                    }
                    condlPrintln("\n" + PRE_TR + "</TR>", true);
                    condlPrintln("", false);
                }
                condlPrintln("</TABLE>", true);
                if (rows.size() != 1) {
                    stdprintln("\n" + rows.size() + " rows", true);
                }
                condlPrintln("<HR>", true);
                break;

            default :
                if (fetchingVar != null) {
                    userVars.put(fetchingVar, Integer.toString(updateCount));
                    fetchingVar = null;
                }
                if (updateCount != 0) {
                    stdprintln(Integer.toString(updateCount) + " row"
                               + ((updateCount == 1) ? ""
                                                     : "s") + " updated");
                }
                break;
        }
    }

    private final static int    COL_HEAD = 0,
                                COL_ODD  = 1,
                                COL_EVEN = 2
    ;
    private static final String PRE_TR   = spaces(4);
    private static final String PRE_TD   = spaces(8);

    /**
     * Print a properly formatted HTML &lt;TR&gt; command for the given
     * situation.
     *
     * @param colType Column type:  COL_HEAD, COL_ODD or COL_EVEN.
     */
    private static String htmlRow(int colType) {

        switch (colType) {

            case COL_HEAD :
                return PRE_TR + "<TR style='font-weight: bold;'>";

            case COL_ODD :
                return PRE_TR
                       + "<TR style='background: #94d6ef; font: normal "
                       + "normal 10px/10px Arial, Helvitica, sans-serif;'>";

            case COL_EVEN :
                return PRE_TR
                       + "<TR style='background: silver; font: normal "
                       + "normal 10px/10px Arial, Helvitica, sans-serif;'>";
        }
        return null;
    }

    /**
     * Returns a divider of hypens of requested length.
     *
     * @param len Length of output String.
     */
    private static String divider(int len) {
        return (len > DIVIDER.length()) ? DIVIDER
                                        : DIVIDER.substring(0, len);
    }

    /**
     * Returns a String of spaces of requested length.
     *
     * @param len Length of output String.
     */
    private static String spaces(int len) {
        return (len > SPACES.length()) ? SPACES
                                       : SPACES.substring(0, len);
    }

    /**
     * Pads given input string out to requested length with space
     * characters.
     *
     * @param inString Base string.
     * @param fulllen  Output String length.
     * @param rightJustify  True to right justify, false to left justify.
     */
    private static String pad(String inString, int fulllen,
                              boolean rightJustify, boolean doPad) {

        if (!doPad) {
            return inString;
        }
        int len = fulllen - inString.length();
        if (len < 1) {
            return inString;
        }
        String pad = spaces(len);
        return ((rightJustify ? pad
                              : "") + inString + (rightJustify ? ""
                                                               : pad));
    }

    /**
     * Display command history, which consists of complete or incomplete SQL
     * commands.
     */
    private void showHistory() {

        int      ctr = -1;
        String   s;
        String[] reversedList = new String[statementHistory.length];
        try {
            for (int i = curHist; i >= 0; i--) {
                s = statementHistory[i];
                if (s == null) {
                    return;
                }
                reversedList[++ctr] = s;
            }
            for (int i = statementHistory.length - 1; i > curHist; i--) {
                s = statementHistory[i];
                if (s == null) {
                    return;
                }
                reversedList[++ctr] = s;
            }
        } finally {
            if (ctr < 0) {
                stdprintln("<<<    No history yet    >>>");
                return;
            }
            for (int i = ctr; i >= 0; i--) {
                psStd.println(((i == 0) ? "BUFR"
                                        : ("-" + i + "  ")) + " **********************************************\n"
                                        + reversedList[i]);
            }
            psStd.println(
                "\n<<<  Copy a command to buffer like \"\\-3\"       "
                + "Re-execute buffer like \":;\"  >>>");
        }
    }

    /**
     * Return a SQL Command from command history.
     */
    private String commandFromHistory(int commandsAgo) throws BadSpecial {

        if (commandsAgo >= statementHistory.length) {
            throw new BadSpecial("History can only hold up to "
                                 + statementHistory.length + " commands");
        }
        String s =
            statementHistory[(statementHistory.length + curHist - commandsAgo) % statementHistory.length];
        if (s == null) {
            throw new BadSpecial("History doesn't go back that far");
        }
        return s;
    }

    /**
     * Push a command onto the history array (the first element of which
     * is the "Buffer").
     */
    private void setBuf(String inString) {

        curHist++;
        if (curHist == statementHistory.length) {
            curHist = 0;
        }
        statementHistory[curHist] = inString;
    }

    /**
     * Describe the columns of specified table.
     *
     * @param tableName  Table that will be described.
     */
    private void describe(String tableName) throws SQLException {

        Statement statement = curConn.createStatement();
        statement.execute("SELECT * FROM " + tableName + " WHERE 1 = 2");
        ResultSet         r    = statement.getResultSet();
        ResultSetMetaData m    = r.getMetaData();
        int               cols = m.getColumnCount();
        String            val;
        ArrayList         rows        = new ArrayList();
        String[]          headerArray = {
            "name", "datatype", "width", "no-nulls"
        };
        String[]          fieldArray;
        int[]             maxWidth  = {
            0, 0, 0, 0
        };
        boolean[]         rightJust = {
            false, false, true, false
        };

        // STEP 1: GATHER DATA
        for (int i = 0; i < headerArray.length; i++) {
            if (htmlMode) {
                continue;
            }
            if (headerArray[i].length() > maxWidth[i]) {
                maxWidth[i] = headerArray[i].length();
            }
        }
        for (int i = 0; i < cols; i++) {
            fieldArray    = new String[4];
            fieldArray[0] = m.getColumnName(i + 1);
            fieldArray[1] = m.getColumnTypeName(i + 1);
            fieldArray[2] = Integer.toString(m.getColumnDisplaySize(i + 1));
            fieldArray[3] =
                ((m.isNullable(i + 1) == java.sql.ResultSetMetaData.columnNullable)
                 ? (htmlMode ? "&nbsp;"
                             : "")
                 : "*");
            rows.add(fieldArray);
            for (int j = 0; j < fieldArray.length; j++) {
                if (fieldArray[j].length() > maxWidth[j]) {
                    maxWidth[j] = fieldArray[j].length();
                }
            }
        }

        // STEP 2: DISPLAY DATA
        condlPrint("<TABLE border='1'>\n" + htmlRow(COL_HEAD) + '\n'
                   + PRE_TD, true);
        for (int i = 0; i < headerArray.length; i++) {
            condlPrint("<TD>" + headerArray[i] + "</TD>", true);
            condlPrint(((i > 0) ? spaces(2)
                                : "") + pad(headerArray[i], maxWidth[i],
                                            rightJust[i],
                                            (i < headerArray.length - 1
                                             || rightJust[i])), false);
        }
        condlPrintln("\n" + PRE_TR + "</TR>", true);
        condlPrintln("", false);
        if (!htmlMode) {
            for (int i = 0; i < headerArray.length; i++) {
                condlPrint(((i > 0) ? spaces(2)
                                    : "") + divider(maxWidth[i]), false);
            }
            condlPrintln("", false);
        }
        for (int i = 0; i < rows.size(); i++) {
            condlPrint(htmlRow(((i % 2) == 0) ? COL_EVEN
                                              : COL_ODD) + '\n'
                                              + PRE_TD, true);
            fieldArray = (String[]) rows.get(i);
            for (int j = 0; j < fieldArray.length; j++) {
                condlPrint("<TD>" + fieldArray[j] + "</TD>", true);
                condlPrint(((j > 0) ? spaces(2)
                                    : "") + pad(fieldArray[j], maxWidth[j],
                                                rightJust[j],
                                                (j < fieldArray.length - 1
                                                 || rightJust[j])), false);
            }
            condlPrintln("\n" + PRE_TR + "</TR>", true);
            condlPrintln("", false);
        }
        condlPrintln("\n</TABLE>\n<HR>", true);
        r.close();
        statement.close();
    }

    static public String[] getTokenArray(String inString) {

        // I forget how to code a String array literal outside of a
        // definition.
        String[] mtString = {};
        if (inString == null) {
            return mtString;
        }
        StringTokenizer toker = new StringTokenizer(inString);
        String[]        sa    = new String[toker.countTokens()];
        for (int i = 0; i < sa.length; i++) {
            sa[i] = toker.nextToken();
        }
        return sa;
    }

    private boolean eval(String[] inTokens) throws BadSpecial {

        // dereference *VARNAME variables.
        // N.b. we work with a "copy" of the tokens.
        boolean  negate = inTokens.length > 0 && inTokens[0].equals("!");
        String[] tokens = new String[negate ? (inTokens.length - 1)
                                            : inTokens.length];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = (inTokens[i + (negate ? 1
                                              : 0)].length() > 1 && inTokens[i + (negate ? 1
                                                                                         : 0)].charAt(
                                                                                         0) == '*') ? ((String) userVars.get(
                                                                                             inTokens[i + (negate ? 1
                                                                                                                  : 0)]
                                                                                                                  .substring(
                                                                                                                      1)))
                                                                                                    : inTokens[i + (negate ? 1
                                                                                                                           : 0)];
            if (tokens[i] == null) {
                tokens[i] = "";
            }
        }
        if (tokens.length == 1) {
            return (tokens[0].length() > 0 &&!tokens[0].equals("0")) ^ negate;
        }
        if (tokens.length == 3) {
            if (tokens[1].equals("==")) {
                return tokens[0].equals(tokens[2]) ^ negate;
            }
            if (tokens[1].equals("!=") || tokens[1].equals("<>")
                    || tokens[1].equals("><")) {
                return (!tokens[0].equals(tokens[2])) ^ negate;
            }
            if (tokens[1].equals(">")) {
                return (tokens[0].length() > tokens[2].length() || ((tokens[0].length() == tokens[2].length()) && tokens[0].compareTo(tokens[2]) > 0))
                       ^ negate;
            }
            if (tokens[1].equals("<")) {
                return (tokens[2].length() > tokens[0].length() || ((tokens[2].length() == tokens[0].length()) && tokens[2].compareTo(tokens[0]) > 0))
                       ^ negate;
            }
        }
        throw new BadSpecial("Unrecognized logical operation");
    }

    private void closeQueryOutputStream() {

        if (pwQuery == null) {
            return;
        }
        if (htmlMode) {
            pwQuery.println("</BODY></HTML>");
            pwQuery.flush();
        }
        pwQuery.close();
        pwQuery = null;
    }

    /**
     * Print to psStd and possibly pwQuery iff current HTML mode matches
     * supplied printHtml.
     */
    private void condlPrintln(String s, boolean printHtml) {

        if ((printHtml &&!htmlMode) || (htmlMode &&!printHtml)) {
            return;
        }
        psStd.println(s);
        if (pwQuery != null) {
            pwQuery.println(s);
            pwQuery.flush();
        }
    }

    /**
     * Print to psStd and possibly pwQuery iff current HTML mode matches
     * supplied printHtml.
     */
    private void condlPrint(String s, boolean printHtml) {

        if ((printHtml &&!htmlMode) || (htmlMode &&!printHtml)) {
            return;
        }
        psStd.print(s);
        if (pwQuery != null) {
            pwQuery.print(s);
            pwQuery.flush();
        }
    }

    private static String formatNicely(Map map) {

        String       key;
        StringBuffer sb = new StringBuffer();
        Iterator     it = (new TreeMap(map)).keySet().iterator();
        while (it.hasNext()) {
            key = (String) it.next();
            sb.append("    " + key + ": (" + map.get(key) + ")\n");
        }
        return sb.toString();
    }
}
