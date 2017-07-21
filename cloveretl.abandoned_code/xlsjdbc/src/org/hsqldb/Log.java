/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals 
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2004, The HSQL Development Group
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


package org.hsqldb;

import java.io.File;
import java.io.IOException;

import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.ZipUnzipFile;
import org.hsqldb.scriptio.ScriptReaderBase;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.scriptio.ScriptWriterText;

// fredt@users 20020215 - patch 1.7.0 by fredt
// to move operations on the database.properties files to new
// class HsqlDatabaseProperties
// fredt@users 20020220 - patch 488200 by xclayl@users - throw exception
// throw addded to all methods relying on file io
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020405 - patch 1.7.0 by fredt - no change in db location
// because important information about the database is now stored in the
// *.properties file, all database files should be in the same folder as the
// *.properties file
// tony_lai@users 20020820 - export hsqldb.log_size to .properties file
// tony_lai@users 20020820 - changes to shutdown compact to save memory
// fredt@users 20020910 - patch 1.7.1 by Nitin Chauhan - code improvements
// fredt@users 20021208 - ongoing revamp
// fredt@users 20021212 - do not rewrite the *.backup file if the *.data
// file has not been updated in the current session.
// boucherb@users 20030510 - patch 1.7.2 consolidated all periodic database
// tasks in one timed task queue
/*
 todo - when a *.script file that has no properties file is opened, it is
 always assumed that the cache_version is 1.6.0, which may not be true;
 - when both *.script and *.backup exist, there is a problem openning. the
 backup does not get deflated into *.data and an attemp is made to read the
 index roots from the empty *.data file.

 */

/**
 *  This class is responsible for most file handling. An HSQLDB database
 *  consists of a .properties file, a .script file (contains an SQL script),
 *  a .data file (contains data of cached tables) and a .backup file
 *  (contains the compressed .data file). Since 1.7.2 a .log file is also
 *  present. <p>
 *
 *  This is an example of the .properties file. The version and the modified
 *  properties are automatically created by the database and should not be
 *  changed manually: <pre>
 *  modified=no
 *  version=1.43
 *  </pre>
 *  The following lines are optional, this means they are not created
 *  automatically by the database, but they are interpreted if they exist in
 *  the .script file. They have to be created manually if required. If they
 *  don't exist the default is used. This are the defaults of the database
 *  'test':
 *  <pre>
 *  readonly=false
 *  </pre>
 *
 * @version 1.7.2
 */
class Log {

    // block size for copying data
    private static final int       COPY_BLOCK_SIZE = 1 << 16;
    private HsqlDatabaseProperties pProperties;
    private String                 sName;
    private Database               dDatabase;
    private ScriptWriterText       dbLogWriter;
    private String                 sFileScript;
    private String                 sFileCache;
    private String                 sFileBackup;
    private String                 sFileLog;
    private boolean                bRestoring;
    private boolean                filesReadOnly;

    // metadata visibilty when not private
    int maxLogSize;
    int iLogCount;
    int scriptFormat;

    //private Thread tRunner;
    private Object        timerTask;
    volatile int          writeDelay = 60;
    private DataFileCache cCache;

    // used for tracing
    private static final HsqlTimer timer = DatabaseManager.getTimer();

    /**
     *  Constructor declaration
     *
     * @param  db
     * @param  name
     * @exception  HsqlException  Description of the Exception
     */
    Log(Database db, String name) throws HsqlException {

        dDatabase   = db;
        sName       = name;
        pProperties = db.getProperties();

        if (!db.isFilesReadOnly()) {
            Runnable r = new LogSyncRunner();

            timerTask = timer.schedulePeriodicallyAfter(0, 1000, r, false);
        }
    }

    protected class LogSyncRunner implements Runnable {

        private int ticks = 0;

        public void run() {

            try {
                if (++ticks >= writeDelay && dbLogWriter != null) {
                    dbLogWriter.sync();

                    ticks = 0;
                }

                // todo: try to do Cache.cleanUp() here, too
            } catch (Exception e) {

                // ignore exceptions
                // may be InterruptedException or IOException
                if (Trace.TRACE) {
                    Trace.printSystemOut(e.toString());
                }
            }
        }
    }

    /**
     *  Method declaration
     *
     * @param  delay
     */
    void setWriteDelay(int delay) {

        writeDelay = delay;

        if (dbLogWriter != null) {
            dbLogWriter.setWriteDelay(delay);
        }
    }

    void readScript() throws HsqlException {

        bRestoring = true;

        try {
            if (dDatabase.isFilesInJar() || FileUtil.exists(sFileScript)) {
                ScriptReaderBase scr =
                    ScriptReaderBase.newScriptReader(dDatabase, sFileScript,
                                                     scriptFormat);

                scr.readAll(dDatabase.sessionManager.getSysSession());
                scr.close();
            }
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, e.getMessage());
        }

        if (!dDatabase.isFilesInJar() && FileUtil.exists(sFileLog)) {
            ScriptRunner.runScript(dDatabase, sFileLog,
                                   ScriptWriterBase.SCRIPT_TEXT_170);
        }

        bRestoring = false;
    }

    /**
     * When opening a database, the hsqldb.compatible_version property is
     * used to determine if this version of the engine is equal to or greater
     * than the earliest version of the engine capable of opening that
     * database.<p>
     *
     * @throws  HsqlException
     */
    void open() throws HsqlException {

        // Allows the user to set log size in the properties file.
        int logMegas = pProperties.getIntegerProperty("hsqldb.log_size", 0);

        maxLogSize = logMegas * 1024 * 1024;
        scriptFormat = pProperties.getIntegerProperty("hsqldb.script_format",
                ScriptWriterBase.SCRIPT_TEXT_170);
        filesReadOnly = dDatabase.isFilesReadOnly();
        sFileScript   = sName + ".script";
        sFileLog      = sName + ".log";
        sFileCache    = sName + ".data";
        sFileBackup   = sName + ".backup";

        if (filesReadOnly) {
            if (cCache != null) {
                cCache.open(true);
            }

            reopenAllTextCaches();
            readScript();

            return;
        }

        boolean needbackup = false;
        int     state      = pProperties.getDBModified();

        try {
            if (state == HsqlDatabaseProperties.FILES_MODIFIED_NEW) {
                FileUtil.renameOverwrite(sFileScript + ".new", sFileScript);
                FileUtil.renameOverwrite(sFileBackup + ".new", sFileBackup);
                FileUtil.delete(sFileLog);
            } else if (state == HsqlDatabaseProperties.FILES_MODIFIED) {

                // recovering after a crash (or forgot to close correctly)
                restoreBackup();

                needbackup = true;
            }
        } catch (IOException e) {}

        pProperties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED);

        if (cCache != null) {
            cCache.open(false);
        }

        reopenAllTextCaches();

        if (!dDatabase.isNew) {
            readScript();

            if (needbackup) {
                close(false, true);
                pProperties.setDBModified(
                    HsqlDatabaseProperties.FILES_MODIFIED);

                if (cCache != null) {
                    cCache.open(false);
                }

                reopenAllTextCaches();
            }
        }

        openLog();
    }

    DataFileCache getCache() throws HsqlException {

        if (dDatabase.isFilesInJar()) {
            return null;
        }

        if (cCache == null) {
            cCache = new DataFileCache(sFileCache, this.dDatabase);

            cCache.open(filesReadOnly);
        }

        return cCache;
    }

    /**
     *  Method declaration
     */
    void stop() {

        if (timerTask != null) {
            HsqlTimer.cancel(timerTask);

            timerTask = null;
        }
    }

    /**
     *  Method declaration
     *
     * @param  compact
     * @param  cache
     * @throws  HsqlException
     */
    void close(boolean compact, boolean cache) throws HsqlException {

        boolean needsbackup = false;

        if (filesReadOnly) {
            return;
        }

        // no more logging
        closeLog();

        // create '.script.new' (for this the cache may be still required)
        writeScript(compact);

        // flush the cache (important: after writing the script)
        if (cache && cCache != null) {
            needsbackup = cCache.fileModified;

            cCache.close();
        }

        closeAllTextCaches(compact);

        // create '.backup.new' using the '.data'
        if (cache && needsbackup &&!compact) {
            cCache.backup(sFileBackup + ".new");
        }

        // we have the new files
        pProperties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED_NEW);

        try {

            // old files can be removed and new files renamed
            FileUtil.renameOverwrite(sFileScript + ".new", sFileScript);
            FileUtil.delete(sFileLog);

            if (cache && needsbackup &&!compact) {
                FileUtil.renameOverwrite(sFileBackup + ".new", sFileBackup);
            }
        } catch (IOException e) {}

        // now its done completely
        pProperties.setProperty("version", org.hsqldb.jdbc.jdbcUtil.VERSION);
        pProperties.setProperty("hsqldb.compatible_version", "1.7.2");

        // this one last to save the props
        pProperties.setDBModified(HsqlDatabaseProperties.FILES_NOT_MODIFIED);

        if (compact) {
            try {

                // cancel the log sync task of this process (just for security)
                stop();

                // delete the .data so then a new file is created
                // delete won't always work with NIO so reset the file
                if (FileUtil.exists(sFileCache)) {
                    DataFileCache.resetFreePos(sFileCache);
                }

                FileUtil.delete(sFileCache);
                FileUtil.delete(sFileBackup);
            } catch (IOException e) {}
        }
    }

    /**
     *  Method declaration
     *
     * @throws  HsqlException
     */
    void checkpoint(boolean defrag) throws HsqlException {

        if (filesReadOnly) {
            return;
        }

        if (defrag && cCache != null) {
            cCache.defrag();
        }

        // close as normal
        close(false, !defrag);
        pProperties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED);

        if (!defrag && cCache != null) {
            cCache.open(false);
        }

        reopenAllTextCaches();
        openLog();
    }

    /**
     *  Method declaration
     *
     * @param  megas
     */
    void setLogSize(int megas) {

        pProperties.setProperty("hsqldb.log_size", megas);

        maxLogSize = megas * 1024 * 1024;
    }

    /**
     *  Method declaration
     *
     * @param  type
     */
    void setScriptType(int type) throws HsqlException {

        boolean needsCheckpoint = scriptFormat != type;

        scriptFormat = type;

        pProperties.setProperty("hsqldb.script_format", scriptFormat);

        if (needsCheckpoint) {
            checkpoint(false);
        }
    }

    /**
     *  Method declaration
     *
     * @param  c
     * @param  s
     * @throws  HsqlException
     */
    void writeStatement(Session c, String s) throws HsqlException {

        if (s == null || s.length() == 0) {
            return;
        }

        if (filesReadOnly || bRestoring) {
            return;
        }

        int id = (c == null) ? 0
                             : c.getId();

        try {
            dbLogWriter.writeLogStatement(s, id);
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, sFileLog);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            checkpoint(false);
        }
    }

    void writeInsertStatement(Session c, Table t,
                              Object[] row) throws HsqlException {

        if (filesReadOnly || bRestoring) {
            return;
        }

        int id = (c == null) ? 0
                             : c.getId();

        try {
            dbLogWriter.writeRow(id, t, row);
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, sFileLog);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            checkpoint(false);
        }
    }

    void writeDeleteStatement(Session c, Table t,
                              Object[] row) throws HsqlException {

        if (filesReadOnly || bRestoring) {
            return;
        }

        int id = (c == null) ? 0
                             : c.getId();

        try {
            dbLogWriter.writeDeleteStatement(id, t, row);
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, sFileLog);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            checkpoint(false);
        }
    }

    void writeSequenceStatement(Session c,
                                NumberSequence s) throws HsqlException {

        if (filesReadOnly || bRestoring) {
            return;
        }

        int id = (c == null) ? 0
                             : c.getId();

        try {
            dbLogWriter.writeSequenceStatement(id, s);
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, sFileLog);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            checkpoint(false);
        }
    }

    void synchLog() {
        dbLogWriter.sync();
    }

    void writeCommit() throws HsqlException {
        dbLogWriter.sync();
    }

    /**
     *  Method declaration
     *
     * @throws  HsqlException
     */
    void shutdown() throws HsqlException {

        stop();

        if (cCache != null) {
            cCache.closeFile();

            cCache = null;
        }

        shutdownAllTextCaches();
        closeLog();
    }

    /**
     *  Method declaration
     *
     * @throws  HsqlException
     */
    private void restoreBackup() throws HsqlException {

        try {

            // the cache file must be deleted anyway
            // the backup may not exist because it was never made or is empty
            if (FileUtil.exists(sFileCache)) {
                DataFileCache.resetFreePos(sFileCache);
            }

            FileUtil.delete(sFileCache);
        } catch (IOException e) {}

        try {
            ZipUnzipFile.decompressFile(sFileBackup, sFileCache);
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR, Trace.Message_Pair,
                              new Object[] {
                sFileBackup, e.getMessage()
            });
        }
    }

    /**
     *  Method declaration
     *
     * @throws  HsqlException
     */
    private void openLog() throws HsqlException {

        try {
            dbLogWriter = new ScriptWriterText(dDatabase, sFileLog, false,
                                               false);

            dbLogWriter.setWriteDelay(writeDelay);

            Session[] sessions = dDatabase.sessionManager.getAllSessions();

            for (int i = 0; i < sessions.length; i++) {
                Session session = sessions[i];

                if (session.isAutoCommit() == false) {
                    dbLogWriter.writeLogStatement(
                        session.getAutoCommitStatement(), session.getId());
                }
            }
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR, sFileScript);
        }
    }

    /**
     *  Method declaration
     *
     * @throws  HsqlException
     */
    private synchronized void closeLog() throws HsqlException {

        if (dbLogWriter != null) {
            dbLogWriter.close();

            dbLogWriter = null;
        }
    }

    /**
     *  Method declaration
     *
     * @param  full
     * @throws  HsqlException
     */
    private void writeScript(boolean full) throws HsqlException {

        try {
            FileUtil.delete(sFileScript + ".new");
        } catch (IOException e) {}

        // script; but only positions of cached tables, not full
        //fredt - to do - flag for chache set index
        ScriptWriterBase scw = ScriptWriterBase.newScriptWriter(dDatabase,
            sFileScript + ".new", full, true, scriptFormat);

        scw.writeAll();
        scw.close();
    }

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - text tables
    private HashMap textCacheList = new HashMap();

    DataFileCache openTextCache(Table table, String source,
                                boolean readOnlyData,
                                boolean reversed) throws HsqlException {

        closeTextCache(table);

        if (!pProperties.isPropertyTrue("textdb.allow_full_path")
                && !new File(source).isAbsolute()) {
            if (source.indexOf("..") != -1) {
                throw (Trace.error(Trace.ACCESS_IS_DENIED, source));
            }

            String path =
                new File(new File(sName).getAbsolutePath()).getParent();

            if (path != null) {
                source = path + File.separator + source;
            }
        }

        TextCache c;
        int       type;

        if (reversed) {
            c = new TextCache(source, table);
        } else {
            c = new TextCache(source, table);
        }

        c.open(readOnlyData || filesReadOnly);
        textCacheList.put(table.tableName, c);

        return c;
    }

    void closeTextCache(Table table) throws HsqlException {

        TextCache c = (TextCache) textCacheList.remove(table.tableName);

        if (c != null) {
            c.close();
        }
    }

    void closeAllTextCaches(boolean compact) throws HsqlException {

        Iterator it = textCacheList.values().iterator();

        while (it.hasNext()) {
            if (compact) {
                ((TextCache) it.next()).purge();
            } else {
                ((TextCache) it.next()).close();
            }
        }
    }

    void reopenAllTextCaches() throws HsqlException {

        Iterator it = textCacheList.values().iterator();

        while (it.hasNext()) {
            ((TextCache) it.next()).reopen();
        }
    }

    void shutdownAllTextCaches() throws HsqlException {

        Iterator it = textCacheList.values().iterator();

        while (it.hasNext()) {
            ((TextCache) it.next()).closeFile();
        }

        textCacheList = new HashMap();
    }
}
