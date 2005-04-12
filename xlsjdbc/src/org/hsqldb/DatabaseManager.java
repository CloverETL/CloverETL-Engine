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


package org.hsqldb;

import java.util.Vector;

import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.store.ValuePool;

/**
 * Multifunction class with all static methods.<p>
 *
 * Handles initial attempts to connect to HSQLDB databases within the JVM
 * (or a classloader within the JVM). Opens the database if it is not open
 * or connects to it if it is already open. This allows the same database to
 * be used by different instances of Server and by direct connections.<p>
 *
 * Maintains a map of Server instances and notifies each server when its
 * database has shut down.<p>
 *
 * Maintains a reference to the timer used for file locks and logging.<p>
 *
 * Parses a connection URL into parts.
 *
 *
 * @author fred@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class DatabaseManager {

    // Database and Server registry

    /** provides unique ID's for the Databases currently in registry */
    private static int dbIDCounter;

    /** name to Database mapping for mem: databases */
    static final HashMap memDatabaseMap = new HashMap();

    /** File to Database mapping for file: databases */
    static final HashMap fileDatabaseMap = new HashMap();

    /** File to Database mapping for res: databases */
    static final HashMap resDatabaseMap = new HashMap();

    /** id number to Database for Databases currently in registry */
    static final IntKeyHashMap databaseIDMap = new IntKeyHashMap();

    /**
     * Returns a vector containing the URI (type + path) for all the databases.
     */
    public static Vector getDatabaseURIs() {

        Vector   v  = new Vector();
        Iterator it = databaseIDMap.values().iterator();

        while (it.hasNext()) {
            Database db = (Database) it.next();

            v.addElement(db.getURI());
        }

        return v;
    }

    /**
     * Closes all the databases using the given mode.<p>
     *
     * CLOSEMODE_IMMEDIATELY = -1;
     * CLOSEMODE_NORMAL      = 0;
     * CLOSEMODE_COMPACT     = 1;
     * CLOSEMODE_SCRIPT      = 2;
     */
    public static void closeDatabases(int mode) {

        Iterator it = databaseIDMap.values().iterator();

        while (it.hasNext()) {
            Database db = (Database) it.next();

            try {
                db.close(mode);
            } catch (HsqlException e) {}
        }
    }

    /**
     * Used by server to open a new session
     */
    static Session newSession(int dbID, String user,
                              String password) throws HsqlException {

        Database db = (Database) databaseIDMap.get(dbID);

        return db.connect(user, password);
    }

    /**
     * Used by in-process connections and by Servlet
     */

// loosecannon1@users 1.7.2 patch properties on the JDBC URL
    public static Session newSession(String type, String path, String user,
                                     String password, boolean ifexists,
                                     HsqlProperties props)
                                     throws HsqlException {

        Database db = getDatabase(type, path, ifexists, props);

        return db.connect(user, password);
    }

    /**
     * This returns an existing session. Used with repeat HTTP connections
     * belonging to the same JDBC Conenction / HSQL Session pair.
     */
    static Session getSession(String type, String path,
                              int sessionId) throws HsqlException {

        if (path == null) {
            return null;
        }

        Database db = lookupDatabaseObject(type, path);

        return db.sessionManager.getSession(sessionId);
    }

    /**
     * Used by server to open or create a database
     */

// loosecannon1@users 1.7.2 patch properties on the JDBC URL
    static int getDatabase(String type, String path, Server server,
                           HsqlProperties props) throws HsqlException {

        Database db = getDatabase(type, path, false, props);

        registerServer(server, db);

        return db.databaseID;
    }

    /**
     * This has to be improved once a threading model is in place.
     * Current behaviour:
     *
     * Attempts to connect to different databases do not block. Two db's can
     * open simultaneously.
     *
     * Attempts to connect to a db while it is opening or closing will block
     * until the db is open or closed. At this point the db state is either
     * DATABASE_ONLINE (after db.open() has returned) which allows a new
     * connection to be made, or the state is DATABASE_SHUTDOWN which means
     * the db can be reopened for the new connection).
     *
     */

// loosecannon1@users 1.7.2 patch properties on the JDBC URL
    static Database getDatabase(String type, String path, boolean ifexists,
                                HsqlProperties props) throws HsqlException {

        // If the (type, path) pair does not correspond to a registered
        // instance, then getDatabaseObject() returns a newly constructed
        // and registered Database instance.
        // The database state will be DATABASE_SHUTDOWN,
        // which means that the switch below will attempt to
        // open the database instance.
        Database db = getDatabaseObject(type, path, ifexists, props);

        synchronized (db) {
            switch (db.getState()) {

                case Database.DATABASE_ONLINE :
                    break;

                case Database.DATABASE_SHUTDOWN :

                    // if the database was shutdown while this attempt
                    // was waiting, add the database back to the registry
                    if (lookupDatabaseObject(type, path) == null) {
                        addDatabaseObject(type, path, db);
                    }

                    db.open();
                    break;

                // This state will currently not be reached as Database.Close() is
                // called while a lock is held on the database.
                // If we remove the lock from this method and a database is
                // being shutdown by a thread and in the meantime another thread
                // attempts to connect to the db. The threads could belong to
                // different server instances or be in-process.
                case Database.DATABASE_CLOSING :

                // this case will not be reached as the state is set and
                // cleared within the db.open() call above, which is called
                // from this synchronized block
                // it is here simply as a placeholder for future development
                case Database.DATABASE_OPENING :
                    throw Trace.error(Trace.DATABASE_ALREADY_IN_USE,
                                      Trace.DatabaseManager_getDatabase);
            }
        }

        return db;
    }

// loosecannon1@users 1.7.2 patch properties on the JDBC URL
    private static synchronized Database getDatabaseObject(String type,
            String path, boolean ifexists,
            HsqlProperties props) throws HsqlException {

        Database db;
        Object   key = path;
        HashMap  databaseMap;

        if (type == S_FILE) {
            databaseMap = fileDatabaseMap;
            key         = filePathToKey(path);
        } else if (type == S_RES) {
            databaseMap = resDatabaseMap;
        } else {
            databaseMap = memDatabaseMap;
        }

        db = (Database) databaseMap.get(key);

        if (db == null) {
            db = new Database(type, path, type + key, ifexists, props);
            db.databaseID = dbIDCounter;

            databaseIDMap.put(dbIDCounter, db);

            dbIDCounter++;

            databaseMap.put(key, db);
        }

        return db;
    }

    /**
     * Looks up database of a given type and path in the registry. Returns
     * null if there is none.
     */
    private static synchronized Database lookupDatabaseObject(String type,
            String path) throws HsqlException {

        Object  key = path;
        HashMap databaseMap;

        if (type == S_FILE) {
            databaseMap = fileDatabaseMap;
            key         = filePathToKey(path);
        } else if (type == S_RES) {
            databaseMap = resDatabaseMap;
        } else {
            databaseMap = memDatabaseMap;
        }

        return (Database) databaseMap.get(key);
    }

    /**
     * Adds a database to the registry. Returns
     * null if there is none.
     */
    private static synchronized void addDatabaseObject(String type,
            String path, Database db) throws HsqlException {

        Object  key = path;
        HashMap databaseMap;

        if (type == S_FILE) {
            databaseMap = fileDatabaseMap;
            key         = filePathToKey(path);
        } else if (type == S_RES) {
            databaseMap = resDatabaseMap;
        } else {
            databaseMap = memDatabaseMap;
        }

        databaseIDMap.put(db.databaseID, db);
        databaseMap.put(key, db);
    }

    /**
     * Removes the database from registry.
     */
    static void removeDatabase(Database database) {

        int     dbID = database.databaseID;
        String  type = database.getType();
        String  path = database.getPath();
        Object  key  = path;
        HashMap databaseMap;

        notifyServers(database);

        if (type == S_FILE) {
            databaseMap = fileDatabaseMap;

// boucherb@users 20040124 - patch 1.7.2
// Under the current contract, it's essentially impossible for an
// exception to get thrown here, because the database could not
// have been registered successfully before hand using the same
// path
//
// Eventually, we might think about storing the key with the
// database instance so as to avoid this unnecessary additional
// conversion and highly unlikely corner case handling.
            try {
                key = filePathToKey(path);
            } catch (HsqlException e) {
                Iterator it       = databaseMap.keySet().iterator();
                Object   foundKey = null;

                while (it.hasNext()) {
                    Object currentKey = it.next();

                    if (databaseMap.get(currentKey) == database) {
                        foundKey = currentKey;

                        break;
                    }
                }

                if (foundKey == null) {
                    e.printStackTrace();

                    // ??? return;
                } else {
                    key = foundKey;
                }
            }
        } else if (type == S_RES) {
            databaseMap = resDatabaseMap;
        } else {
            databaseMap = memDatabaseMap;
        }

        databaseIDMap.remove(dbID);
        databaseMap.remove(key);

        if (databaseIDMap.isEmpty()) {
            ValuePool.resetPool();
        }
    }

    /**
     * Maintains a map of servers to sets of databases.
     * Servers register each of their databases.
     * When a database is shutdown, all the servers accessing it are notified.
     * The database is then removed form the sets for all servers and the
     * servers that have no other database are removed from the map.
     */
    static HashMap serverMap = new HashMap();

    /**
     * Deregisters a server completely.
     */
    static void deRegisterServer(Server server) {
        serverMap.remove(server);
    }

    /**
     * Deregisters a server as serving a given database. Not yet used.
     */
    private static void deRegisterServer(Server server, Database db) {

        Iterator it = serverMap.values().iterator();

        for (; it.hasNext(); ) {
            HashSet databases = (HashSet) it.next();

            databases.remove(db);

            if (databases.isEmpty()) {
                it.remove();
            }
        }
    }

    /**
     * Registers a server as serving a given database.
     */
    private static void registerServer(Server server, Database db) {

        if (!serverMap.containsKey(server)) {
            serverMap.put(server, new HashSet());
        }

        HashSet databases = (HashSet) serverMap.get(server);

        databases.add(db);
    }

    /**
     * Notifies all servers that serve the database that the database has been
     * shutdown.
     */
    private static void notifyServers(Database db) {

        Iterator it = serverMap.keySet().iterator();

        for (; it.hasNext(); ) {
            Server  server    = (Server) it.next();
            HashSet databases = (HashSet) serverMap.get(server);

            if (databases.contains(db)) {
                server.notify(ServerConstants.SC_DATABASE_SHUTDOWN,
                              db.databaseID);
            }
        }
    }

    static boolean isServerDB(Database db) {

        Iterator it = serverMap.keySet().iterator();

        for (; it.hasNext(); ) {
            Server  server    = (Server) it.next();
            HashSet databases = (HashSet) serverMap.get(server);

            if (databases.contains(db)) {
                return true;
            }
        }

        return false;
    }

    // URL parsing

    /**
     * Parses the url into components that are returned in a properties
     * object. <p>
     *
     * The following components are isolated: <p>
     *
     * <ul>
     * url: the original url<p>
     * connection_type: a static string that indicate the protocol. If the
     * url does not begin with a valid protocol, null is returned by this
     * method instead of the properties object.<p>
     * host: name of host in networked modes in lowercase<p>
     * port: port number in networked mode, or 0 if not present<p>
     * path: path of the resource on server in networked modes,
     * / (slash) in all cases apart from
     * servlet path which is / (slash) plus the name of the servlet<p>
     * database: database name. For memory, resource and networked modes,
     * this is returned in lowercase, for file databases the original
     * case of characters is preserved. Returns empty string if name is not
     * present in the url.<p>
     * for each protocol if port number is not in the url<p>
     * Additional connection properties specified as key/value pairs.
     * </ul>
     * @return null returned if the part that should represent the port is not
     * an integer or the part for database name is empty.
     * Empty HsqlProperties returned if if url does not begin with valid
     * protocol and could refer to another JDBC driver.
     *
     */
    static HsqlProperties parseURL(String url, boolean hasPrefix) {

        String         urlImage = url.toLowerCase();
        HsqlProperties props    = new HsqlProperties();

        if (hasPrefix &&!urlImage.startsWith(S_URL_PREFIX)) {
            return props;
        }

        int     pos  = hasPrefix ? S_URL_PREFIX.length()
                                 : 0;
        String  type = null;
        String  host;
        int     port = 0;
        String  database;
        String  path;
        boolean isNetwork = false;

        props.setProperty("url", url);

        int semicolpos = url.indexOf(';', pos);

        if (semicolpos < 0) {
            semicolpos = url.length();
        } else {
            String arguments = urlImage.substring(semicolpos + 1,
                                                  urlImage.length());
            HsqlProperties extraProps =
                HsqlProperties.delimitedArgPairsToProps(arguments, "=", ";",
                    null);

            //todo - check if properties have valid names / values
            props.addProperties(extraProps);
        }

        if (semicolpos == pos + 1 && urlImage.startsWith(S_DOT, pos)) {
            type = S_DOT;
        } else if (urlImage.startsWith(S_MEM, pos)) {
            type = S_MEM;
        } else if (urlImage.startsWith(S_FILE, pos)) {
            type = S_FILE;
        } else if (urlImage.startsWith(S_RES, pos)) {
            type = S_RES;
        } else if (urlImage.startsWith(S_ALIAS, pos)) {
            type = S_ALIAS;
        } else if (urlImage.startsWith(S_HSQL, pos)) {
            type      = S_HSQL;
            port      = ServerConstants.SC_DEFAULT_HSQL_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HSQLS, pos)) {
            type      = S_HSQLS;
            port      = ServerConstants.SC_DEFAULT_HSQLS_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HTTP, pos)) {
            type      = S_HTTP;
            port      = ServerConstants.SC_DEFAULT_HTTP_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HTTPS, pos)) {
            type      = S_HTTPS;
            port      = ServerConstants.SC_DEFAULT_HTTPS_SERVER_PORT;
            isNetwork = true;
        }

        if (type == null) {
            type = S_FILE;
        } else if (type == S_DOT) {
            type = S_MEM;

            // keep pos
        } else {
            pos += type.length();
        }

        props.setProperty("connection_type", type);

        if (isNetwork) {
            int slashpos = url.indexOf('/', pos);

            if (slashpos < pos || slashpos > semicolpos) {
                slashpos = semicolpos;
            }

            int colonpos = url.indexOf(':', pos);

            if (colonpos < pos || colonpos > slashpos) {
                colonpos = slashpos;
            } else {
                try {
                    port = Integer.parseInt(url.substring(colonpos + 1,
                                                          slashpos));
                } catch (NumberFormatException e) {
                    return null;
                }
            }

            host = urlImage.substring(pos, colonpos);

            int secondslashpos = url.lastIndexOf('/', semicolpos);

            if (secondslashpos < pos) {
                path     = "/";
                database = "";
            } else if (secondslashpos == slashpos) {
                path     = "/";
                database = urlImage.substring(secondslashpos + 1, semicolpos);
            } else {
                path     = url.substring(slashpos, secondslashpos);
                database = urlImage.substring(secondslashpos + 1, semicolpos);
            }

            props.setProperty("port", port);
            props.setProperty("host", host);
            props.setProperty("path", path);
        } else {
            if (type == S_MEM || type == S_RES) {
                database = urlImage.substring(pos, semicolpos).toLowerCase();

                if (type == S_RES) {
                    if (database.indexOf('/') != 0) {
                        database = '/' + database;
                    }
                }
            } else {
                database = url.substring(pos, semicolpos);
            }

            if (database.length() == 0) {
                return null;
            }
        }

        props.setProperty("database", database);

        return props;
    }

    static final String        S_DOT        = ".";
    public static final String S_MEM        = "mem:";
    public static final String S_FILE       = "file:";
    public static final String S_RES        = "res:";
    public static final String S_ALIAS      = "alias:";
    public static final String S_HSQL       = "hsql://";
    public static final String S_HSQLS      = "hsqls://";
    public static final String S_HTTP       = "http://";
    public static final String S_HTTPS      = "https://";
    public static final String S_URL_PREFIX = "jdbc:hsqldb:";

/*
    public static void main(String[] argv) {

        parseURL("JDBC:hsqldb:../data/mydb.db", true);
        parseURL("JDBC:hsqldb:../data/mydb.db;ifexists=true", true);
        parseURL("JDBC:hsqldb:HSQL://localhost:9000/mydb", true);
        parseURL(
            "JDBC:hsqldb:Http://localhost:8080/servlet/org.hsqldb.Servlet/mydb;ifexists=true",
            true);
        parseURL("JDBC:hsqldb:Http://localhost/servlet/org.hsqldb.Servlet/",
                 true);
        parseURL("JDBC:hsqldb:hsql://myhost", true);
    }
*/

    // Garbage Collection
    static void gc() {

        if ((Record.gcFrequency > 0)
                && (Record.memoryRecords > Record.gcFrequency)) {
            Record.memoryRecords = 0;

            System.gc();
        }
    }

    // Timer
    private static final HsqlTimer timer = new HsqlTimer();

    public static HsqlTimer getTimer() {
        return timer;
    }

    // converts file path to database lookup key, converting any
    // any thrown exception to an HsqlException in the process
    private static Object filePathToKey(String path) throws HsqlException {

        try {
            return FileUtil.canonicalFile(path);
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR, e.toString());
        }
    }
}
