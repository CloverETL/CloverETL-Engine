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

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.Timestamp;

import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.store.ValuePool;

// fredt@users - 1.7.2 - structural modifications to allow inheritance
// boucherb@users - 1.7.2 - 20020225
// - factored out all reusable code into DIXXX support classes
// - completed Fred's work on allowing inheritance
// boucherb@users - 1.7.2 - 20020304 - bug fixes, refinements, better java docs

/**
 * Extends DatabaseInformationMain to provide additional system table
 * support. <p>
 *
 * @author boucherb@users
 * @version 1.7.2
 * @since HSQLDB 1.7.2
 */
final class DatabaseInformationFull
extends org.hsqldb.DatabaseInformationMain {

    /** Provides SQL function/procedure reporting support. */
    protected DIProcedureInfo pi;

    /**
     * Constructs a new DatabaseInformationFull instance. <p>
     *
     * @param db the database for which to produce system tables.
     * @throws HsqlException if a database access error occurs.
     */
    DatabaseInformationFull(Database db) throws HsqlException {

        super(db);

        pi = new DIProcedureInfo(ns);
    }

    /**
     * Retrieves the system table corresponding to the specified index. <p>
     *
     * @param tableIndex index identifying the system table to generate
     * @throws HsqlException if a database access error occurs
     * @return the system table corresponding to the specified index
     */
    protected Table generateTable(int tableIndex) throws HsqlException {

        switch (tableIndex) {

            case SYSTEM_PROCEDURECOLUMNS :
                return SYSTEM_PROCEDURECOLUMNS();

            case SYSTEM_PROCEDURES :
                return SYSTEM_PROCEDURES();

            case SYSTEM_SUPERTABLES :
                return SYSTEM_SUPERTABLES();

            case SYSTEM_SUPERTYPES :
                return SYSTEM_SUPERTYPES();

            case SYSTEM_UDTATTRIBUTES :
                return SYSTEM_UDTATTRIBUTES();

            case SYSTEM_UDTS :
                return SYSTEM_UDTS();

            case SYSTEM_VERSIONCOLUMNS :
                return SYSTEM_VERSIONCOLUMNS();

            // HSQLDB-specific
            case SYSTEM_ALIASES :
                return SYSTEM_ALIASES();

            case SYSTEM_CACHEINFO :
                return SYSTEM_CACHEINFO();

            case SYSTEM_CLASSPRIVILEGES :
                return SYSTEM_CLASSPRIVILEGES();

            case SYSTEM_SESSIONINFO :
                return SYSTEM_SESSIONINFO();

            case SYSTEM_PROPERTIES :
                return SYSTEM_PROPERTIES();

            case SYSTEM_SESSIONS :
                return SYSTEM_SESSIONS();

            case SYSTEM_TRIGGERCOLUMNS :
                return SYSTEM_TRIGGERCOLUMNS();

            case SYSTEM_TRIGGERS :
                return SYSTEM_TRIGGERS();

            case SYSTEM_VIEWS :
                return SYSTEM_VIEWS();

            case SYSTEM_TEXTTABLES :
                return SYSTEM_TEXTTABLES();

            case SYSTEM_USAGE_PRIVILEGES :
                return SYSTEM_USAGE_PRIVILEGES();

            case SYSTEM_CHECK_COLUMN_USAGE :
                return SYSTEM_CHECK_COLUMN_USAGE();

            case SYSTEM_CHECK_ROUTINE_USAGE :
                return SYSTEM_CHECK_ROUTINE_USAGE();

            case SYSTEM_CHECK_TABLE_USAGE :
                return SYSTEM_CHECK_TABLE_USAGE();

            case SYSTEM_TABLE_CONSTRAINTS :
                return SYSTEM_TABLE_CONSTRAINTS();

            case SYSTEM_VIEW_TABLE_USAGE :
                return SYSTEM_VIEW_TABLE_USAGE();

            case SYSTEM_VIEW_COLUMN_USAGE :
                return SYSTEM_VIEW_COLUMN_USAGE();

            case SYSTEM_VIEW_ROUTINE_USAGE :
                return SYSTEM_VIEW_ROUTINE_USAGE();

            default :
                return super.generateTable(tableIndex);
        }
    }

    /**
     * Retrieves a <code>Table</code> object describing the aliases defined
     * within this database. <p>
     *
     * Currently two types of alias are reported: DOMAIN alaises (alternate
     * names for column data types when issuing "CREATE TABLE" DDL) and
     * ROUTINE aliases (alternate names that can be used when invoking
     * routines as SQL functions or stored procedures). <p>
     *
     * Each row is an alias description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * OBJECT_TYPE  VARCHAR   type of the aliased object
     * OBJECT_CAT   VARCHAR   catalog of the aliased object
     * OBJECT_SCHEM VARCHAR   schema of the aliased object
     * OBJECT_NAME  VARCHAR   simple identifier of the aliased object
     * ALIAS_CAT    VARCHAR   catalog in which alias is defined
     * ALIAS_SCHEM  VARCHAR   schema in which alias is defined
     * ALIAS        VARCHAR   alias for the indicated object
     * </pre> <p>
     *
     * <b>Note:</b> Up to and including HSQLDB 1.7.2, user-defined aliases
     * are supported only for SQL function and stored procedure calls
     * (indicated by the value "ROUTINE" in the OBJECT_TYPE
     * column), and there is no syntax for dropping aliases, only for
     * creating them. <p>
     * @return a Table object describing the accessisble
     *      aliases in the context of the calling session
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_ALIASES() throws HsqlException {

        Table t = sysTables[SYSTEM_ALIASES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_ALIASES]);

            addColumn(t, "OBJECT_TYPE", Types.VARCHAR, 32, false);    // not null
            addColumn(t, "OBJECT_CAT", Types.VARCHAR);
            addColumn(t, "OBJECT_SCHEM", Types.VARCHAR);
            addColumn(t, "OBJECT_NAME", Types.VARCHAR, false);        // not null
            addColumn(t, "ALIAS_CAT", Types.VARCHAR);
            addColumn(t, "ALIAS_SCHEM", Types.VARCHAR);
            addColumn(t, "ALIAS", Types.VARCHAR, false);              // not null

            // order: OBJECT_TYPE, OBJECT_NAME, ALIAS.
            // true PK.
            t.createPrimaryKey(null, new int[] {
                0, 3, 6
            }, true);

            return t;
        }

        // Holders for calculated column values
        String cat;
        String schem;
        String alias;
        String objName;
        String objType;

        // Intermediate holders
        String   className;
        HashMap  hAliases;
        Iterator aliases;
        Object[] row;
        int      pos;

        // Column number mappings
        final int ialias_object_type  = 0;
        final int ialias_object_cat   = 1;
        final int ialias_object_schem = 2;
        final int ialias_object_name  = 3;
        final int ialias_cat          = 4;
        final int ialias_schem        = 5;
        final int ialias              = 6;

        // Initialization
        hAliases = database.getAliasMap();
        aliases  = hAliases.keySet().iterator();
        objType  = "ROUTINE";

        // Do it.
        while (aliases.hasNext()) {
            row     = t.getNewRow();
            alias   = (String) aliases.next();
            objName = (String) hAliases.get(alias);

            // must have class grant to see method call aliases
            pos = objName.lastIndexOf('.');

            if (pos <= 0) {

                // should never occur in practice, as this is typically a Java
                // method name, but there's nothing preventing a user from
                // creating an alias entry that is not in method FQN form;
                // such entries are not illegal, only useless.  Probably,
                // we should eventually try to disallow them.
                continue;
            }

            className = objName.substring(0, pos);

            if (!session.isAccessible(className)) {
                continue;
            }

            cat                      = ns.getCatalogName(objName);
            schem                    = ns.getSchemaName(className);
            row[ialias_object_type]  = objType;
            row[ialias_object_cat]   = cat;
            row[ialias_object_schem] = schem;
            row[ialias_object_name]  = objName;
            row[ialias_cat]          = cat;
            row[ialias_schem]        = schem;
            row[ialias]              = alias;

            t.insert(row);
        }

        // must have create/alter table rights to see domain aliases
        if (session.isAdmin()) {
            Iterator typeAliases = Types.typeAliases.keySet().iterator();

            objType = "DOMAIN";

            while (typeAliases.hasNext()) {
                row   = t.getNewRow();
                alias = (String) typeAliases.next();

                int tn = Types.typeAliases.get(alias, Integer.MIN_VALUE);

                objName = Types.getTypeString(tn);

                if (alias.equals(objName)) {
                    continue;
                }

                cat                      = ns.getCatalogName(objName);
                schem                    = ns.getSchemaName(objName);
                row[ialias_object_type]  = objType;
                row[ialias_object_cat]   = cat;
                row[ialias_object_schem] = schem;
                row[ialias_object_name]  = objName;
                row[ialias_cat]          = cat;
                row[ialias_schem]        = schem;
                row[ialias]              = alias;

                t.insert(row);
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

// boucherb@users - 20020305 - added cache_hash column to cover the case
// of multiple text tables sharing single text file.  This is possible
// without file corruption under read-only text tables, but really should be
// disallowed under all other cases, else corruption is sure to result.

    /**
     * Retrieves a <code>Table</code> object describing the current
     * state of all row caching objects for the accessible
     * tables defined within this database. <p>
     *
     * Currently, the row caching objects for which state is reported are: <p>
     *
     * <OL>
     * <LI> the system-wide <code>Cache</code> object used by CACHED tables.
     * <LI> any <code>TextCache</code> objects in use by [TEMP] TEXT tables.
     * </OL> <p>
     *
     * Each row is a cache object state description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * CACHE_CLASS         VARCHAR   FQN of Cache class
     * CACHE_HASH          INTEGER   in-memory hashCode() value of Cache object
     * CACHE_FILE          VARCHAR   absolute path of cache data file
     * CACHE_LENGTH        INTEGER   number of data bytes currently cached
     * CACHE_SIZE          INTEGER   number of rows currently cached
     * FREE_BYTES          INTEGER   total bytes in available allocation units
     * SMALLEST_FREE_ITEM  INTEGER   bytes of smallest available allocation unit
     * LARGEST_FREE_ITEM   INTEGER   bytes of largest available allocation unit
     * FREE_COUNT          INTEGER   total # of allocation units available
     * FREE_POS            INTEGER   largest file position allocated + 1
     * MAX_CACHE_SIZE      INTEGER   maximum allowable cached Row objects
     * MAX_CACHE_BYTE_SIZE INTEGER   maximum allowable size of cached Row objects
     * WRITER_LENGTH       INTEGER   length of row write buffer array
     * </pre> <p>
     *
     * <b>Notes:</b> <p>
     *
     * <code>TextCache</code> objects do not maintain a free list because
     * deleted rows are only marked deleted and never reused. As such, the
     * columns FREE_BYTES, SMALLEST_FREE_ITEM, LARGEST_FREE_ITEM, and
     * FREE_COUNT are always reported as zero for rows reporting on
     * <code>TextCache</code> objects. <p>
     *
     * Currently, CACHE_SIZE, FREE_BYTES, SMALLEST_FREE_ITEM, LARGEST_FREE_ITEM,
     * FREE_COUNT and FREE_POS are the only dynamically changing values.
     * All others are constant for the life of a cache object. In a future
     * release, other column values may also change over the life of a cache
     * object, as SQL syntax may eventually be introduced to allow runtime
     * modification of certain cache properties. <p>
     *
     * @return a description of the current state of all row caching
     *      objects associated with the accessible tables of the database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_CACHEINFO() throws HsqlException {

        Table t = sysTables[SYSTEM_CACHEINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_CACHEINFO]);

            addColumn(t, "CACHE_CLASS", Types.VARCHAR, false);           // not null
            addColumn(t, "CACHE_HASH", Types.INTEGER, false);            // not null
            addColumn(t, "CACHE_FILE", Types.VARCHAR, false);            // not null
            addColumn(t, "CACHE_LENGTH", Types.INTEGER, false);          // not null
            addColumn(t, "CACHE_SIZE", Types.INTEGER, false);            // not null
            addColumn(t, "FREE_BYTES", Types.INTEGER, false);            // not null
            addColumn(t, "SMALLEST_FREE_ITEM", Types.INTEGER, false);    // not null
            addColumn(t, "LARGEST_FREE_ITEM", Types.INTEGER, false);     // not null
            addColumn(t, "FREE_COUNT", Types.INTEGER, false);            // not null
            addColumn(t, "FREE_POS", Types.INTEGER, false);              // not null
            addColumn(t, "MAX_CACHE_SIZE", Types.INTEGER, false);        // not null
            addColumn(t, "MAX_CACHE_BYTE_SIZE", Types.BIGINT, false);    // not null
            addColumn(t, "WRITER_LENGTH", Types.INTEGER, false);         // not null

            // order: CACHE_CLASS, CACHE_FILE
            // added for unique: CACHE_HASH
            // true PK
            t.createPrimaryKey(null, new int[] {
                0, 2, 1
            }, true);

            return t;
        }

        DataFileCache cache;
        Object[]      row;
        HashSet       cacheSet;
        Iterator      caches;
        Iterator      tables;
        Table         table;
        CacheFree     cacheFree;
        int           iFreeBytes;
        int           iLargestFreeItem;
        long          lSmallestFreeItem;

        // column number mappings
        final int icache_class     = 0;
        final int icache_hash      = 1;
        final int icache_file      = 2;
        final int icache_length    = 3;
        final int icache_size      = 4;
        final int ifree_bytes      = 5;
        final int is_free_item     = 6;
        final int il_free_item     = 7;
        final int ifree_count      = 8;
        final int ifree_pos        = 9;
        final int imax_cache_sz    = 10;
        final int imax_cache_bytes = 11;
        final int iwriter_length   = 12;

        // Initialization
        cacheSet = new HashSet();

        // dynamic system tables are never cached
        tables = database.getTables().iterator();

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isCached() && isAccessibleTable(table)) {
                cache = table.cache;

                if (cache != null) {
                    cacheSet.add(cache);
                }
            }
        }

        caches = cacheSet.iterator();

        // Do it.
        while (caches.hasNext()) {
            cache             = (DataFileCache) caches.next();
            row               = t.getNewRow();
            cacheFree         = new CacheFree();
            iFreeBytes        = 0;
            iLargestFreeItem  = 0;
            lSmallestFreeItem = Long.MAX_VALUE;
            cacheFree.fNext   = cache.fRoot;

            while (cacheFree.fNext != null) {
                cacheFree  = cacheFree.fNext;
                iFreeBytes += cacheFree.iLength;

                if (cacheFree.iLength > iLargestFreeItem) {
                    iLargestFreeItem = cacheFree.iLength;
                }

                if (cacheFree.iLength < lSmallestFreeItem) {
                    lSmallestFreeItem = cacheFree.iLength;
                }
            }

            if (lSmallestFreeItem > Integer.MAX_VALUE) {
                lSmallestFreeItem = 0;
            }

            row[icache_class]     = cache.getClass().getName();
            row[icache_hash]      = ValuePool.getInt(cache.hashCode());
            row[icache_file] = FileUtil.canonicalOrAbsolutePath(cache.sName);
            row[icache_length]    = ValuePool.getLong(cache.cacheBytesLength);
            row[icache_size]      = ValuePool.getInt(cache.getCachedCount());
            row[ifree_bytes]      = ValuePool.getInt(iFreeBytes);
            row[is_free_item]     = ValuePool.getInt((int) lSmallestFreeItem);
            row[il_free_item]     = ValuePool.getInt(iLargestFreeItem);
            row[ifree_count]      = ValuePool.getInt(cache.iFreeCount);
            row[ifree_pos]        = ValuePool.getInt(cache.fileFreePosition);
            row[imax_cache_sz]    = ValuePool.getInt(cache.maxCacheSize);
            row[imax_cache_bytes] = ValuePool.getLong(cache.maxCacheBytes);
            row[iwriter_length] = ValuePool.getInt(
                cache.rowOut.getOutputStream().getBuffer().length);

            t.insert(row);
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the visible
     * access rights for all accessible Java Class objects defined
     * within this database.<p>
     *
     * Each row is a Class privilege description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * CLASS_CAT    VARCHAR   catalog in which the class is defined
     * CLASS_SCHEM  VARCHAR   schema in which the class is defined
     * CLASS_NAME   VARCHAR   fully qualified name of class
     * GRANTOR      VARCHAR   grantor of access
     * GRANTEE      VARCHAR   grantee of access
     * PRIVILEGE    VARCHAR   name of access: {"EXECUTE" | "TRIGGER"}
     * IS_GRANTABLE VARCHAR   grantable?: {"YES" | "NO" | NULL (unknown)}
     * </pre>
     *
     * <b>Note:</b> Users with the administrative privilege implicily have
     * full and unrestricted access to all Classes available to the database
     * class loader.  However, only explicitly granted rights are reported
     * in this table.  Explicit Class grants/revokes to admin users have no
     * effect in reality, but are reported in this table anyway for
     * completeness. <p>
     *
     * @return a <code>Table</code> object describing the visible
     *        access rights for all accessible Java Class
     *        objects defined within this database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_CLASSPRIVILEGES() throws HsqlException {

        Table t = sysTables[SYSTEM_CLASSPRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_CLASSPRIVILEGES]);

            addColumn(t, "CLASS_CAT", Types.VARCHAR);
            addColumn(t, "CLASS_SCHEM", Types.VARCHAR);
            addColumn(t, "CLASS_NAME", Types.VARCHAR, false);         // not null
            addColumn(t, "GRANTOR", Types.VARCHAR, false);            // not null
            addColumn(t, "GRANTEE", Types.VARCHAR, false);            // not null
            addColumn(t, "PRIVILEGE", Types.VARCHAR, 7, false);       // not null
            addColumn(t, "IS_GRANTABLE", Types.VARCHAR, 3, false);    // not null
            t.createPrimaryKey(null, new int[] {
                2, 4, 5
            }, true);

            return t;
        }

        // calculated column values
        String clsCat;
        String clsSchem;
        String clsName;
        String grantorName;
        String granteeName;
        String privilege;
        String isGrantable;

        // intermediate holders
        UserManager   um;
        HsqlArrayList users;
        HashSet       classNameSet;
        Iterator      classNames;
        User          granteeUser;
        Object[]      row;

        // column number mappings
        final int icls_cat   = 0;
        final int icls_schem = 1;
        final int icls_name  = 2;
        final int igrantor   = 3;
        final int igrantee   = 4;
        final int iprivilege = 5;
        final int iis_grntbl = 6;

        // Initialization
        grantorName = UserManager.SYS_USER_NAME;
        um          = database.getUserManager();
        users       = um.listVisibleUsers(session, true);

        // Do it.
        for (int i = 0; i < users.size(); i++) {
            granteeUser  = (User) users.get(i);
            granteeName  = granteeUser.getName();
            isGrantable  = granteeUser.isAdmin() ? "YES"
                                                 : "NO";
            classNameSet = granteeUser.getGrantedClassNames(false);

            if (granteeUser.isPublic()) {
                ns.addBuiltinToSet(classNameSet);
            }

            classNames = classNameSet.iterator();

// boucherb@users 20030305 - TODO completed.
// "EXECUTE" is closest to correct (from: SQL 200n ROUTINE_PRIVILEGES)
// There is nothing even like CLASS_PRIVILEGES table under SQL 200n spec.
            privilege = "EXECUTE";

            while (classNames.hasNext()) {
                clsName         = (String) classNames.next();
                clsCat          = ns.getCatalogName(clsName);
                clsSchem        = ns.getSchemaName(clsName);
                row             = t.getNewRow();
                row[icls_cat]   = clsCat;
                row[icls_schem] = clsSchem;
                row[icls_name]  = clsName;
                row[igrantor]   = grantorName;
                row[igrantee]   = granteeName;
                row[iprivilege] = privilege;
                row[iis_grntbl] = isGrantable;

                t.insert(row);
            }

            classNames = ns.iterateAccessibleTriggerClassNames(granteeUser);

// boucherb@users 20030305 - TODO completed.
// "TRIGGER" is closest to correct. (from: SQL 200n TABLE_PRIVILEGES)
// There is nothing even like CLASS_PRIVILEGES table under SQL 200n spec.
            privilege = "TRIGGER";

            while (classNames.hasNext()) {
                clsName         = (String) classNames.next();
                clsCat          = ns.getCatalogName(clsName);
                clsSchem        = ns.getSchemaName(clsName);
                row             = t.getNewRow();
                row[icls_cat]   = clsCat;
                row[icls_schem] = clsSchem;
                row[icls_name]  = clsName;
                row[igrantor]   = grantorName;
                row[igrantee]   = granteeName;
                row[iprivilege] = privilege;
                row[iis_grntbl] = isGrantable;

                t.insert(row);
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing attributes
     * for the calling session context.<p>
     *
     * The rows report the following {key,value} pairs:<p>
     *
     * <pre class="SqlCodeExample">
     * KEY (VARCHAR)       VALUE (VARCHAR)
     * ------------------- ---------------
     * SESSION_ID          the id of the calling session
     * AUTOCOMMIT          YES: session is in autocommit mode, else NO
     * USER                the name of user connected in the calling session
     * (was READ_ONLY)
     * SESSION_READONLY    TRUE: session is in read-only mode, else FALSE
     * (new)
     * DATABASE_READONLY   TRUE: database is in read-only mode, else FALSE
     * MAXROWS             the MAXROWS setting in the calling session
     * DATABASE            the name of the database
     * IDENTITY            the last identity value used by calling session
     * </pre>
     *
     * <b>Note:</b>  This table <em>may</em> become deprecated in a future
     * release, as the information it reports now duplicates information
     * reported in the newer SYSTEM_SESSIONS and SYSTEM_PROPERTIES
     * tables. <p>
     *
     * @return a <code>Table</code> object describing the
     *        attributes of the connection associated
     *        with the current execution context
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_SESSIONINFO() throws HsqlException {

        Table t = sysTables[SYSTEM_SESSIONINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SESSIONINFO]);

            addColumn(t, "KEY", Types.VARCHAR, false);      // not null
            addColumn(t, "VALUE", Types.VARCHAR, false);    // not null
            t.createPrimaryKey(null);

            return t;
        }

        Object[] row;

        row    = t.getNewRow();
        row[0] = "SESSION_ID";
        row[1] = String.valueOf(session.getId());

        t.insert(row);

        row    = t.getNewRow();
        row[0] = "AUTOCOMMIT";
        row[1] = session.isAutoCommit() ? "TRUE"
                                        : "FALSE";

        t.insert(row);

        row    = t.getNewRow();
        row[0] = "USER";
        row[1] = session.getUsername();

        t.insert(row);

        row    = t.getNewRow();
        row[0] = "SESSION_READONLY";
        row[1] = session.isReadOnly() ? "TRUE"
                                      : "FALSE";

        t.insert(row);

        row    = t.getNewRow();
        row[0] = "DATABASE_READONLY";
        row[1] = database.databaseReadOnly ? "TRUE"
                                           : "FALSE";

        t.insert(row);

        // fredt - value set by SET MAXROWS in SQL, not Statement.setMaxRows()
        row    = t.getNewRow();
        row[0] = "MAXROWS";
        row[1] = String.valueOf(session.getSQLMaxRows());

        t.insert(row);

        row    = t.getNewRow();
        row[0] = "DATABASE";
        row[1] = database.getURI();

        t.insert(row);

        row    = t.getNewRow();
        row[0] = "IDENTITY";
        row[1] = String.valueOf(session.getLastIdentity());

        t.insert(row);
        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the capabilities
     * and operating parameter properties for the engine hosting this
     * database, as well as their applicability in terms of scope and
     * name space. <p>
     *
     * Reported properties include certain predefined <code>Database</code>
     * properties file values as well as certain database scope
     * attributes. <p>
     *
     * It is intended that all <code>Database</code> attributes and
     * properties that can be set via the database properties file,
     * JDBC connection properties or SQL SET/ALTER statements will
     * eventually be reported here or, where more applicable, in an
     * ANSI/ISO conforming feature info base table in the defintion
     * schema. <p>
     *
     * Currently, the database properties reported are: <p>
     *
     * <OL>
     *     <LI>hsqldb.cache_file_scale - the scaling factor used to translate data and index structure file pointers
     *     <LI>hsqldb.cache_scale - base-2 exponent scaling allowable cache row count
     *     <LI>hsqldb.cache_size_scale - base-2 exponent scaling allowable cache byte count
     *     <LI>hsqldb.cache_version -
     *     <LI>hsqldb.catalogs - whether to report the database catalog (database uri)
     *     <LI>hsqldb.compatible_version -
     *     <LI>hsqldb.files_readonly - whether the database is in files_readonly mode
     *     <LI>hsqldb.first_identity - the default first identity value
     *     <LI>hsqldb.gc_interval - # new records forcing gc ({0|NULL}=>never)
     *     <LI>hsqldb.max_nio_scale - scale factor for cache nio mapped buffers
     *     <LI>hsqldb.nio_data_file - whether cache uses nio mapped buffers
     *     <LI>hsqldb.original_version -
     *     <LI>hsqldb.schemas - whether to report synthetic schema values for compatibility
     *     <LI>sql.compare_in_locale - is JVM Locale used in collations?
     *     <LI>sql.enforce_size - column length specifications enforced (truncate/pad)?
     *     <LI>sql.enforce_strict_size - column length specifications enforced strictly (raise exception on overflow)?
     *     <LI>textdb.all_quoted - default policy regarding whether to quote all character field values
     *     <LI>textdb.cache_scale - base-2 exponent scaling allowable cache row count
     *     <LI>textdb.cache_size_scale - base-2 exponent scaling allowable cache byte count
     *     <LI>textdb.encoding - default TEXT table file encoding
     *     <LI>textdb.fs - default field separator
     *     <LI>textdb.vs - default varchar field separator
     *     <LI>textdb.lvs - default long varchar field separator
     *     <LI>textdb.ignore_first - default policy regarding whether to ignore the first line
     *     <LI>textdb.quoted - default policy regarding treatement character field values that _may_ require quoting
     *     <LI>IGNORECASE - currently ignoring case in character comparisons?
     *     <LI>LOGSIZSE - # bytes to which REDO log grows before auto-checkpoint
     *     <LI>REFERENTIAL_INTEGITY - currently enforcing referential integrity?
     *     <LI>SCRIPTFORMAT - 0 : TEXT, 1 : BINARY, ...
     *     <LI>WRITEDELAY - does REDO log currently use buffered write strategy?
     * </OL> <p>
     *
     * @return table describing database and session operating parameters
     *      and capabilities
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_PROPERTIES() throws HsqlException {

        Table t = sysTables[SYSTEM_PROPERTIES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROPERTIES]);

            addColumn(t, "PROPERTY_SCOPE", Types.VARCHAR, false);
            addColumn(t, "PROPERTY_NAMESPACE", Types.VARCHAR, false);
            addColumn(t, "PROPERTY_NAME", Types.VARCHAR, false);
            addColumn(t, "PROPERTY_VALUE", Types.VARCHAR);
            addColumn(t, "PROPERTY_CLASS", Types.VARCHAR, false);

            // order PROPERTY_SCOPE, PROPERTY_NAMESPACE, PROPERTY_NAME
            // true PK
            t.createPrimaryKey(null, new int[] {
                0, 1, 2
            }, true);

            return t;
        }

        // calculated column values
        String scope;
        String nameSpace;
        String name;
        Object value;

        // intermediate holders
        Method[]       methods;
        Class          returnType;
        Method         method;
        Object[]       emptyParms;
        Object[]       row;
        Result         r;
        HsqlProperties props;
        Log            log;

        // column number mappings
        final int iscope = 0;
        final int ins    = 1;
        final int iname  = 2;
        final int ivalue = 3;
        final int iclass = 4;

        // First, we want the names and values for
        // all JDBC capabilities constants
        scope     = "SESSION";
        props     = database.getProperties();
        nameSpace = "database.properties";
        log       = database.logger.lLog;

        // hsqldb.catalogs
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.catalogs";
        row[ivalue] = props.getProperty("hsqldb.catalogs", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // hsqldb.schemas
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.schemas";
        row[ivalue] = props.getProperty("hsqldb.schemas", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // sql.enforce_size
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "sql.enforce_size";
        row[ivalue] = props.getProperty("sql.enforce_size", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // sql.enforce_strict_size
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "sql.enforce_strict_size";
        row[ivalue] = props.getProperty("sql.enforce_strict_size", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // sql.compare_in_locale
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "sql.compare_in_locale";
        row[ivalue] = props.getProperty("sql.compare_in_locale", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // hsqldb.files_readonly
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.files_readonly";
        row[ivalue] = props.getProperty("hsqldb.files_readonly", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // hsqldb.first_identity
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.first_identity";
        row[ivalue] = props.getProperty("hsqldb.first_identity", "0");
        row[iclass] = "int";

        t.insert(row);

        // hsqldb.compatible_version
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.compatible_version";
        row[ivalue] = props.getProperty("hsqldb.compatible_version");
        row[iclass] = "java.lang.String";

        t.insert(row);

        // hsqldb.cache_version
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.cache_version";
        row[ivalue] = props.getProperty("hsqldb.cache_version");
        row[iclass] = "java.lang.String";

        t.insert(row);

        // hsqldb.original_version
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.original_version";
        row[ivalue] = props.getProperty("hsqldb.original_version");
        row[iclass] = "java.lang.String";

        t.insert(row);

        // hsqldb.cache_scale
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.cache_scale";
        row[ivalue] = props.getProperty("hsqldb.cache_scale");
        row[iclass] = "int";

        t.insert(row);

        // hsqldb.cache_file_scale
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.cache_file_scale";
        row[ivalue] = props.getProperty("hsqldb.cache_file_scale");
        row[iclass] = "int";

        t.insert(row);

        // hsqldb.cache_size_scale
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.cache_size_scale";
        row[ivalue] = props.getProperty("hsqldb.cache_size_scale");
        row[iclass] = "int";

        t.insert(row);

        // hsqldb.max_nio_scale
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.max_nio_scale";
        row[ivalue] = props.getProperty("hsqldb.max_nio_scale");
        row[iclass] = "int";

        t.insert(row);

        // hsqldb.nio_data_file
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.nio_data_file";
        row[ivalue] = props.getProperty("hsqldb.nio_data_file");
        row[iclass] = "boolean";

        t.insert(row);

        // textdb.all_quoted
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.all_quoted";
        row[ivalue] = props.getProperty("textdb.all_quoted", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // textdb.cache_scale
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.cache_scale";
        row[ivalue] = props.getProperty("textdb.cache_scale", "10");
        row[iclass] = "int";

        t.insert(row);

        // textdb.cache_size_scale
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.cache_size_scale";
        row[ivalue] = props.getProperty("textdb.cache_size_scale", "12");
        row[iclass] = "int";

        t.insert(row);

        // textdb.ignore_first
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.ignore_first";
        row[ivalue] = props.getProperty("textdb.ignore_first", "false");
        row[iclass] = "boolean";

        t.insert(row);

        // textdb.quoted
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.quoted";
        row[ivalue] = props.getProperty("textdb.quoted", "true");
        row[iclass] = "boolean";

        t.insert(row);

        // textdb.fs
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.fs";
        row[ivalue] = props.getProperty("textdb.fs", ",");
        row[iclass] = "java.lang.String";

        // textdb.vs
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.vs";
        row[ivalue] = props.getProperty("textdb.vs", ",");
        row[iclass] = "java.lang.String";

        t.insert(row);

        // textdb.lvs
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.lvs";
        row[ivalue] = props.getProperty("textdb.lvs", ",");
        row[iclass] = "java.lang.String";

        t.insert(row);

        // textdb.encoding
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "textdb.encoding";
        row[ivalue] = props.getProperty("textdb.encoding", "ASCII");
        row[iclass] = "java.lang.String";

        t.insert(row);

        // hsqldb.gc_interval
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "hsqldb.gc_interval";
        row[ivalue] = props.getProperty("hsqldb.gc_interval", "0");
        row[iclass] = "int";

        t.insert(row);

        // Now get a snapshot of the properties that may change over
        // the lifetime of the session
        scope     = "TRANSACTION";
        nameSpace = "org.hsqldb.Database";

        // log size
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "LOGSIZE";
        row[ivalue] = props.getProperty("hsqldb.log_size", "200");
        row[iclass] = "int";

        t.insert(row);

        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "SCRIPTFORMAT";

        try {
            row[ivalue] =
                ScriptWriterBase.LIST_SCRIPT_FORMATS[log.scriptFormat];
        } catch (Exception e) {}

        row[iclass] = "java.lang.String";

        t.insert(row);

        // write delay
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "WRITE_DELAY";
        row[ivalue] = (log == null) ? "0"
                                    : "" + log.writeDelay;
        row[iclass] = "int";

        t.insert(row);

        // ignore case
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "IGNORECASE";
        row[ivalue] = database.isIgnoreCase() ? "true"
                                              : "false";
        row[iclass] = "boolean";

        t.insert(row);

        // referential integrity
        row         = t.getNewRow();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "REFERENTIAL_INTEGRITY";
        row[ivalue] = database.isReferentialIntegrity() ? "true"
                                                        : "false";
        row[iclass] = "boolean";

        t.insert(row);
        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing all visible
     * sessions. ADMIN users see *all* sessions, including the SYS session,
     * while non-admin users see only their own session.<p>
     *
     * Each row is a session state description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * SESSION_ID         INTEGER   session identifier
     * CONNECTED          TIMESTAMP time at which session was created
     * USER_NAME          VARCHAR   db user name of current session user
     * IS_ADMIN           BOOLEAN   is session user an admin user?
     * AUTOCOMMIT         BOOLEAN   is session in autocommit mode?
     * READONLY           BOOLEAN   is session in read-only mode?
     * MAXROWS            INTEGER   session's MAXROWS setting
     * LAST_IDENTITY      INTEGER   last identity value used by this session
     * TRANSACTION_SIZE   INTEGER   # of undo items in current transaction
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing all visible
     *      sessions
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_SESSIONS() throws HsqlException {

        Table t = sysTables[SYSTEM_SESSIONS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SESSIONS]);

            addColumn(t, "SESSION_ID", Types.INTEGER, false);
            addColumn(t, "CONNECTED", Types.TIMESTAMP, false);
            addColumn(t, "USER_NAME", Types.VARCHAR, false);
            addColumn(t, "IS_ADMIN", Types.BOOLEAN, false);
            addColumn(t, "AUTOCOMMIT", Types.BOOLEAN, false);
            addColumn(t, "READONLY", Types.BOOLEAN, false);
            addColumn(t, "MAXROWS", Types.INTEGER, false);

            // Note: some sessions may have a NULL LAST_IDENTITY value
            addColumn(t, "LAST_IDENTITY", Types.BIGINT);
            addColumn(t, "TRANSACTION_SIZE", Types.INTEGER, false);

            // order:  SESSION_ID
            // true primary key
            t.createPrimaryKey(null, new int[]{ 0 }, true);

            return t;
        }

        // intermediate holders
        Session[] sessions;
        Session   s;
        Object[]  row;

        // column number mappings
        final int isid      = 0;
        final int ict       = 1;
        final int iuname    = 2;
        final int iis_admin = 3;
        final int iautocmt  = 4;
        final int ireadonly = 5;
        final int imaxrows  = 6;
        final int ilast_id  = 7;
        final int it_size   = 8;

        // Initialisation
        sessions = ns.listVisibleSessions(session);

        // Do it.
        for (int i = 0; i < sessions.length; i++) {
            s              = sessions[i];
            row            = t.getNewRow();
            row[isid]      = ValuePool.getInt(s.getId());
            row[ict]       = new Timestamp(s.getConnectTime());
            row[iuname]    = s.getUsername();
            row[iis_admin] = ValuePool.getBoolean(s.isAdmin());
            row[iautocmt]  = ValuePool.getBoolean(s.isAutoCommit());
            row[ireadonly] = ValuePool.getBoolean(s.isReadOnly());
            row[imaxrows]  = ValuePool.getInt(s.getSQLMaxRows());
            row[ilast_id] =
                ValuePool.getLong(s.getLastIdentity().longValue());
            row[it_size] = ValuePool.getInt(s.getTransactionSize());

            t.insert(row);
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * direct super table (if any) of each accessible table defined
     * within this database. <p>
     *
     * Each row is a super table description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT       VARCHAR   the table's catalog
     * TABLE_SCHEM     VARCHAR   table schema
     * TABLE_NAME      VARCHAR   table name
     * SUPERTABLE_NAME VARCHAR   the direct super table's name
     * </pre> <p>
     * @return a <code>Table</code> object describing the accessible
     *        direct supertable (if any) of each accessible
     *        table defined within this database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_SUPERTABLES() throws HsqlException {

        Table t = sysTables[SYSTEM_SUPERTABLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SUPERTABLES]);

            addColumn(t, "TABLE_CAT", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEM", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, false);         // not null
            addColumn(t, "SUPERTABLE_NAME", Types.VARCHAR, false);    // not null
            t.createPrimaryKey(null);

            return t;
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * direct super type (if any) of each accessible user-defined type (UDT)
     * defined within this database. <p>
     *
     * Each row is a super type description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TYPE_CAT        VARCHAR   the UDT's catalog
     * TYPE_SCHEM      VARCHAR   UDT's schema
     * TYPE_NAME       VARCHAR   type name of the UDT
     * SUPERTYPE_CAT   VARCHAR   the direct super type's catalog
     * SUPERTYPE_SCHEM VARCHAR   the direct super type's schema
     * SUPERTYPE_NAME  VARCHAR   the direct super type's name
     * </pre> <p>
     * @return a <code>Table</code> object describing the accessible
     *        direct supertype (if any) of each accessible
     *        user-defined type (UDT) defined within this database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_SUPERTYPES() throws HsqlException {

        Table t = sysTables[SYSTEM_SUPERTYPES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SUPERTYPES]);

            addColumn(t, "TYPE_CAT", Types.VARCHAR);
            addColumn(t, "TYPE_SCHEM", Types.VARCHAR);
            addColumn(t, "TYPE_NAME", Types.VARCHAR, false);         // not null
            addColumn(t, "SUPERTYPE_CAT", Types.VARCHAR);
            addColumn(t, "SUPERTYPE_SCHEM", Types.VARCHAR);
            addColumn(t, "SUPERTYPE_NAME", Types.VARCHAR, false);    // not null
            t.createPrimaryKey(null);

            return t;
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the TEXT TABLE objects
     * defined within this database. The table contains one row for each row
     * in the SYSTEM_TABLES table with a HSQLDB_TYPE of  TEXT . <p>
     *
     * Each row is a description of the attributes that defines its TEXT TABLE,
     * with the following columns:
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT                 VARCHAR   table's catalog name
     * TABLE_SCHEM               VARCHAR   table's simple schema name
     * TABLE_NAME                VARCHAR   table's simple name
     * DATA_SOURCE_DEFINITION    VARCHAR   the "spec" proption of the table's
     *                                     SET TABLE ... SOURCE DDL declaration
     * FILE_PATH                 VARCHAR   absolute file path.
     * FILE_ENCODING             VARCHAR   endcoding of table's text file
     * FIELD_SEPARATOR           VARCHAR   default field separator
     * VARCHAR_SEPARATOR         VARCAHR   varchar field separator
     * LONGVARCHAR_SEPARATOR     VARCHAR   longvarchar field separator
     * IS_IGNORE_FIRST           BOOLEAN   ignores first line of file?
     * IS_QUOTED                 BOOLEAN   fields are quoted if necessary?
     * IS_ALL_QUOTED             BOOLEAN   all fields are quoted?
     * IS_DESC                   BOOLEAN   read rows starting at end of file?
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the text attributes
     * of the accessible text tables defined within this database
     * @throws HsqlException if an error occurs while producing the table
     *
     */
    final Table SYSTEM_TEXTTABLES() throws HsqlException {

        Table t = sysTables[SYSTEM_TEXTTABLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TEXTTABLES]);

            addColumn(t, "TABLE_CAT", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEM", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, false);    // not null
            addColumn(t, "DATA_SOURCE_DEFINTION", Types.VARCHAR);
            addColumn(t, "FILE_PATH", Types.VARCHAR);
            addColumn(t, "FILE_ENCODING", Types.VARCHAR);
            addColumn(t, "FIELD_SEPARATOR", Types.VARCHAR);
            addColumn(t, "VARCHAR_SEPARATOR", Types.VARCHAR);
            addColumn(t, "LONGVARCHAR_SEPARATOR", Types.VARCHAR);
            addColumn(t, "IS_IGNORE_FIRST", Types.BOOLEAN);
            addColumn(t, "IS_ALL_QUOTED", Types.BOOLEAN);
            addColumn(t, "IS_QUOTED", Types.BOOLEAN);
            addColumn(t, "IS_DESC", Types.BOOLEAN);

            // ------------------------------------------------------------
            t.createPrimaryKey();

            return t;
        }

        // intermediate holders
        Iterator tables;
        Table    table;
        Object   row[];

//        DITableInfo ti;
        TextCache tc;

        // column number mappings
        final int itable_cat   = 0;
        final int itable_schem = 1;
        final int itable_name  = 2;
        final int idsd         = 3;
        final int ifile_path   = 4;
        final int ifile_enc    = 5;
        final int ifs          = 6;
        final int ivfs         = 7;
        final int ilvfs        = 8;
        final int iif          = 9;
        final int iiq          = 10;
        final int iiaq         = 11;
        final int iid          = 12;

        // Initialization
        tables = database.getTables().iterator();

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!table.isText() ||!isAccessibleTable(table)) {
                continue;
            }

            row               = t.getNewRow();
            row[itable_cat]   = ns.getCatalogName(table);
            row[itable_schem] = ns.getSchemaName(table);
            row[itable_name]  = table.getName().name;

            if (table.cache != null && table.cache instanceof TextCache) {
                tc              = (TextCache) table.cache;
                row[idsd]       = table.getDataSource();
                row[ifile_path] = FileUtil.canonicalOrAbsolutePath(tc.sName);

// VITAZ: patched - removed for incompatibility with data providers                
//                row[ifile_enc]  = tc.stringEncoding;
//                row[ifs]        = tc.fs;
//                row[ivfs]       = tc.vs;
//                row[ilvfs]      = tc.lvs;
//                row[iif]        = ValuePool.getBoolean(tc.ignoreFirst);
//                row[iiq]        = ValuePool.getBoolean(tc.isQuoted);
//                row[iiaq]       = ValuePool.getBoolean(tc.isAllQuoted);
                row[iid] = ValuePool.getBoolean(table.isDescDataSource());
            }

            t.insert(row);
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the usage
     * of accessible columns in accessible triggers defined within
     * the database. <p>
     *
     * Each column usage description has the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TRIGGER_CAT   VARCHAR   Trigger catalog.
     * TRIGGER_SCHEM VARCHAR   Trigger schema.
     * TRIGGER_NAME  VARCHAR   Trigger name.
     * TABLE_CAT     VARCHAR   Catalog of table on which the trigger is defined.
     * TABLE_SCHEM   VARCHAR   Schema of table on which the trigger is defined.
     * TABLE_NAME    VARCHAR   Table on which the trigger is defined.
     * COLUMN_NAME   VARCHAR   Name of the column used in the trigger.
     * COLUMN_LIST   VARCHAR   Specified in UPDATE clause?: ("Y" | "N"}
     * COLUMN_USAGE  VARCHAR   {"NEW" | "OLD" | "IN" | "OUT" | "IN OUT"}
     * </pre> <p>
     * @return a <code>Table</code> object describing of the usage
     *        of accessible columns in accessible triggers
     *        defined within this database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_TRIGGERCOLUMNS() throws HsqlException {

        Table t = sysTables[SYSTEM_TRIGGERCOLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TRIGGERCOLUMNS]);

            addColumn(t, "TRIGGER_CAT", Types.VARCHAR);
            addColumn(t, "TRIGGER_SCHEM", Types.VARCHAR);
            addColumn(t, "TRIGGER_NAME", Types.VARCHAR);
            addColumn(t, "TABLE_CAT", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEM", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR);
            addColumn(t, "COLUMN_NAME", Types.VARCHAR);
            addColumn(t, "COLUMN_LIST", Types.VARCHAR);
            addColumn(t, "COLUMN_USAGE", Types.VARCHAR);

            // order:  all columns, in order, as each column
            // of each table may eventually be listed under various capacities
            // (when a more comprehensive trugger system is put in place)
            // false PK, as cat and schem may be null
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8
            }, false);

            return t;
        }

        Result rs;

        // - used appends to make class file constant pool smaller
        // - saves ~ 100 bytes jar space
        rs = session.sqlExecuteDirectNoPreChecks(
            (new StringBuffer(185)).append("select").append(' ').append(
                "a.").append("TRIGGER_CAT").append(',').append("a.").append(
                "TRIGGER_SCHEM").append(',').append("a.").append(
                "TRIGGER_NAME").append(',').append("a.").append(
                "TABLE_CAT").append(',').append("a.").append(
                "TABLE_SCHEM").append(',').append("a.").append(
                "TABLE_NAME").append(',').append("b.").append(
                "COLUMN_NAME").append(',').append("'Y'").append(',').append(
                "'IN'").append(' ').append("from").append(' ').append(
                "SYSTEM_TRIGGERS").append(" a,").append(
                "SYSTEM_COLUMNS").append(" b ").append("where").append(
                ' ').append("a.").append("TABLE_NAME").append('=').append(
                "b.").append("TABLE_NAME").toString());

        t.insert(rs);
        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * triggers defined within the database. <p>
     *
     * Each row is a trigger description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TRIGGER_CAT       VARCHAR   Trigger catalog.
     * TRIGGER_SCHEM     VARCHAR   Trigger Schema.
     * TRIGGER_NAME      VARCHAR   Trigger Name.
     * TRIGGER_TYPE      VARCHAR   {("BEFORE" | "AFTER") + [" EACH ROW"] }
     * TRIGGERING_EVENT  VARCHAR   {"INSERT" | "UPDATE" | "DELETE"}
     *                             (future?: "INSTEAD OF " + ("SELECT" | ...))
     * TABLE_CAT         VARCHAR   Table's catalog.
     * TABLE_SCHEM       VARCHAR   Table's schema.
     * BASE_OBJECT_TYPE  VARCHAR   "TABLE"
     *                             (future?: "VIEW" | "SCHEMA" | "DATABASE")
     * TABLE_NAME        VARCHAR   Table on which trigger is defined
     * COLUMN_NAME       VARCHAR   NULL (future?: nested table column name)
     * REFERENCING_NAMES VARCHAR   ROW, OLD, NEW, etc.
     * WHEN_CLAUSE       VARCHAR   Condition firing trigger (NULL => always)
     * STATUS            VARCHAR   {"ENABLED" | "DISABLED"}
     * DESCRIPTION       VARCHAR   typically, the trigger's DDL
     * ACTION_TYPE       VARCHAR   "CALL" (future?: embedded language name)
     * TRIGGER_BODY      VARCHAR   Statement(s) executed
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the accessible
     *    triggers defined within this database.
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_TRIGGERS() throws HsqlException {

        Table t = sysTables[SYSTEM_TRIGGERS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TRIGGERS]);

            addColumn(t, "TRIGGER_CAT", Types.VARCHAR);
            addColumn(t, "TRIGGER_SCHEM", Types.VARCHAR);
            addColumn(t, "TRIGGER_NAME", Types.VARCHAR, false);
            addColumn(t, "TRIGGER_TYPE", Types.VARCHAR, 15, false);
            addColumn(t, "TRIGGERING_EVENT", Types.VARCHAR, 10, false);
            addColumn(t, "TABLE_CAT", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEM", Types.VARCHAR);
            addColumn(t, "BASE_OBJECT_TYPE", Types.VARCHAR, 8, false);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, false);
            addColumn(t, "COLUMN_NAME", Types.VARCHAR);
            addColumn(t, "REFERENCING_NAMES", Types.VARCHAR, false);
            addColumn(t, "WHEN_CLAUSE", Types.VARCHAR);
            addColumn(t, "STATUS", Types.VARCHAR, 8, false);
            addColumn(t, "DESCRIPTION", Types.VARCHAR, false);
            addColumn(t, "ACTION_TYPE", Types.VARCHAR, false);
            addColumn(t, "TRIGGER_BODY", Types.VARCHAR, false);

            // order: TRIGGER_TYPE, TRIGGER_SCHEM, TRIGGER_NAME
            // added for unique: TRIGGER_CAT
            // false PK, as TRIGGER_SCHEM and/or TRIGGER_CAT may be null
            t.createPrimaryKey(null, new int[] {
                3, 1, 2, 0
            }, false);

            return t;
        }

        // calculated column values
        String triggerCatalog;
        String triggerSchema;
        String triggerName;
        String triggerType;
        String triggeringEvent;
        String tableCatalog;
        String tableSchema;
        String baseObjectType;
        String tableName;
        String columnName;
        String referencingNames;
        String whenClause;
        String status;
        String description;
        String actionType;
        String triggerBody;

        // Intermediate holders
        Iterator        tables;
        Table           table;
        HsqlArrayList[] vTrigs;
        HsqlArrayList   triggerList;
        TriggerDef      def;
        Object          row[];

        // column number mappings
        final int itrigger_cat       = 0;
        final int itrigger_schem     = 1;
        final int itrigger_name      = 2;
        final int itrigger_type      = 3;
        final int itriggering_event  = 4;
        final int itable_cat         = 5;
        final int itable_schem       = 6;
        final int ibase_object_type  = 7;
        final int itable_name        = 8;
        final int icolumn_name       = 9;
        final int ireferencing_names = 10;
        final int iwhen_clause       = 11;
        final int istatus            = 12;
        final int idescription       = 13;
        final int iaction_type       = 14;
        final int itrigger_body      = 15;

        // Initialization
        tables = database.getTables().iterator();

        // these are the only values supported, currently
        actionType       = "CALL";
        baseObjectType   = "TABLE";
        columnName       = null;
        referencingNames = "ROW";
        whenClause       = null;

        // Do it.
        while (tables.hasNext()) {
            table  = (Table) tables.next();
            vTrigs = table.triggerLists;

            // faster test first
            if (vTrigs == null) {
                continue;
            }

            if (!isAccessibleTable(table)) {
                continue;
            }

            tableCatalog   = ns.getCatalogName(table);
            triggerCatalog = tableCatalog;
            tableSchema    = ns.getSchemaName(table);
            triggerSchema  = tableSchema;
            tableName      = table.getName().name;

            for (int i = 0; i < vTrigs.length; i++) {
                triggerList = vTrigs[i];

                if (triggerList == null) {
                    continue;
                }

                for (int j = 0; j < triggerList.size(); j++) {
                    def = (TriggerDef) triggerList.get(j);

                    if (def == null) {
                        continue;
                    }

                    triggerName = def.name.name;
                    description = def.getDDL().toString();
                    status      = def.valid ? "ENABLED"
                                            : "DISABLED";
                    triggerBody = def.fire;
                    triggerType = def.when;

                    if (def.forEachRow) {
                        triggerType += " EACH ROW";
                    }

                    triggeringEvent         = def.operation;
                    row                     = t.getNewRow();
                    row[itrigger_cat]       = triggerCatalog;
                    row[itrigger_schem]     = triggerSchema;
                    row[itrigger_name]      = triggerName;
                    row[itrigger_type]      = triggerType;
                    row[itriggering_event]  = triggeringEvent;
                    row[itable_cat]         = tableCatalog;
                    row[itable_schem]       = tableSchema;
                    row[ibase_object_type]  = baseObjectType;
                    row[itable_name]        = tableName;
                    row[icolumn_name]       = columnName;
                    row[ireferencing_names] = referencingNames;
                    row[iwhen_clause]       = whenClause;
                    row[istatus]            = status;
                    row[idescription]       = description;
                    row[iaction_type]       = actionType;
                    row[itrigger_body]      = triggerBody;

                    t.insert(row);
                }
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * attributes of the accessible user-defined type (UDT) objects
     * defined within this database. <p>
     *
     * This description does not contain inherited attributes. <p>
     *
     * Each row is a user-defined type attributes description with the
     * following columns:
     *
     * <pre class="SqlCodeExample">
     * TYPE_CAT          VARCHAR   type catalog
     * TYPE_SCHEM        VARCHAR   type schema
     * TYPE_NAME         VARCHAR   type name
     * ATTR_NAME         VARCHAR   attribute name
     * DATA_TYPE         SMALLINT  attribute's SQL type from DITypes
     * ATTR_TYPE_NAME    VARCHAR   UDT: fully qualified type name
     *                            REF: fully qualified type name of target type of
     *                            the reference type.
     * ATTR_SIZE         INTEGER   column size.
     *                            char or date types => maximum number of characters;
     *                            numeric or decimal types => precision.
     * DECIMAL_DIGITS    INTEGER   # of fractional digits (scale) of number type
     * NUM_PREC_RADIX    INTEGER   Radix of number type
     * NULLABLE          INTEGER   whether NULL is allowed
     * REMARKS           VARCHAR   comment describing attribute
     * ATTR_DEF          VARCHAR   default attribute value
     * SQL_DATA_TYPE     INTEGER   expected value of SQL CLI SQL_DESC_TYPE in the SQLDA
     * SQL_DATETIME_SUB  INTEGER   DATETIME/INTERVAL => datetime/interval subcode
     * CHAR_OCTET_LENGTH INTEGER   for char types:  max bytes in column
     * ORDINAL_POSITION  INTEGER   index of column in table (starting at 1)
     * IS_NULLABLE       VARCHAR   "NO" => strictly no NULL values;
     *                             "YES" => maybe NULL values;
     *                             "" => unknown.
     * SCOPE_CATALOG     VARCHAR   catalog of REF attribute scope table or NULL
     * SCOPE_SCHEMA      VARCHAR   schema of REF attribute scope table or NULL
     * SCOPE_TABLE       VARCHAR   name of REF attribute scope table or NULL
     * SOURCE_DATA_TYPE  SMALLINT  For DISTINCT or user-generated REF DATA_TYPE:
     *                            source SQL type from DITypes
     *                            For other DATA_TYPE values:  NULL
     * </pre>
     *
     * <B>Note:</B> Currently, neither the HSQLDB engine or the JDBC driver
     * support UDTs, so an empty table is returned. <p>
     * @return a <code>Table</code> object describing the accessible
     *        attrubutes of the accessible user-defined type
     *        (UDT) objects defined within this database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_UDTATTRIBUTES() throws HsqlException {

        Table t = sysTables[SYSTEM_UDTATTRIBUTES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_UDTATTRIBUTES]);

            addColumn(t, "TYPE_CAT", Types.VARCHAR);
            addColumn(t, "TYPE_SCHEM", Types.VARCHAR);
            addColumn(t, "TYPE_NAME", Types.VARCHAR, false);           // not null
            addColumn(t, "ATTR_NAME", Types.VARCHAR, false);           // not null
            addColumn(t, "DATA_TYPE", Types.SMALLINT, false);          // not null
            addColumn(t, "ATTR_TYPE_NAME", Types.VARCHAR, false);      // not null
            addColumn(t, "ATTR_SIZE", Types.INTEGER);
            addColumn(t, "DECIMAL_DIGITS", Types.INTEGER);
            addColumn(t, "NUM_PREC_RADIX", Types.INTEGER);
            addColumn(t, "NULLABLE", Types.INTEGER);
            addColumn(t, "REMARKS", Types.VARCHAR);
            addColumn(t, "ATTR_DEF", Types.VARCHAR);
            addColumn(t, "SQL_DATA_TYPE", Types.INTEGER);
            addColumn(t, "SQL_DATETIME_SUB", Types.INTEGER);
            addColumn(t, "CHAR_OCTET_LENGTH", Types.INTEGER);
            addColumn(t, "ORDINAL_POSITION", Types.INTEGER, false);    // not null
            addColumn(t, "IS_NULLABLE", Types.VARCHAR, false);         // not null
            addColumn(t, "SCOPE_CATALOG", Types.VARCHAR);
            addColumn(t, "SCOPE_SCHEMA", Types.VARCHAR);
            addColumn(t, "SCOPE_TABLE", Types.VARCHAR);
            addColumn(t, "SOURCE_DATA_TYPE", Types.SMALLINT);
            t.createPrimaryKey(null);

            return t;
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * user-defined types defined in this database. <p>
     *
     * Schema-specific UDTs may have type JAVA_OBJECT, STRUCT, or DISTINCT.
     *
     * <P>Each row is a UDT descripion with the following columns:
     * <OL>
     *   <LI><B>TYPE_CAT</B> <code>VARCHAR</code> => the type's catalog
     *   <LI><B>TYPE_SCHEM</B> <code>VARCHAR</code> => type's schema
     *   <LI><B>TYPE_NAME</B> <code>VARCHAR</code> => type name
     *   <LI><B>CLASS_NAME</B> <code>VARCHAR</code> => Java class name
     *   <LI><B>DATA_TYPE</B> <code>VARCHAR</code> =>
     *         type value defined in <code>DITypes</code>;
     *         one of <code>JAVA_OBJECT</code>, <code>STRUCT</code>, or
     *        <code>DISTINCT</code>
     *   <LI><B>REMARKS</B> <code>VARCHAR</code> =>
     *          explanatory comment on the type
     *   <LI><B>BASE_TYPE</B><code>SMALLINT</code> =>
     *          type code of the source type of a DISTINCT type or the
     *          type that implements the user-generated reference type of the
     *          SELF_REFERENCING_COLUMN of a structured type as defined in
     *          DITypes (null if DATA_TYPE is not DISTINCT or not
     *          STRUCT with REFERENCE_GENERATION = USER_DEFINED)
     *
     * </OL> <p>
     *
     * <B>Note:</B> Currently, neither the HSQLDB engine or the JDBC driver
     * support UDTs, so an empty table is returned. <p>
     *
     * @return a <code>Table</code> object describing the accessible
     *      user-defined types defined in this database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_UDTS() throws HsqlException {

        Table t = sysTables[SYSTEM_UDTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_UDTS]);

            addColumn(t, "TYPE_CAT", Types.VARCHAR);
            addColumn(t, "TYPE_SCHEM", Types.VARCHAR);
            addColumn(t, "TYPE_NAME", Types.VARCHAR, false);     // not null
            addColumn(t, "CLASS_NAME", Types.VARCHAR, false);    // not null
            addColumn(t, "DATA_TYPE", Types.VARCHAR, false);     // not null
            addColumn(t, "REMARKS", Types.VARCHAR);
            addColumn(t, "BASE_TYPE", Types.SMALLINT);
            t.createPrimaryKey(null);

            return t;
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * columns that are automatically updated when any value in a row
     * is updated. <p>
     *
     * Each row is a version column description with the following columns: <p>
     *
     * <OL>
     * <LI><B>SCOPE</B> <code>SMALLINT</code> => is not used
     * <LI><B>COLUMN_NAME</B> <code>VARCHAR</code> => column name
     * <LI><B>DATA_TYPE</B> <code>SMALLINT</code> =>
     *        SQL data type from java.sql.Types
     * <LI><B>TYPE_NAME</B> <code>SMALLINT</code> =>
     *       Data source dependent type name
     * <LI><B>COLUMN_SIZE</B> <code>INTEGER</code> => precision
     * <LI><B>BUFFER_LENGTH</B> <code>INTEGER</code> =>
     *        length of column value in bytes
     * <LI><B>DECIMAL_DIGITS</B> <code>SMALLINT</code> => scale
     * <LI><B>PSEUDO_COLUMN</B> <code>SMALLINT</code> =>
     *        is this a pseudo column like an Oracle <code>ROWID</code>:<BR>
     *        (as defined in <code>java.sql.DatabaseMetadata</code>)
     * <UL>
     *    <LI><code>versionColumnUnknown</code> - may or may not be
     *        pseudo column
     *    <LI><code>versionColumnNotPseudo</code> - is NOT a pseudo column
     *    <LI><code>versionColumnPseudo</code> - is a pseudo column
     * </UL>
     * </OL> <p>
     *
     * <B>Note:</B> Currently, the HSQLDB engine does not support version
     * columns, so an empty table is returned. <p>
     *
     * @return a <code>Table</code> object describing the columns
     *        that are automatically updated when any value
     *        in a row is updated
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_VERSIONCOLUMNS() throws HsqlException {

        Table t = sysTables[SYSTEM_VERSIONCOLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_VERSIONCOLUMNS]);

            // ----------------------------------------------------------------
            // required by DatabaseMetaData.getVersionColumns result set
            // ----------------------------------------------------------------
            addColumn(t, "SCOPE", Types.INTEGER);
            addColumn(t, "COLUMN_NAME", Types.VARCHAR, false);       // not null
            addColumn(t, "DATA_TYPE", Types.SMALLINT, false);        // not null
            addColumn(t, "TYPE_NAME", Types.VARCHAR, false);         // not null
            addColumn(t, "COLUMN_SIZE", Types.SMALLINT);
            addColumn(t, "BUFFER_LENGTH", Types.INTEGER);
            addColumn(t, "DECIMAL_DIGITS", Types.SMALLINT);
            addColumn(t, "PSEUDO_COLUMN", Types.SMALLINT, false);    // not null

            // -----------------------------------------------------------------
            // required by DatabaseMetaData.getVersionColumns filter parameters
            // -----------------------------------------------------------------
            addColumn(t, "TABLE_CAT", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEM", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, false);        // not null

            // -----------------------------------------------------------------
            t.createPrimaryKey(null);

            return t;
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the VIEW objects
     * defined within this database. The table contains one row for each row
     * in the SYSTEM_TABLES table with a TABLE_TYPE of  VIEW . <p>
     *
     * Each row is a description of the query expression that defines its view,
     * with the following columns:
     *
     * <pre class="SqlCodeExample">
     * TABLE_CATALOG    VARCHAR     name of view's defining catalog.
     * TABLE_SCHEMA     VARCHAR     unqualified name of view's defining schema.
     * TABLE_NAME       VARCHAR     the simple name of the view.
     * VIEW_DEFINITION  VARCHAR     the character representation of the
     *                              &lt;query expression&gt; contained in the
     *                              corresponding &lt;view descriptor&gt;.
     * CHECK_OPTION     VARCHAR     {"CASCADED" | "LOCAL" | "NONE"}
     * IS_UPDATABLE     VARCHAR     {"YES" | "NO"}
     * VALID            BOOLEAN     Always TRUE: VIEW_DEFINITION currently
     *                              represents a valid &lt;query expression&gt.
     *
     * </pre> <p>
     *
     * @return a tabular description of the text source of all
     *        <code>View</code> objects accessible to
     *        the user.
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_VIEWS() throws HsqlException {

        Table t = sysTables[SYSTEM_VIEWS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_VIEWS]);

            addColumn(t, "TABLE_CATALOG", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEMA", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, true);         // not null
            addColumn(t, "VIEW_DEFINITION", Types.VARCHAR, true);    // not null
            addColumn(t, "CHECK_OPTION", Types.VARCHAR, 8, true);    // not null
            addColumn(t, "IS_UPDATABLE", Types.VARCHAR, 3, true);    // not null
            addColumn(t, "VALID", Types.BOOLEAN, true);              // not null

            // order TABLE_NAME
            // added for unique: TABLE_SCHEMA, TABLE_CATALOG
            // false PK, as TABLE_SCHEMA and/or TABLE_CATALOG may be null
            t.createPrimaryKey(null, new int[] {
                1, 2, 0
            }, false);

            return t;
        }

        Parser    parser;
        String    defn;
        Select    select;
        Tokenizer tokenizer;
        Iterator  tables;
        Table     table;
        Object[]  row;
        Session   sys;
        final int icat   = 0;
        final int ischem = 1;
        final int iname  = 2;
        final int idefn  = 3;
        final int icopt  = 4;
        final int iiupd  = 5;
        final int ivalid = 6;

        tables = database.getTables().iterator();
        sys    = database.sessionManager.getSysSession();

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!table.isView() ||!isAccessibleTable(table)) {
                continue;
            }

            row         = t.getNewRow();
            defn        = ((View) table).getStatement();
            row[icat]   = ns.getCatalogName(table);
            row[ischem] = ns.getSchemaName(table);
            row[iname]  = table.getName().name;
            row[idefn]  = defn;
            row[icopt]  = "NONE";
            row[iiupd]  = "NO";
            row[ivalid] = Boolean.TRUE;

            t.insert(row);
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the
     * return, parameter and result columns of the accessible
     * routines defined within this database.<p>
     *
     * Each row is a procedure column description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * PROCEDURE_CAT   VARCHAR   routine catalog
     * PROCEDURE_SCHEM VARCHAR   routine schema
     * PROCEDURE_NAME  VARCHAR   routine name
     * COLUMN_NAME     VARCHAR   column/parameter name
     * COLUMN_TYPE     SMALLINT  kind of column/parameter
     * DATA_TYPE       SMALLINT  SQL type from DITypes
     * TYPE_NAME       VARCHAR   SQL type name
     * PRECISION       INTEGER   precision (length) of type
     * LENGTH          INTEGER   transfer size, in bytes, if definitely known
     *                           (roughly equivalent to BUFFER_SIZE for table
     *                           columns)
     * SCALE           SMALLINT  scale
     * RADIX           SMALLINT  radix
     * NULLABLE        SMALLINT  can column contain NULL?
     * REMARKS         VARCHAR   explanatory comment on column
     * SPECIFIC_NAME   VARCHAR   typically (but not restricted to) a
     *                           fully qulified Java Method name and signature
     * SEQ             INTEGER   The JDBC-specified order within
     *                           runs of PROCEDURE_SCHEM, PROCEDURE_NAME,
     *                           SPECIFIC_NAME, which is:
     *
     *                           return value (0), if any, first, followed
     *                           by the parameter descriptions in call order
     *                           (1..n1), followed by the result column
     *                           descriptions in column number order
     *                           (n1 + 1..n1 + n2)
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the
     *        return, parameter and result columns
     *        of the accessible routines defined
     *        within this database.
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_PROCEDURECOLUMNS() throws HsqlException {

        Table t = sysTables[SYSTEM_PROCEDURECOLUMNS];

        if (t == null) {
            return super.SYSTEM_PROCEDURECOLUMNS();
        }

        // calculated column values
        String  procedureCatalog;
        String  procedureSchema;
        String  procedureName;
        String  columnName;
        Integer columnType;
        Integer dataType;
        String  dataTypeName;
        Integer precision;
        Integer length;
        Integer scale;
        Integer radix;
        Integer nullability;
        String  remark;
        String  specificName;
        int     colSequence;
        int     colCount;

        // intermediate holders
        HsqlArrayList aliasList;
        Object[]      info;
        Method        method;
        Iterator      methods;
        Object[]      row;
        DITypeInfo    ti;

        // Initialization
        methods = ns.iterateAllAccessibleMethods(session, true);    // and aliases
        ti      = new DITypeInfo();

        // no such thing as identity or ignorecase return/parameter
        // procedure columns.  Future: may need to worry about this if
        // result columns are ever reported
        ti.setTypeSub(Types.TYPE_SUB_DEFAULT);

        // Do it.
        while (methods.hasNext()) {
            info             = (Object[]) methods.next();
            method           = (Method) info[0];
            aliasList        = (HsqlArrayList) info[1];
            procedureCatalog = ns.getCatalogName(method);
            procedureSchema  = ns.getSchemaName(method);

            pi.setMethod(method);

            specificName  = pi.getSpecificName();
            procedureName = pi.getFQN();
            colCount      = pi.getColCount();

            for (int i = 0; i < colCount; i++) {
                ti.setTypeCode(pi.getColTypeCode(i));

                columnName   = pi.getColName(i);
                columnType   = pi.getColUsage(i);
                dataType     = pi.getColDataType(i);
                dataTypeName = ti.getTypeName();
                precision    = ti.getPrecision();
                length       = pi.getColLen(i);
                scale        = ti.getDefaultScale();
                radix        = ti.getNumPrecRadix();
                nullability  = pi.getColNullability(i);
                remark       = pi.getColRemark(i);
                colSequence  = pi.getColSequence(i);

                addPColRows(t, aliasList, procedureCatalog, procedureSchema,
                            procedureName, columnName, columnType, dataType,
                            dataTypeName, precision, length, scale, radix,
                            nullability, remark, specificName, colSequence);
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * routines defined within this database.
     *
     * Each row is a procedure description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * PROCEDURE_CAT     VARCHAR   catalog in which routine is defined
     * PROCEDURE_SCHEM   VARCHAR   schema in which routine is defined
     * PROCEDURE_NAME    VARCHAR   simple routine identifier
     * NUM_INPUT_PARAMS  INTEGER   number of input parameters
     * NUM_OUTPUT_PARAMS INTEGER   number of output parameters
     * NUM_RESULT_SETS   INTEGER   number of result sets returned
     * REMARKS           VARCHAR   explanatory comment on the routine
     * PROCEDURE_TYPE    SMALLINT  { Unknown | No Result | Returns Result }
     * ORIGIN            VARCHAR   {ALIAS |
     *                             [BUILTIN | USER DEFINED] ROUTINE |
     *                             [BUILTIN | USER DEFINED] TRIGGER |
     *                              ...}
     * SPECIFIC_NAME     VARCHAR   typically (but not restricted to) a
     *                             a fully qualified Java Method name
     *                             and signature
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the accessible
     *        routines defined within the this database
     * @throws HsqlException if an error occurs while producing the table
     */
    Table SYSTEM_PROCEDURES() throws HsqlException {

        Table t = sysTables[SYSTEM_PROCEDURES];

        if (t == null) {
            return super.SYSTEM_PROCEDURES();
        }

        // calculated column values
        // ------------------------
        // required
        // ------------------------
        String  catalog;
        String  schema;
        String  procName;
        Integer numInputParams;
        Integer numOutputParams;
        Integer numResultSets;
        String  remarks;
        Integer procRType;

        // -------------------
        // extended
        // -------------------
        String procOrigin;
        String specificName;

        // intermediate holders
        String        alias;
        HsqlArrayList aliasList;
        Iterator      methods;
        Object[]      methodInfo;
        Method        method;
        String        methodOrigin;
        Object[]      row;

        // Initialization
        methods = ns.iterateAllAccessibleMethods(session, true);    //and aliases

        // Do it.
        while (methods.hasNext()) {
            methodInfo   = (Object[]) methods.next();
            method       = (Method) methodInfo[0];
            aliasList    = (HsqlArrayList) methodInfo[1];
            methodOrigin = (String) methodInfo[2];

            pi.setMethod(method);

            catalog         = ns.getCatalogName(method);
            schema          = ns.getSchemaName(method);
            procName        = pi.getFQN();
            numInputParams  = pi.getInputParmCount();
            numOutputParams = pi.getOutputParmCount();
            numResultSets   = pi.getResultSetCount();
            remarks         = pi.getRemark();
            procRType       = pi.getResultType(methodOrigin);
            procOrigin      = pi.getOrigin(methodOrigin);
            specificName    = pi.getSpecificName();

            addProcRows(t, aliasList, catalog, schema, procName,
                        numInputParams, numOutputParams, numResultSets,
                        remarks, procRType, procOrigin, specificName);
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The SYSTEM_USAGE_PRIVILEGES table has one row for each usage privilege
     * descriptor. <p>
     *
     * It effectively contains a representation of the usage privilege
     * descriptors. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_USAGE_PRIVILEGES (
     *      GRANTOR         VARCHAR NOT NULL,
     *      GRANTEE         VARCHAR NOT NULL,
     *      OBJECT_CATALOG  VARCHAR NULL,
     *      OBJECT_SCHEMA   VARCHAR NULL,
     *      OBJECT_NAME     VARCHAR NOT NULL,
     *      OBJECT_TYPE     VARCHAR NOT NULL
     *
     *          CHECK ( OBJECT_TYPE IN (
     *                      'DOMAIN',
     *                      'CHARACTER SET',
     *                      'COLLATION',
     *                      'TRANSLATION',
     *                      'SEQUENCE' ) ),
     *
     *      IS_GRANTABLE    VARCHAR NOT NULL
     *
     *          CHECK ( IS_GRANTABLE IN ( 'YES', 'NO' ) ),
     *
     *      UNIQUE( GRANTOR, GRANTEE, OBJECT_CATALOG,
     *              OBJECT_SCHEMA, OBJECT_NAME, OBJECT_TYPE )
     * )
     * </pre>
     *
     * <b>Description:</b><p>
     *
     * <ol>
     * <li> The value of GRANTOR is the &lt;authorization identifier&gt; of the
     *      user or role who granted usage privileges on the object of the type
     *      identified by OBJECT_TYPE that is identified by OBJECT_CATALOG,
     *      OBJECT_SCHEMA, and OBJECT_NAME, to the user or role identified by the
     *      value of GRANTEE forthe usage privilege being described. <p>
     *
     * <li> The value of GRANTEE is the &lt;authorization identifier&gt; of some
     *      user or role, or PUBLIC to indicate all users, to whom the usage
     *      privilege being described is granted. <p>
     *
     * <li> The values of OBJECT_CATALOG, OBJECT_SCHEMA, and OBJECT_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the object to which the privilege applies. <p>
     *
     * <li> The values of OBJECT_TYPE have the following meanings: <p>
     *
     *      <table border cellpadding="3">
     *          <tr>
     *              <td nowrap>DOMAIN</td>
     *              <td nowrap>The object to which the privilege applies is
     *                         a domain.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>CHARACTER SET</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         character set.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>COLLATION</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         collation.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>TRANSLATION</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         transliteration.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>SEQUENCE</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         sequence generator.</td>
     *          <tr>
     *      </table> <p>
     *
     * <li> The values of IS_GRANTABLE have the following meanings: <p>
     *
     *      <table border cellpadding="3">
     *          <tr>
     *              <td nowrap>YES</td>
     *              <td nowrap>The privilege being described was granted
     *                         WITH GRANT OPTION and is thus grantable.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>NO</td>
     *              <td nowrap>The privilege being described was not granted
     *                  WITH GRANT OPTION and is thus not grantable.</td>
     *          <tr>
     *      </table> <p>
     * <ol>
     */
    Table SYSTEM_USAGE_PRIVILEGES() throws HsqlException {

        Table t = sysTables[SYSTEM_USAGE_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_USAGE_PRIVILEGES]);

            addColumn(t, "GRANTOR", Types.VARCHAR, false);            // not null
            addColumn(t, "GRANTEE", Types.VARCHAR, false);            // not null
            addColumn(t, "OBJECT_CATALOG", Types.VARCHAR);
            addColumn(t, "OBJECT_SCHEMA", Types.VARCHAR);
            addColumn(t, "OBJECT_NAME", Types.VARCHAR, false);        // not null
            addColumn(t, "OBJECT_TYPE", Types.VARCHAR, 32, false);    // not null
            addColumn(t, "IS_GRANTABLE", Types.VARCHAR, 3, false);    // not null

            // order: COLUMN_NAME, PRIVILEGE
            // for unique: GRANTEE, GRANTOR, TABLE_NAME, TABLE_SCHEM, TABLE_CAT
            // false PK, as TABLE_SCHEM and/or TABLE_CAT may be null
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        Result rs = session.sqlExecuteDirectNoPreChecks(
            "select 'SYS', 'PUBLIC', SEQUENCE_CATALOG, SEQUENCE_SCHEMA, "
            + "SEQUENCE_NAME, 'SEQUENCE', 'FALSE' from  SYSTEM_SEQUENCES");

        t.insert(rs);
        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The CHECK_COLUMN_USAGE table has one row for each column identified by
     * a &lt;column reference&gt; contained in the &lt;search condition&gt;
     * of a check constraint, domain constraint, or assertion. <p>
     *
     * <b>Definition:</b><p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE CHECK_COLUMN_USAGE (
     *      CONSTRAINT_CATALOG  VARCHAR NULL,
     *      CONSTRAINT_SCHEMA   VARCHAR NULL,
     *      CONSTRAINT_NAME     VARCHAR NOT NULL,
     *      TABLE_CATALOG       VARCHAR NULL,
     *      TABLE_SCHEMA        VARCHAR NULL,
     *      TABLE_NAME          VARCHAR NOT NULL,
     *      COLUMN_NAME         VARCHAR NOT NULL,
     *      UNIQUE( CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME,
     *              TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema name,
     *      and qualified identifier, respectively, of the constraint being
     *      described. <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, and
     *      COLUMN_NAME are the catalog name, unqualified schema name,
     *      qualified identifier, and column name, respectively, of a column
     *      identified by a &lt;column reference&gt; explicitly or implicitly
     *      contained in the &lt;search condition&gt; of the constraint
     *      being described.
     * </ol>
     */
    Table SYSTEM_CHECK_COLUMN_USAGE() throws HsqlException {

        Table t = sysTables[SYSTEM_CHECK_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[SYSTEM_CHECK_COLUMN_USAGE]);

            addColumn(t, "CONSTRAINT_CATALOG", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_SCHEMA", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_NAME", Types.VARCHAR, false);    // not null
            addColumn(t, "TABLE_CATALOG", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEMA", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, false);         // not null
            addColumn(t, "COLUMN_NAME", Types.VARCHAR, false);        // not null
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        // calculated column values
        String constraintCatalog;
        String constraintSchema;
        String constraintName;

        // Intermediate holders
        Iterator             tables;
        Table                table;
        Constraint[]         constraints;
        int                  constraintCount;
        Constraint           constraint;
        Expression.Collector collector;
        Expression           expression;
        TableFilter          tableFilter;
        Table                columnTable;
        Iterator             iterator;
        Result               result;
        Object[]             resultRow;
        Object[]             row;

        // column number mappings
        final int icons_cat   = 0;
        final int icons_schem = 1;
        final int icons_name  = 2;
        final int itab_cat    = 3;
        final int itab_schem  = 4;
        final int itab_name   = 5;
        final int itab_col    = 6;

        // Initialization
        tables    = database.getTables().iterator();
        collector = new Expression.Collector();
        result    = new Result(ResultConstants.DATA, 4);
        result.metaData.colTypes[0] = result.metaData.colTypes[1] =
            result.metaData.colTypes[2] = result.metaData.colTypes[3] =
            Types.VARCHAR;

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!isAccessibleTable(table)) {
                continue;
            }

            constraints       = table.getConstraints();
            constraintCount   = constraints.length;
            constraintCatalog = ns.getCatalogName(table);
            constraintSchema  = ns.getSchemaName(table);

            // process constraints
            for (int i = 0; i < constraintCount; i++) {
                constraint = (Constraint) constraints[i];

                if (constraint.getType() != Constraint.CHECK) {
                    continue;
                }

                constraintName = constraint.getName().name;

                result.setRows(null);
                collector.clear();
                collector.addAll(constraint.core.check, Expression.COLUMN);

                iterator = collector.iterator();

                // calculate distinct column references
                while (iterator.hasNext()) {
                    expression  = (Expression) iterator.next();
                    tableFilter = expression.getFilter();
                    columnTable = tableFilter.getTable();

                    if (columnTable.getTableType() == Table.SYSTEM_SUBQUERY
                            ||!isAccessibleTable(columnTable)) {
                        continue;
                    }

                    result.add(new Object[] {
                        ns.getCatalogName(columnTable),
                        ns.getSchemaName(columnTable),
                        columnTable.getName().name, expression.getColumnName()
                    });
                }

                result.removeDuplicates();

                iterator = result.iterator();

                while (iterator.hasNext()) {
                    row              = t.getNewRow();
                    resultRow        = (Object[]) iterator.next();
                    row[icons_cat]   = constraintCatalog;
                    row[icons_schem] = constraintSchema;
                    row[icons_name]  = constraintName;
                    row[itab_cat]    = resultRow[0];
                    row[itab_schem]  = resultRow[1];
                    row[itab_name]   = resultRow[2];
                    row[itab_col]    = resultRow[3];

                    t.insert(row);
                }
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The CHECK_ROUTINE_USAGE base table has one row for each
     * SQL-invoked routine identified as the subject routine of either a
     * &lt;routine invocation&gt;, a &lt;method reference&gt;, a
     * &lt;method invocation&gt;, or a &lt;static method invocation&gt;
     * contained in an &lt;assertion definition&gt;, a &lt;domain
     * constraint&gt;, or a &lt;table constraint definition&gt;. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_CHECK_ROUTINE_USAGE (
     *      CONSTRAINT_CATALOG      VARCHAR NULL,
     *      CONSTRAINT_SCHEMA       VARCHAR NULL,
     *      CONSTRAINT_NAME         VARCHAR NOT NULL,
     *      SPECIFIC_CATALOG        VARCHAR NULL,
     *      SPECIFIC_SCHEMA         VARCHAR NULL,
     *      SPECIFIC_NAME           VARCHAR NOT NULL,
     *      UNIQUE( CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME,
     *              SPECIFIC_CATALOG, SPECIFIC_SCHEMA, SPECIFIC_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The CHECK_ROUTINE_USAGE table has one row for each
     *      SQL-invoked routine R identified as the subject routine of either a
     *      &lt;routine invocation&gt;, a &lt;method reference&gt;, a &lt;method
     *      invocation&gt;, or a &lt;static method invocation&gt; contained in
     *      an &lt;assertion definition&gt; or in the &lt;check constraint
     *      definition&gt; contained in either a &lt;domain constraint&gt; or a
     *      &lt;table constraint definition&gt;. <p>
     *
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema name, and
     *      qualified identifier, respectively, of the assertion or check
     *     constraint being described. <p>
     *
     * <li> The values of SPECIFIC_CATALOG, SPECIFIC_SCHEMA, and SPECIFIC_NAME
     *      are the catalog name, unqualified schema name, and qualified
     *      identifier, respectively, of the specific name of R. <p>
     *
     * </ol>
     */
    Table SYSTEM_CHECK_ROUTINE_USAGE() throws HsqlException {

        Table t = sysTables[SYSTEM_CHECK_ROUTINE_USAGE];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[SYSTEM_CHECK_ROUTINE_USAGE]);

            addColumn(t, "CONSTRAINT_CATALOG", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_SCHEMA", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_NAME", Types.VARCHAR, false);    // not null
            addColumn(t, "SPECIFIC_CATALOG", Types.VARCHAR);
            addColumn(t, "SPECIFIC_SCHEMA", Types.VARCHAR);
            addColumn(t, "SPECIFIC_NAME", Types.VARCHAR, false);      // not null
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // calculated column values
        String constraintCatalog;
        String constraintSchema;
        String constraintName;

        // Intermediate holders
        Iterator             tables;
        Table                table;
        Constraint[]         constraints;
        int                  constraintCount;
        Constraint           constraint;
        Expression.Collector collector;
        Expression           expression;
        Function             function;
        Iterator             iterator;
        HashSet              methodSet;
        Method               method;
        Object[]             row;

        // column number mappings
        final int icons_cat   = 0;
        final int icons_schem = 1;
        final int icons_name  = 2;
        final int ir_cat      = 3;
        final int ir_schem    = 4;
        final int ir_name     = 5;

        tables    = database.getTables().iterator();
        collector = new Expression.Collector();

        while (tables.hasNext()) {
            collector.clear();

            table = (Table) tables.next();

            if (!isAccessibleTable(table)) {
                continue;
            }

            constraints       = table.getConstraints();
            constraintCount   = constraints.length;
            constraintCatalog = ns.getCatalogName(table);
            constraintSchema  = ns.getSchemaName(table);

            for (int i = 0; i < constraintCount; i++) {
                constraint = (Constraint) constraints[i];

                if (constraint.getType() != Constraint.CHECK) {
                    continue;
                }

                constraintName = constraint.getName().name;

                collector.addAll(constraint.core.check, Expression.FUNCTION);

                methodSet = new HashSet();
                iterator  = collector.iterator();

                while (iterator.hasNext()) {
                    expression = (Expression) iterator.next();
                    function   = expression.function;

                    if (!session.isAccessible(
                            function.mMethod.getDeclaringClass().getName())) {
                        continue;
                    }

                    methodSet.add(function.mMethod);
                }

                iterator = methodSet.iterator();

                while (iterator.hasNext()) {
                    method           = (Method) iterator.next();
                    row              = t.getNewRow();
                    row[icons_cat]   = constraintCatalog;
                    row[icons_schem] = constraintSchema;
                    row[icons_name]  = constraintName;
                    row[ir_cat]      = ns.getCatalogName(method);
                    row[ir_schem]    = ns.getSchemaName(method);
                    row[ir_name]     = ns.getMethodSpecificName(method);

                    t.insert(row);
                }
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The CHECK_TABLE_USAGE table has one row for each table identified by a
     * &lt;table name&gt; simply contained in a &lt;table reference&gt;
     * contained in the &lt;search condition&gt; of a check constraint,
     * domain constraint, or assertion. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE STABLE SYSTEM_CHECK_TABLE_USAGE (
     *      CONSTRAINT_CATALOG      VARCHAR NULL,
     *      CONSTRAINT_SCHEMA       VARCHAR NULL,
     *      CONSTRAINT_NAME         VARCHAR NOT NULL,
     *      TABLE_CATALOG           VARCHAR NULL,
     *      TABLE_SCHEMA            VARCHAR NOT NULL,
     *      TABLE_NAME INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *     UNIQUE( CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME,
     *            TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema name,
     *       and qualified identifier, respectively, of the constraint being
     *      described. <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of a table identified by a &lt;table name&gt;
     *      simply contained in a &lt;table reference&gt; contained in the
     *      *lt;search condition&gt; of the constraint being described.
     * </ol>
     */
    Table SYSTEM_CHECK_TABLE_USAGE() throws HsqlException {

        Table t = sysTables[SYSTEM_CHECK_TABLE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_CHECK_TABLE_USAGE]);

            addColumn(t, "CONSTRAINT_CATALOG", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_SCHEMA", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_NAME", Types.VARCHAR, false);    // not null
            addColumn(t, "TABLE_CATALOG", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEMA", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, false);         // not null
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        //
        Result rs = session.sqlExecuteDirectNoPreChecks(
            "select DISTINCT CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, "
            + "CONSTRAINT_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME "
            + "from SYSTEM_CHECK_COLUMN_USAGE");

        t.insert(rs);
        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The TABLE_CONSTRAINTS table has one row for each table constraint
     * associated with a table.  <p>
     *
     * It effectively contains a representation of the table constraint
     * descriptors. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_TABLE_CONSTRAINTS (
     *      CONSTRAINT_CATALOG      VARCHAR NULL,
     *      CONSTRAINT_SCHEMA       VARCHAR NULL,
     *      CONSTRAINT_NAME         VARCHAR NOT NULL,
     *      CONSTRAINT_TYPE         VARCHAR NOT NULL,
     *      TABLE_CATALOG           VARCHAR NULL,
     *      TABLE_SCHEMA            VARCHAR NULL,
     *      TABLE_NAME              VARCHAR NOT NULL,
     *      IS_DEFERRABLE           VARCHAR NOT NULL,
     *      INITIALLY_DEFERRED      VARCHAR NOT NULL,
     *
     *      CHECK ( CONSTRAINT_TYPE IN
     *                      ( 'UNIQUE', 'PRIMARY KEY',
     *                        'FOREIGN KEY', 'CHECK' ) ),
     *
     *      CHECK ( ( IS_DEFERRABLE, INITIALLY_DEFERRED ) IN
     *              ( VALUES ( 'NO',  'NO'  ),
     *                       ( 'YES', 'NO'  ),
     *                       ( 'YES', 'YES' ) ) )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema
     *      name, and qualified identifier, respectively, of the
     *      constraint being described. If the &lt;table constraint
     *      definition&gt; or &lt;add table constraint definition&gt;
     *      that defined the constraint did not specify a
     *      &lt;constraint name&gt;, then the values of CONSTRAINT_CATALOG,
     *      CONSTRAINT_SCHEMA, and CONSTRAINT_NAME are
     *      implementation-defined. <p>
     *
     * <li> The values of CONSTRAINT_TYPE have the following meanings: <p>
     *  <table border cellpadding="3">
     *  <tr>
     *      <td nowrap>FOREIGN KEY</td>
     *      <td nowrap>The constraint being described is a
     *                 foreign key constraint.</td>
     *  </tr>
     *  <tr>
     *      <td nowrap>UNIQUE</td>
     *      <td nowrap>The constraint being described is a
     *                 unique constraint.</td>
     *  </tr>
     *  <tr>
     *      <td nowrap>PRIMARY KEY</td>
     *      <td nowrap>The constraint being described is a
     *                 primary key constraint.</td>
     *  </tr>
     *  <tr>
     *      <td nowrap>CHECK</td>
     *      <td nowrap>The constraint being described is a
     *                 check constraint.</td>
     *  </tr>
     * </table> <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are
     *      the catalog name, the unqualified schema name, and the
     *      qualified identifier of the name of the table to which the
     *      table constraint being described applies. <p>
     *
     * <li> The values of IS_DEFERRABLE have the following meanings: <p>
     *
     *  <table>
     *      <tr>
     *          <td nowrap>YES</td>
     *          <td nowrap>The table constraint is deferrable.</td>
     *      </tr>
     *      <tr>
     *          <td nowrap>NO</td>
     *          <td nowrap>The table constraint is not deferrable.</td>
     *      </tr>
     *  </table> <p>
     *
     * <li> The values of INITIALLY_DEFERRED have the following meanings: <p>
     *
     *  <table>
     *      <tr>
     *          <td nowrap>YES</td>
     *          <td nowrap>The table constraint is initially deferred.</td>
     *      </tr>
     *      <tr>
     *          <td nowrap>NO</td>
     *          <td nowrap>The table constraint is initially immediate.</td>
     *      </tr>
     *  </table> <p>
     * </ol>
     */
    Table SYSTEM_TABLE_CONSTRAINTS() throws HsqlException {

        Table t = sysTables[SYSTEM_TABLE_CONSTRAINTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TABLE_CONSTRAINTS]);

            addColumn(t, "CONSTRAINT_CATALOG", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_SCHEMA", Types.VARCHAR);
            addColumn(t, "CONSTRAINT_NAME", Types.VARCHAR, false);          // not null
            addColumn(t, "CONSTRAINT_TYPE", Types.VARCHAR, 11, false);      // not null
            addColumn(t, "TABLE_CATALOG", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEMA", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, false);               // not null
            addColumn(t, "IS_DEFERRABLE", Types.VARCHAR, 3, false);         // not null
            addColumn(t, "INITIALLY_DEFERRED", Types.VARCHAR, 3, false);    // not null

            // false PK, as CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA,
            // TABLE_CATALOG and/or TABLE_SCHEMA may be null
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);

            return t;
        }

        // Intermediate holders
        Iterator     tables;
        Table        table;
        Index        index;
        Constraint[] constraints;
        int          constraintCount;
        Constraint   constraint;
        String       cat;
        String       schem;
        HashSet      constraintSet;
        Object[]     row;

        // column number mappings
        final int icons_cat   = 0;
        final int icons_schem = 1;
        final int icons_name  = 2;
        final int icons_type  = 3;
        final int itab_cat    = 4;
        final int itab_schem  = 5;
        final int itab_name   = 6;
        final int iis_defr    = 7;
        final int iinit_defr  = 8;

        // initialization
        tables        = database.getTables().iterator();
        constraintSet = new HashSet();
        table         = null;    // else complier complains

        // do it
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() ||!isAccessibleTable(table)) {
                continue;
            }

            index = table.getPrimaryIndex();

            if (index != null && table.getPrimaryKey() != null) {
                row              = t.getNewRow();
                cat              = ns.getCatalogName(table);
                schem            = ns.getSchemaName(table);
                row[icons_cat]   = cat;
                row[icons_schem] = schem;
                row[icons_name]  = index.getName().name;
                row[icons_type]  = "PRIMARY KEY";
                row[itab_cat]    = cat;
                row[itab_schem]  = schem;
                row[itab_name]   = table.getName().name;
                row[iis_defr]    = "NO";
                row[iinit_defr]  = "NO";

                t.insert(row);
            }

            constraints     = table.getConstraints();
            constraintCount = constraints.length;

            for (int i = 0; i < constraintCount; i++) {
                constraint = constraints[i];

                if (constraint.getType() == Constraint.FOREIGN_KEY
                        &&!isAccessibleTable(constraint.getRef())) {
                    continue;
                }

                constraintSet.add(constraint);
            }
        }

        for (Iterator it = constraintSet.iterator(); it.hasNext(); ) {
            row        = t.getNewRow();
            constraint = (Constraint) it.next();

            switch (constraint.getType()) {

                case Constraint.CHECK : {
                    row[icons_type] = "CHECK";
                    table           = constraint.getMain();

                    break;
                }
                case Constraint.UNIQUE : {
                    row[icons_type] = "UNIQUE";
                    table           = constraint.getMain();

                    break;
                }
                case Constraint.FOREIGN_KEY : {
                    row[icons_type] = "FOREIGN KEY";
                    table           = constraint.getRef();

                    break;
                }
                case Constraint.MAIN :
                default : {
                    continue;
                }
            }

            cat              = ns.getCatalogName(table);
            schem            = ns.getSchemaName(table);
            row[icons_cat]   = cat;
            row[icons_schem] = schem;
            row[icons_name]  = constraint.constName.name;
            row[itab_cat]    = cat;
            row[itab_schem]  = schem;
            row[itab_name]   = table.getName().name;
            row[iis_defr]    = "NO";
            row[iinit_defr]  = "NO";

            t.insert(row);
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The SYSTEM_VIEW_TABLE_USAGE table has one row for each table identified
     * by a &lt;table name&gt; simply contained in a &lt;table reference&gt;
     * that is contained in the &lt;query expression&gt; of a view. <p>
     *
     * <b>Definition</b><p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_VIEW_TABLE_USAGE (
     *      VIEW_CATALOG    VARCHAR NULL,
     *      VIEW_SCHEMA     VARCHAR NULL,
     *      VIEW_NAME       VARCHAR NULL,
     *      TABLE_CATALOG   VARCHAR NULL,
     *      TABLE_SCHEMA    VARCHAR NULL,
     *      TABLE_NAME      VARCHAR NULL,
     *      UNIQUE( VIEW_CATALOG, VIEW_SCHEMA, VIEW_NAME,
     *              TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b><p>
     *
     * <ol>
     * <li> The values of VIEW_CATALOG, VIEW_SCHEMA, and VIEW_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the view being described. <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of a table identified by a &lt;table name&gt;
     *      simply contained in a &lt;table reference&gt; that is contained in
     *      the &lt;query expression&gt; of the view being described.
     * </ol>
     */
    Table SYSTEM_VIEW_TABLE_USAGE() throws HsqlException {

        Table t = sysTables[SYSTEM_VIEW_TABLE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_VIEW_TABLE_USAGE]);

            addColumn(t, "VIEW_CATALOG", Types.VARCHAR);
            addColumn(t, "VIEW_SCHEMA", Types.VARCHAR);
            addColumn(t, "VIEW_NAME", Types.VARCHAR, true);     // not null
            addColumn(t, "TABLE_CATALOG", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEMA", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, true);    // not null

            // false PK, as VIEW_CATALOG, VIEW_SCHEMA, TABLE_CATALOG, and/or
            // TABLE_SCHEMA may be NULL
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        //
        Result rs = session.sqlExecuteDirectNoPreChecks(
            "select DISTINCT VIEW_CATALOG, VIEW_SCHEMA, "
            + "VIEW_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME "
            + "from SYSTEM_VIEW_COLUMN_USAGE");

        t.insert(rs);
        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The SYSTEM_VIEW_COLUMN_USAGE table has one row for each column of a
     * table that is explicitly or implicitly referenced in the
     * &lt;query expression&gt; of the view being described. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_VIEW_COLUMN_USAGE (
     *      VIEW_CATALOG    VARCHAR NULL,
     *      VIEW_SCHEMA     VARCHAR NULL,
     *      VIEW_NAME       VARCHAR NOT NULL,
     *      TABLE_CATALOG   VARCHAR NULL,
     *      TABLE_SCHEMA    VARCHAR NULL,
     *      TABLE_NAME      VARCHAR NOT NULL,
     *      COLUMN_NAME     VARCHAR NOT NULL,
     *      UNIQUE ( VIEW_CATALOG, VIEW_SCHEMA, VIEW_NAME,
     *               TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
     *               COLUMN_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of VIEW_CATALOG, VIEW_SCHEMA, and VIEW_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the view being described. <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, and
     *      COLUMN_NAME are the catalog name, unqualified schema name,
     *      qualified identifier, and column name, respectively, of a column
     *      of a table that is explicitly or implicitly referenced in the
     *      &lt;query expression&gt; of the view being described.
     * </ol>
     */
    Table SYSTEM_VIEW_COLUMN_USAGE() throws HsqlException {

        Table t = sysTables[SYSTEM_VIEW_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_VIEW_COLUMN_USAGE]);

            addColumn(t, "VIEW_CATALOG", Types.VARCHAR);
            addColumn(t, "VIEW_SCHEMA", Types.VARCHAR);
            addColumn(t, "VIEW_NAME", Types.VARCHAR, true);      // not null
            addColumn(t, "TABLE_CATALOG", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEMA", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, true);     // not null
            addColumn(t, "COLUMN_NAME", Types.VARCHAR, true);    // not null

            // false PK, as VIEW_CATALOG, VIEW_SCHEMA, TABLE_CATALOG, and/or
            // TABLE_SCHEMA may be NULL
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        // Calculated column values
        String viewCatalog;
        String viewSchema;
        String viewName;

        // Intermediate holders
        Iterator             tables;
        View                 view;
        Table                table;
        Object[]             row;
        SubQuery[]           subqueries;
        Select               select;
        Expression           expression;
        TableFilter          tableFilter;
        Table                columnTable;
        Result               result;
        Object[]             resultRow;
        Iterator             iterator;
        Expression.Collector collector;

        // Column number mappings
        final int iv_cat   = 0;
        final int iv_schem = 1;
        final int iv_name  = 2;
        final int it_cat   = 3;
        final int it_schem = 4;
        final int it_name  = 5;
        final int it_cname = 6;

        // Initialization
        tables    = database.getTables().iterator();
        collector = new Expression.Collector();
        result    = new Result(ResultConstants.DATA, 4);
        result.metaData.colTypes[0] = result.metaData.colTypes[1] =
            result.metaData.colTypes[2] = result.metaData.colTypes[3] =
            Types.VARCHAR;

        // Do it.
        while (tables.hasNext()) {
            collector.clear();
            result.setRows(null);

            table = (Table) tables.next();

            if (table.isView() && isAccessibleTable(table)) {

                // fall through
            } else {
                continue;
            }

            viewCatalog = ns.getCatalogName(table);
            viewSchema  = ns.getSchemaName(table);
            viewName    = table.getName().name;
            view        = (View) table;
            subqueries  = view.viewSubqueries;

            collector.addAll(view.viewSelect, Expression.COLUMN);

            for (int i = 0; i < subqueries.length; i++) {
                collector.addAll(subqueries[i].select, Expression.COLUMN);
            }

            iterator = collector.iterator();

            while (iterator.hasNext()) {
                expression  = (Expression) iterator.next();
                tableFilter = expression.getFilter();
                columnTable = tableFilter.getTable();

                if (columnTable.tableType == Table.SYSTEM_SUBQUERY
                        ||!isAccessibleTable(columnTable)) {
                    continue;
                }

                result.add(new Object[] {
                    ns.getCatalogName(columnTable),
                    ns.getSchemaName(columnTable), columnTable.getName().name,
                    expression.getColumnName()
                });
            }

            result.removeDuplicates();

            iterator = result.iterator();

            while (iterator.hasNext()) {
                row           = t.getNewRow();
                resultRow     = (Object[]) iterator.next();
                row[iv_cat]   = viewCatalog;
                row[iv_schem] = viewSchema;
                row[iv_name]  = viewName;
                row[it_cat]   = resultRow[0];
                row[it_schem] = resultRow[1];
                row[it_name]  = resultRow[2];
                row[it_cname] = resultRow[3];

                t.insert(row);
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * The SYSTEM_VIEW_ROUTINE_USAGE table has one row for each SQL-invoked
     * routine identified as the subject routine of either a &lt;routine
     * invocation&gt;, a &lt;method reference&gt;, a &lt;method invocation&gt;,
     * or a &lt;static method invocation&gt; contained in a &lt;view
     * definition&gt;. <p>
     *
     * <b>Definition</b><p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE VIEW_ROUTINE_USAGE (
     *      TABLE_CATALOG       VARCHAR NULL,
     *      TABLE_SCHEMA        VARCHAR NULL,
     *      TABLE_NAME          VARCHAR NOT NULL,
     *      SPECIFIC_CATALOG    VARCHAR NULL,
     *      SPECIFIC_SCHEMA     VARCHAR NULL,
     *      SPECIFIC_NAME       VARCHAR NOT NULL,
     *      UNIQUE( TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
     *              SPECIFIC_CATALOG, SPECIFIC_SCHEMA,
     *              SPECIFIC_NAME )
     * )
     * </pre>
     *
     * <b>Description</b><p>
     *
     * <ol>
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the viewed table being described. <p>
     *
     * <li> The values of SPECIFIC_CATALOG, SPECIFIC_SCHEMA, and SPECIFIC_NAME are
     *      the catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the specific name of R. <p>
     * </ol>
     */
    Table SYSTEM_VIEW_ROUTINE_USAGE() throws HsqlException {

        Table t = sysTables[SYSTEM_VIEW_ROUTINE_USAGE];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[SYSTEM_VIEW_ROUTINE_USAGE]);

            addColumn(t, "TABLE_CATALOG", Types.VARCHAR);
            addColumn(t, "TABLE_SCHEMA", Types.VARCHAR);
            addColumn(t, "TABLE_NAME", Types.VARCHAR, true);       // not null
            addColumn(t, "SPECIFIC_CATALOG", Types.VARCHAR);
            addColumn(t, "SPECIFIC_SCHEMA", Types.VARCHAR);
            addColumn(t, "SPECIFIC_NAME", Types.VARCHAR, true);    // not null

            // false PK, as VIEW_CATALOG, VIEW_SCHEMA, TABLE_CATALOG, and/or
            // TABLE_SCHEMA may be NULL
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // Calculated column values
        String viewCat;
        String viewSchem;
        String viewName;

        // Intermediate holders
        Iterator             tables;
        View                 view;
        Table                table;
        Object[]             row;
        SubQuery[]           subqueries;
        Select               select;
        Expression           expression;
        Function             function;
        Expression.Collector collector;
        Method               method;
        HashSet              methodSet;
        Iterator             iterator;

        // Column number mappings
        final int iv_cat   = 0;
        final int iv_schem = 1;
        final int iv_name  = 2;
        final int ir_cat   = 3;
        final int ir_schem = 4;
        final int ir_name  = 5;
        final int ir_sig   = 6;

        // Initialization
        tables    = database.getTables().iterator();
        collector = new Expression.Collector();

        // Do it.
        while (tables.hasNext()) {
            collector.clear();

            table = (Table) tables.next();

            if (table.isView() && isAccessibleTable(table)) {

                // fall through
            } else {
                continue;
            }

            viewCat    = ns.getCatalogName(table);
            viewSchem  = ns.getSchemaName(table);
            viewName   = table.getName().name;
            view       = (View) table;
            subqueries = view.viewSubqueries;

            collector.addAll(view.viewSelect, Expression.FUNCTION);

            for (int i = 0; i < subqueries.length; i++) {
                collector.addAll(subqueries[i].select, Expression.FUNCTION);
            }

            methodSet = new HashSet();
            iterator  = collector.iterator();

            while (iterator.hasNext()) {
                expression = (Expression) iterator.next();
                function   = expression.function;

                if (session.isAccessible(
                        function.mMethod.getDeclaringClass().getName())) {
                    methodSet.add(function.mMethod);
                }
            }

            iterator = methodSet.iterator();

            while (iterator.hasNext()) {
                method        = (Method) iterator.next();
                row           = t.getNewRow();
                row[iv_cat]   = viewCat;
                row[iv_schem] = viewSchem;
                row[iv_name]  = viewName;
                row[ir_cat]   = ns.getCatalogName(method);
                row[ir_schem] = ns.getSchemaName(method);
                row[ir_name]  = ns.getMethodSpecificName(method);

                t.insert(row);
            }
        }

        t.setDataReadOnly(true);

        return t;
    }

    /**
     * Inserts a set of procedure column description rows into the
     * <code>Table</code> specified by the <code>t</code> argument. <p>
     *
     * @param t the table in which the rows are to be inserted
     * @param l the list of procedure name aliases to which the
     *        specified column values apply
     * @param cat the procedure's catalog name
     * @param schem the procedure's schema name
     * @param pName the procedure's simple base (non-alias) name
     * @param cName the procedure column name
     * @param cType the column type (return, parameter, result)
     * @param dType the column's data type code
     * @param tName the column's canonical data type name
     * @param prec the column's precision
     * @param len the column's buffer length
     * @param scale the column's scale (decimal digits)
     * @param radix the column's numeric precision radix
     * @param nullability the column's java.sql.DatbaseMetaData
     *      nullabiliy code
     * @param remark a human-readable remark regarding the column
     * @param specificName the specific name of the procedure
     *      (typically but not limited to
     *      a fully qualified Java Method name and signature)
     * @param seq helper value to back JDBC contract sort order
     * @throws HsqlException if there is problem inserting the specified rows
     *      in the table
     *
     */
    protected void addPColRows(Table t, HsqlArrayList l, String cat,
                               String schem, String pName, String cName,
                               Integer cType, Integer dType, String tName,
                               Integer prec, Integer len, Integer scale,
                               Integer radix, Integer nullability,
                               String remark, String specificName,
                               int seq) throws HsqlException {

        // column number mappings
        final int icat       = 0;
        final int ischem     = 1;
        final int iname      = 2;
        final int icol_name  = 3;
        final int icol_type  = 4;
        final int idata_type = 5;
        final int itype_name = 6;
        final int iprec      = 7;
        final int ilength    = 8;
        final int iscale     = 9;
        final int iradix     = 10;
        final int inullable  = 11;
        final int iremark    = 12;
        final int isn        = 13;
        final int iseq       = 14;
        Object[]  row        = t.getNewRow();
        Integer   sequence   = ValuePool.getInt(seq);

        row[icat]       = cat;
        row[ischem]     = schem;
        row[iname]      = pName;
        row[icol_name]  = cName;
        row[icol_type]  = cType;
        row[idata_type] = dType;
        row[itype_name] = tName;
        row[iprec]      = prec;
        row[ilength]    = len;
        row[iscale]     = scale;
        row[iradix]     = radix;
        row[inullable]  = nullability;
        row[iremark]    = remark;
        row[isn]        = specificName;
        row[iseq]       = sequence;

        t.insert(row);

        if (l != null) {
            int size = l.size();

            for (int i = 0; i < size; i++) {
                row             = t.getNewRow();
                pName           = (String) l.get(i);
                row[icat]       = cat;
                row[ischem]     = schem;
                row[iname]      = pName;
                row[icol_name]  = cName;
                row[icol_type]  = cType;
                row[idata_type] = dType;
                row[itype_name] = tName;
                row[iprec]      = prec;
                row[ilength]    = len;
                row[iscale]     = scale;
                row[iradix]     = radix;
                row[inullable]  = nullability;
                row[iremark]    = remark;
                row[isn]        = specificName;
                row[iseq]       = sequence;

                t.insert(row);
            }
        }
    }

    /**
     * Inserts a set of procedure description rows into the <code>Table</code>
     * object specified by the <code>t</code> argument. <p>
     *
     * @param t the table into which the specified rows will eventually
     *      be inserted
     * @param l the list of procedure name aliases to which the specified column
     *      values apply
     * @param cat the procedure catalog name
     * @param schem the procedure schema name
     * @param pName the base (non-alias) procedure name
     * @param ip the procedure input parameter count
     * @param op the procedure output parameter count
     * @param rs the procedure result column count
     * @param remark a human-readable remark regarding the procedure
     * @param pType the procedure type code, indicating whether it is a
     *      function, procedure, or uncatagorized (i.e. returns
     *      a value, does not return a value, or it is unknown
     *      if it returns a value)
     * @param origin origin of the procedure, e.g.
     *      (["BUILTIN" | "USER DEFINED"] "ROUTINE" | "TRIGGER") | "ALIAS", etc.
     * @param specificName the specific name of the procedure
     *      (typically but not limited to a
     *      fully qualified Java Method name and signature)
     * @throws HsqlException if there is problem inserting the specified rows
     *      in the table
     *
     */
    protected void addProcRows(Table t, HsqlArrayList l, String cat,
                               String schem, String pName, Integer ip,
                               Integer op, Integer rs, String remark,
                               Integer pType, String origin,
                               String specificName) throws HsqlException {

        // column number mappings
        final int icat          = 0;
        final int ischem        = 1;
        final int ipname        = 2;
        final int iinput_parms  = 3;
        final int ioutput_parms = 4;
        final int iresult_sets  = 5;
        final int iremark       = 6;
        final int iptype        = 7;
        final int iporigin      = 8;
        final int isn           = 9;
        Object[]  row           = t.getNewRow();

        row[icat]          = cat;
        row[ischem]        = schem;
        row[ipname]        = pName;
        row[iinput_parms]  = ip;
        row[ioutput_parms] = op;
        row[iresult_sets]  = rs;
        row[iremark]       = remark;
        row[iptype]        = pType;
        row[iporigin]      = origin;
        row[isn]           = specificName;

        t.insert(row);

        if (l != null) {
            int size = l.size();

            for (int i = 0; i < size; i++) {
                row                = t.getNewRow();
                pName              = (String) l.get(i);
                row[icat]          = cat;
                row[ischem]        = schem;
                row[ipname]        = pName;
                row[iinput_parms]  = ip;
                row[ioutput_parms] = op;
                row[iresult_sets]  = rs;
                row[iremark]       = remark;
                row[iptype]        = pType;
                row[iporigin]      = "ALIAS";
                row[isn]           = specificName;

                t.insert(row);
            }
        }
    }
}
