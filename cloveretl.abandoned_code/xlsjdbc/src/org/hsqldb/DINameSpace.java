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
import java.lang.reflect.Modifier;

import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.HsqlNameManager.HsqlName;

// boucherb@users - 2004xxxx - patch 1.7.2
// -- canonical database uri for catalog name reporting
// -- enumXXX methods to iterateXXX
// -- simple support for SEQUENCE schema reporting
// -- report built-in procedures/procedure columns without dependency on user grants;

/**
 * Provides catalog and schema related definitions and functionality. <p>
 *
 * Additional features include accessibility tests, class loading, filtered
 * iteration and inverted alias mapping functionality regarding Java Classes
 * and Methods defined within the context of this database name space support
 * object. <p>
 *
 * @author  boucherb@users
 * @version 1.7.2
 * @since HSQLDB 1.7.2
 */
final class DINameSpace {

    /** The Database for which the name space functionality is provided */
    private Database database;

    /** The catalog name reported by this namespace */
    private String catalogName;

    /**
     * Set { <code>Class</code> FQN <code>String</code> objects }. <p>
     *
     * The Set contains the names of the classes providing the public static
     * methods that are automatically made accessible to the PUBLIC user in
     * support of the expected SQL CLI scalar functions and other core
     * HSQLDB SQL functions and stored procedures. <p>
     */
    private static HashSet builtin = new HashSet();

    /** The <code>DEFINITION_SCHEMA</code> schema name. */
    static final String DEFN_SCHEMA = "DEFINITION_SCHEMA";

    /**
     * The <code>DEFINITION_SCHEMA</code> schema name plus the schema
     * separator character.
     */
    private static final String DEFN_SCHEMA_DOT = DEFN_SCHEMA + ".";

    /** Length of <code>DEFN_SCHEMA_DOT</code>. */
    private static final int DEFN_SCHEMA_DOT_LEN = DEFN_SCHEMA_DOT.length();

    /** The <code>INFORMATION_SCHEMA</code> schema name. */
    static final String INFO_SCHEMA = "INFORMATION_SCHEMA";

    /**
     * The <code>INFORMATION_SCHEMA</code> schema name plus the schema
     * separator character.
     */
    private static final String INFO_SCHEMA_DOT = INFO_SCHEMA + ".";

    /** Length of <code>INFO_SCHEMA_DOT</code>. */
    private static final int INFO_SCHEMA_DOT_LEN = INFO_SCHEMA_DOT.length();

    /** The <code>PUBLIC</code> schema name. */
    static final String PUB_SCHEMA = UserManager.PUBLIC_USER_NAME;

    /**
     * The <code>PUBLIC</code> schema name plus the schema
     * separator character.
     */
    private static final String PUB_SCHEMA_DOT = PUB_SCHEMA + ".";

    /** Length of <code>PUB_SCHEMA_DOT</code>. */
    private static final int PUB_SCHEMA_DOT_LEN = PUB_SCHEMA_DOT.length();

    /**
     * List of system schema names:
     * { DEFINITION_SCHEMA, INFORMATION_SCHEMA, PUBLIC }
     */
    private static final HsqlArrayList sysSchemas = new HsqlArrayList();

    // procedure columns
    // make temporary ad-hoc spec a little more "official"
    // until better system in place
    static {
        sysSchemas.add(DEFN_SCHEMA);
        sysSchemas.add(INFO_SCHEMA);
        sysSchemas.add(PUB_SCHEMA);
        builtin.add("org.hsqldb.Library");
        builtin.add("java.lang.Math");
    }

    /**
     * Constructs a new name space support object for the
     * specified Database object. <p>
     *
     * @param database The Database object for which to provide name
     *      space support
     * @throws HsqlException if a database access error occurs
     */
    public DINameSpace(Database database) throws HsqlException {

        try {
            this.database    = database;
            this.catalogName = database.getURI();
        } catch (Exception e) {
            Trace.throwerror(Trace.GENERAL_ERROR, e.toString());
        }
    }

    /**
     * Retrieves the declaring <code>Class</code> object for the specified
     * fully qualified method name, using (if possible) the classLoader
     * attribute of this object's database. <p>
     *
     * @param fqn the fully qualified name of the method for which to
     *        retrieve the declaring <code>Class</code> object.
     * @return the declaring <code>Class</code> object for the
     *        specified fully qualified method name
     */
    Class classForMethodFQN(String fqn) {

        try {
            return classForName(fqn.substring(0, fqn.lastIndexOf('.')));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Retrieves the <code>Class</code> object specified by the
     * <code>name</code> argument, using, if possible, the
     * classLoader attribute of the database. <p>
     *
     * @param name the fully qualified name of the <code>Class</code>
     *      object to retrieve.
     * @throws ClassNotFoundException if the specified class object
     *      cannot be found in the context of this name space
     * @return the <code>Class</code> object specified by the
     *      <code>name</code> argument
     */
    Class classForName(String name) throws ClassNotFoundException {

        try {
            if (database.classLoader == null) {
                return Class.forName(name);
            } else {
                if (name != null) {
                    return database.classLoader.loadClass(name);
                } else {
                    throw new ClassNotFoundException();
                }
            }
        } catch (NoClassDefFoundError err) {
            throw new ClassNotFoundException(err.toString());
        }
    }

    /**
     * Retrieves an <code>Iterator</code> whose elements form the set of
     * distinct names of all visible catalogs, relative to this object's
     * database. <p>
     *
     * If catalog reporting is turned off, then the empty Iterator is
     * returned. <p>
     *
     * <b>Note:</b> in the present implementation, if catalog reporting is
     * turned on, then the iteration consists of a single element that is the
     * uri of this object's database; HSQLDB  currently does not support the
     * concept a single engine hosting multiple catalogs. <p>
     *
     * @return An Iterator whose elements are <code>String</code> objects
     *      naming all visible catalogs, relative to this object's database.
     * @throws HsqlException never (reserved for future use)
     */
    Iterator iterateCatalogNames() throws HsqlException {
        return isReportCatalogs() ? new WrapperIterator(catalogName)
                                  : new WrapperIterator();
    }

    /**
     * Retrieves an <code>Iterator</code> object whose elements form the set
     * of distinct names of system schemas visible in this object's
     * database. <p>
     *
     * If schema reporting is turned off, then the empty Iterator is
     * returned. <p>
     *
     * @return An <code>Iterator</code> whose elements are <code>String</code>
     *      objects naming the system schemas
     * @throws HsqlException never (reserved for future use)
     */
    Iterator iterateSysSchemaNames() throws HsqlException {
        return isReportSchemas() ? sysSchemas.iterator()
                                 : new WrapperIterator();
    }

    /**
     * Retrieves an an <code>Iterator</code> object whose elements form the set
     * of schema names visible in the context of the specified session. <p>
     *
     * If schema reporting is turned off or a null session is specified,
     * then the empty Iterator is returned. <p>
     *
     * @return An <code>Iterator</code> object whose elements are
     *      <code>Strings</code> naming the schemas visible to the
     *      specified session
     * @param session The context in which to provide the iteration
     * @throws HsqlException if a database access error occurs
     */
    Iterator iterateVisibleSchemaNames(Session session) throws HsqlException {

        HsqlArrayList users;
        HsqlArrayList userNames;
        UserManager   userManager;

        if (!isReportSchemas() || session == null) {
            return new WrapperIterator();
        }

        userManager = database.getUserManager();
        users       = userManager.listVisibleUsers(session, false);
        userNames   = new HsqlArrayList();

        for (int i = 0; i < users.size(); i++) {
            User u = (User) users.get(i);

            userNames.add(u.getName());
        }

        return new WrapperIterator(iterateSysSchemaNames(),
                                   userNames.iterator());
    }

    /**
     * Retrieves the one-and-only correct <code>HsqlName</code> instance
     * relative to the specified map, using the s argument as a key to
     * look up the instance. <p>
     *
     * @param s the lookup key
     * @param map the <code>HsqlName</code> instance repository
     * @return the one-and-only correct <code>HsqlName</code> instance
     *      for the specified key, <code>s</code>, relative to the
     *      specified map
     * @see HsqlName
     */
    HsqlName findOrCreateHsqlName(String s, HashMap map) {

        HsqlName name = (HsqlName) map.get(s);

        if (name == null) {
            try {
                name = database.nameManager.newHsqlName(s, false);

                map.put(s, name);
            } catch (Exception e) {}
        }

        return name;
    }

    /**
     * Finds the regular (non-temp, non-system) table or view (if any)
     * corresponding to the given database object identifier, relative to
     * this object's database.<p>
     *
     * Basically, the PUBLIC schema name, in the form of a schema qualifier,
     * is removed from the specified database object identifier and then the
     * usual process for finding a non-temp, non-system table or view is
     * performed using the resulting simple identifier. <p>
     *
     * @return the non-temp, non-system user table or view object (if any)
     *      corresponding to the given name.
     * @param name a database object identifier string representing the
     *      table/view object to find, possibly prefixed
     *      with the PUBLIC schema qualifier
     */
    Table findPubSchemaTable(String name) {

        return (!isReportSchemas() || name == null ||!name.startsWith(PUB_SCHEMA_DOT))
               ? null
               : database.findUserTable(name.substring(PUB_SCHEMA_DOT_LEN));
    }

    /**
     * Finds a TEMP [TEXT] table (if any) corresponding to
     * the given database object identifier, relative to the
     * this object's database and the specified session. <p>
     *
     * @return the TEMP [TEXT] table (if any) corresponding to
     *      the given database object identifier, relative to
     *      this object's database and the he specified session.
     * @param session The context in which to find the table
     * @param name a database object identifier string representing the
     *      table to find, possibly prefixed with a schema qualifier
     */
    Table findUserSchemaTable(Session session, String name) {

        String prefix;

        if (!isReportSchemas() || name == null || session == null) {
            return null;
        }

        // PRE:  we assume user name is never null or ""
        prefix = session.getUsername() + ".";

        return name.startsWith(prefix)
               ? database.findUserTable(session,
                                        name.substring(prefix.length()))
               : null;
    }

    /**
     * Retrieves the name of the catalog corresponding to the indicated
     * object. <p>
     *
     * <B>Note:</B> the uri of this object's database is returned whenever
     * catalog reporting is turned on. <p>
     *
     * This a stub that will be used until such time (if ever) that the
     * engine actually supports the concept of multiple hosted
     * catalogs. <p>
     *
     * @return the name of specified object's qualifying catalog, or null if
     *      catalog reporting is turned off.
     * @param o the object for which the name of its qualifying catalog
     *      is to be retrieved
     */
    String getCatalogName(Object o) {
        return isReportCatalogs() ? catalogName
                                  : null;
    }

    /**
     * Retrieves a map from each distinct value of this object's database
     * SQL routine CALL alias map to the list of keys in the input map
     * mapping to that value. <p>
     *
     * @return The requested map
     */
    HashMap getInverseAliasMap() {

        HashMap       mapIn;
        HashMap       mapOut;
        Iterator      keys;
        Object        key;
        Object        value;
        HsqlArrayList keyList;

        // TODO:
        // update Database to dynamically maintain its own
        // inverse alias map.  This will make things *much*
        // faster for our  purposes here, without appreciably
        // slowing down Database
        mapIn  = database.getAliasMap();
        mapOut = new HashMap();
        keys   = mapIn.keySet().iterator();

        while (keys.hasNext()) {
            key     = keys.next();
            value   = mapIn.get(key);
            keyList = (HsqlArrayList) mapOut.get(value);

            if (keyList == null) {
                keyList = new HsqlArrayList();

                mapOut.put(value, keyList);
            }

            keyList.add(key);
        }

        return mapOut;
    }

    /**
     * Retrieves the fully qualified name of the given Method object. <p>
     *
     * @param m The Method object for which to retreive the fully
     *      qualified name
     * @return the fully qualified name of the specified Method object.
     */
    static String getMethodFQN(Method m) {

        return m == null ? null
                         : m.getDeclaringClass().getName() + '.'
                           + m.getName();
    }

    /**
     * Retrieves the specific name of the given Method object. <p>
     *
     * @param m The Method object for which to retreive the specific name
     * @return the specific name of the specified Method object.
     */
    static String getMethodSpecificName(Method m) {

        return m == null ? null
                         : m.getDeclaringClass().getName() + '.'
                           + getSignature(m);
    }

    static String getSignature(Method method) {

        StringBuffer sb;
        String       signature;
        Class[]      parmTypes;
        int          len;
        int          last;

        sb        = new StringBuffer();
        parmTypes = method.getParameterTypes();
        len       = parmTypes.length;
        last      = len - 1;

        sb.append(method.getName()).append('(');

        for (int i = 0; i < len; i++) {
            sb.append(parmTypes[i].getName());

            if (i < last) {
                sb.append(',');
            }
        }

        sb.append(')');

        signature = sb.toString();

        return signature;
    }

    /**
     * Retrieves the name of the schema corresponding to the indicated object,
     * in the context of this name space. <p>
     *
     * The current implementation makes the determination as follows: <p>
     *
     * <OL>
     * <LI> if schema reporting is turned off, then null is returned
     *      immediately.
     *
     * <LI> if the specifed object is <code>null</code>, then <code>null</code>
     *      is returned immediately.
     *
     * <LI> if the specified object is an <code>org.hsqldb.NumberSequence</code>
     *      instance, then it represents a SEQUENCE object and "PUBLIC" is
     *      returned immediately.
     *
     * <LI> if the specified object is an <code>org.hsqldb.Table</code>
     *      instance and it is a system table, then "DEFINITION_SCHEMA" is
     *      returned.
     *
     * <LI> if the specified object is an <code>org.hsqldb.Table</code>
     *      instance and is a system view, then "INFORMATION_SCHEMA" is
     *      returned.
     *
     * <LI> if the specified object is an <code>org.hsqldb.Table</code>
     *      instance and it is a temp table, then either the name of the
     *      owning session user is returned, or null is returned if the owning
     *      session cannot be found in the context of this name space.
     *
     * <LI> if the specified object is an <code>org.hsqldb.Table</code>
     *      instance and it is has not been covered by any of the previous
     *      cases, then it is assumed to be a regular user-defined table
     *      and "PUBLIC" is returned.
     *
     * <LI> if the specified object is an <code>org.hsqldb.Index</code>
     *      instance, then either the name of the schema of the table
     *      containing the index is returned, or null is returned if no table
     *      containing the index object can be found in the context of this
     *      name space.
     *
     * <LI> if the specified object is a String instance, then it is checked to
     *      see if it names a built in DOMAIN or Class.  If it does, then
     *      "DEFINITION_SCHEMA" is returned.  If it does not, then an attempt
     *      is made to retrieve a Class object named by the string.  If the
     *      string names a Class accessible within this name space, then the
     *      corresponding Class object is passed on to the next step.
     *
     * <LI> if the specified object is a Method or Class instance,
     *      then "DEFINITION_SCHEMA" is returned if the object can be
     *      classified as builtin (made available automatically by the engine).
     *      Otherwise, "PUBLIC" is returned, indicating a user-defined database
     *      object.
     *
     * <LI> if none of the above points are satisfied, null is returned.
     *
     * </OL> <p>
     *
     * @return the name of the schema qualifying the specified object, or null
     *      if schema reporting is turned off or the specified object is null
     *      or cannot be qualified.
     * @param o the object for which the name of its qualifying schema is to
     *      be retrieved
     */
    String getSchemaName(Object o) {

        Class c;
        Table table;

        if (o == null ||!isReportSchemas()) {
            return null;
        }

        if (o instanceof NumberSequence) {
            return PUB_SCHEMA;
        }

        if (o instanceof Table) {
            return ((Table) o).getSchemaName();
        }

        if (o instanceof Index) {
            table = tableForIndex((Index) o);

            return (table == null) ? null
                                   : table.getSchemaName();
        }

        if (o instanceof String) {

            // maybe the name of a DOMAIN?
            if (Types.typeAliases.get(o, Integer.MIN_VALUE)
                    != Integer.MIN_VALUE) {
                return DEFN_SCHEMA;
            }

            // ----------
            // Class name?
            if (isBuiltin((String) o)) {
                return DEFN_SCHEMA;
            }

            try {
                o = classForName((String) o);
            } catch (Exception e) {
                return null;
            }

            // ----------
        }

        c = null;

        if (o instanceof Method) {
            c = ((Method) o).getDeclaringClass();
        } else if (o instanceof Class) {
            c = (Class) o;
        }

        return (c == null) ? null
                           : isBuiltin(c) ? DEFN_SCHEMA
                                          : PUB_SCHEMA;
    }

    /**
     * Adds to the given Set the fully qualified names of the Class objects
     * internally granted to PUBLIC in support of core operation.
     *
     * @param the HashSet to which to add the fully qualified names of
     * the Class objects internally granted to PUBLIC in support of
     * core operation.
     */
    void addBuiltinToSet(HashSet set) {
        set.addAll(builtin.toArray(new String[builtin.size()]));
    }

    /**
     * Retrieves whether the indicated Class object is systematically
     * granted to PUBLIC in support of core operation. <p>
     *
     * @return whether the indicated Class object is systematically
     * granted to PUBLIC in support of core operation
     * @param clazz The Class object for which to make the determination
     */
    boolean isBuiltin(Class clazz) {
        return clazz == null ? false
                             : builtin.contains(clazz.getName());
    }

    /**
     * Retrieves whether the Class object indicated by the fully qualified
     * class name is systematically granted to PUBLIC in support of
     * core operation. <p>
     *
     * @return true if system makes grant, else false
     * @param name fully qualified name of a Class
     */
    boolean isBuiltin(String name) {
        return (name == null) ? false
                              : builtin.contains(name);
    }

    /**
     * Retrieves the Table object enclosing the specified Index object. <p>
     *
     * @return the Table object enclosing the specified Index
     *        object or null if no such Table exists
     *        in the context of this name space
     * @param index The index object for which to perform the search
     */
    Table tableForIndex(Index index) {
        return index == null ? null
                             : tableForIndexName(index.getName().name);
    }

    /**
     * Retrieves the Table object enclosing the Index object with the
     * specified name. <p>
     *
     * @param indexName The name if the Index object for which to
     *        perform the search
     * @return the Table object enclosing the specified Index
     *        object or null if no such Table exists
     *        in the context of this name space
     */
    Table tableForIndexName(String indexName) {

        HsqlName tableName = database.indexNameList.getOwner(indexName);

        return database.findUserTable(tableName.name);
    }

    /**
     * Retrieves the specified database object name, with the catalog
     * qualifier removed. <p>
     *
     * @param name the database object name from which to remove
     *        the catalog qualifier
     * @return the specified database object name, with the
     *        catalog qualifier removed
     */
    String withoutCatalog(String name) {

        if (!isReportCatalogs()) {
            return name;
        }

        String cat_dot = getCatalogName(name) + ".";
        String out;

        if (name.startsWith(cat_dot)) {
            out = name.substring(cat_dot.length());
        } else {
            out = name;
        }

        return out;
    }

    /**
     * Retrieves the specified database object name, with the
     * DEFINTION_SCHEMA qualifier removed. <p>
     *
     * @param name the database object name from which to remove
     *        the schema qualifier
     * @return the specified database object name, with the
     *        schema qualifier removed
     */
    String withoutDefnSchema(String name) {

        return isReportSchemas() && name.startsWith(DEFN_SCHEMA_DOT)
               ? name.substring(DEFN_SCHEMA_DOT_LEN)
               : name;
    }

    /**
     * Retrieves the specified database object name, with the
     * INFORMATION_SCHEMA qualifier removed. <p>
     *
     * @param name the database object name from which to remove
     *        the schema qualifier
     * @return the specified database object name, with the
     *        schema qualifier removed
     */
    String withoutInfoSchema(String name) {

        return isReportSchemas() && name.startsWith(INFO_SCHEMA_DOT)
               ? name.substring(INFO_SCHEMA_DOT_LEN)
               : name;
    }

    /**
     * Retrieves an <code>Iterator</code> object describing the Java
     * <code>Method</code> objects that are both the entry points
     * to executable SQL database objects (such as SQL functions and
     * stored procedures) within the context of this name space. <p>
     *
     * Each element of the <code>Iterator</code> is an Object[3] array
     * whose elements are: <p>
     *
     * <ol>
     * <li>a <code>Method</code> object.
     * <li>an <code>HsqlArrayList</code> object whose elements are the SQL call
     *     aliases for the method.
     * <li>the <code>String</code> "ROUTINE"
     * </ol>
     *
     * <b>Note:</b> Admin users are actually free to invoke *any* public
     * static non-abstract Java Method that can be found through the database
     * class loading process, either as a SQL stored procedure or SQL function,
     * as long as its parameters and return type are compatible with the
     * engine's supported SQL type / Java <code>Class</code> mappings. <p>
     *
     * @return An <code>Iterator</code> object whose elements form the set
     *        of distinct <code>Method</code> objects accessible as
     *        executable as SQL routines within the current execution
     *        context.<p>
     *
     *        Elements are <code>Object[3]</code> instances, with [0] being a
     *        <code>Method</code> object, [1] being an alias list object and
     *        [2] being the <code>String</code> "ROUTINE"<p>
     *
     *        If the <code>Method</code> object at index [0] has aliases,
     *        and the <code>andAliases</code> parameter is specified
     *        as <code>true</code>, then there is an HsqlArrayList
     *        at index [1] whose elements are <code>String</code> objects
     *        whose values are the SQL call aliases for the method.
     *        Otherwise, the value of index [1] is <code>null</code>.
     * @param className The fully qualified name of the class for which to
     *        retrieve the iteration
     * @param andAliases if <code>true</code>, alias lists for qualifying
     *        methods are additionally retrieved.
     * @throws HsqlException if a database access error occurs
     *
     */
    Iterator iterateRoutineMethods(String className,
                                   boolean andAliases) throws HsqlException {

        Class         clazz;
        Method[]      methods;
        Method        method;
        int           mods;
        Object[]      info;
        HsqlArrayList aliasList;
        HsqlArrayList methodList;
        HashMap       invAliasMap;

        try {
            clazz = classForName(className);
        } catch (ClassNotFoundException e) {
            return new WrapperIterator();
        }

        invAliasMap = andAliases ? getInverseAliasMap()
                                 : null;

        // we are interested in inherited methods too,
        // so we use getDeclaredMethods() first.
        // However, under Applet execution or
        // under restrictive SecurityManager policies
        // this may fail, so we use getMethods()
        // if getDeclaredMethods() fails.
        try {
            methods = clazz.getDeclaredMethods();
        } catch (Exception e) {
            methods = clazz.getMethods();
        }

        methodList = new HsqlArrayList(methods.length);

        // add all public static methods to the set
        for (int i = 0; i < methods.length; i++) {
            method = methods[i];
            mods   = method.getModifiers();

            if (!(Modifier.isPublic(mods) && Modifier.isStatic(mods))) {
                continue;
            }

            info = new Object[] {
                method, null, "ROUTINE"
            };

            if (andAliases) {
                info[1] = invAliasMap.get(getMethodFQN(method));
            }

            methodList.add(info);
        }

        // return the iterator
        return methodList.iterator();
    }

    /**
     * Retrieves an <code>Iterator</code> object describing the
     * fully qualified names of all Java <code>Class</code> objects
     * that are both trigger body implementations and that are accessible
     * (whose fire method can potentially be invoked) by actions upon this
     * object's database by the specified <code>User</code>. <p>
     *
     * @param user the <code>User</code> for which to retrieve the
     *      <code>Iterator</code>
     * @throws HsqlException if a database access error occurs
     * @return an <code>Iterator</code> object describing the
     *        fully qualified names of all Java <code>Class</code>
     *        objects that are both trigger body implementations
     *        and that are accessible (whose fire method can
     *        potentially be invoked) by actions upon this object's database
     *        by the specified <code>User</code>.
     */
    Iterator iterateAccessibleTriggerClassNames(User user)
    throws HsqlException {

        Table           table;
        Class           clazz;
        HashSet         classSet;
        TriggerDef      triggerDef;
        HsqlArrayList[] triggerLists;
        HsqlArrayList   triggerList;
        HsqlArrayList   tableList;
        int             listSize;

        classSet  = new HashSet();
        tableList = database.getTables();

        for (int i = 0; i < tableList.size(); i++) {
            table = (Table) tableList.get(i);

            if (!user.isAccessible(table.getName())) {
                continue;
            }

            triggerLists = table.triggerLists;

            if (triggerLists == null) {
                continue;
            }

            for (int j = 0; j < triggerLists.length; j++) {
                triggerList = triggerLists[j];

                if (triggerList == null) {
                    continue;
                }

                listSize = triggerList.size();

                for (int k = 0; k < listSize; k++) {
                    triggerDef = (TriggerDef) triggerList.get(k);

                    if (triggerDef == null ||!triggerDef.valid
                            || triggerDef.trig == null
                            ||!user.isAccessible(
                                table, TriggerDef.indexToRight(k))) {
                        continue;
                    }

                    classSet.add(triggerDef.trig.getClass().getName());
                }
            }
        }

        return classSet.iterator();
    }

    /**
     * Retrieves an <code>Iterator</code> object describing the distinct
     * Java <code>Method</code> objects that are both the entry points
     * to trigger body implementations and that are accessible (can potentially
     * be fired) within the execution context of User currently
     * represented by the specified session. <p>
     *
     * The elements of the Iterator have the same format as those for
     * {@link #iterateRoutineMethods}, except that position [1] of each
     * Object[] element is always null (there are no aliases for trigger bodies)
     * and position [2] is always "TRIGGER". <p>
     * @return an <code>Iterator</code> object describing the Java
     *      <code>Method</code> objects that are both the entry points
     *      to trigger body implementations and that are accessible (can
     *      potentially be fired) within the execution context of User
     *      currently represented by the specified session.
     * @param session The context in which to produce the iteration
     * @throws HsqlException if a database access error occurs.
     */
    Iterator iterateAccessibleTriggerMethods(Session session)
    throws HsqlException {

        Table           table;
        Class           clazz;
        String          className;
        Method          method;
        HsqlArrayList   methodList;
        HashSet         dupCheck;
        Class[]         pTypes;
        TriggerDef      triggerDef;
        HsqlArrayList[] triggerLists;
        HsqlArrayList   triggerList;
        HsqlArrayList   tableList;
        int             listSize;

        pTypes     = new Class[] {
            Integer.TYPE,      // trigger type
            String.class,      // trigger name
            String.class,      // table name
            Object[].class,    // old row
            Object[].class     // new row
        };
        methodList = new HsqlArrayList();
        tableList  = database.getTables();
        dupCheck   = new HashSet();

        for (int i = 0; i < tableList.size(); i++) {
            table = (Table) tableList.get(i);

            if (!session.isAccessible(table.getName())) {
                continue;
            }

            triggerLists = table.triggerLists;

            if (triggerLists == null) {
                continue;
            }

            for (int j = 0; j < triggerLists.length; j++) {
                triggerList = triggerLists[j];

                if (triggerList == null) {
                    continue;
                }

                listSize = triggerList.size();

                for (int k = 0; k < listSize; k++) {
                    try {
                        triggerDef = (TriggerDef) triggerList.get(k);

                        if (triggerDef == null) {
                            continue;
                        }

                        clazz     = triggerDef.trig.getClass();
                        className = clazz.getName();

                        if (dupCheck.contains(className)) {
                            continue;
                        } else {
                            dupCheck.add(className);
                        }

                        method = clazz.getMethod("fire", pTypes);

                        methodList.add(new Object[] {
                            method, null, "TRIGGER"
                        });
                    } catch (Exception e) {

                        //e.printStackTrace();
                    }
                }
            }
        }

        return methodList.iterator();
    }

    /**
     * Retrieves a composite <code>Iterator</code> consisting of the elements
     * from {@link #iterateRoutineMethods} for each Class granted to the
     * specified session and from {@link #iterateAccessibleTriggerMethods} for
     * the specified session. <p>
     *
     * @return a composite <code>Iterator</code> consisting of the elements
     *      from {@link #iterateRoutineMethods} and
     *      {@link #iterateAccessibleTriggerMethods}
     * @param session The context in which to produce the iterator
     * @param andAliases true if the alias lists for the "ROUTINE" type method
     *      elements are to be generated.
     * @throws HsqlException if a database access error occurs
     */
    Iterator iterateAllAccessibleMethods(Session session,
                                         boolean andAliases)
                                         throws HsqlException {

        Iterator out;
        HashSet  classNameSet;
        Iterator classNames;
        Iterator methods;
        String   className;

        out          = new WrapperIterator();
        classNameSet = session.getGrantedClassNames(true);

        addBuiltinToSet(classNameSet);

        classNames = classNameSet.iterator();

        while (classNames.hasNext()) {
            className = (String) classNames.next();
            methods   = iterateRoutineMethods(className, andAliases);
            out       = new WrapperIterator(out, methods);
        }

        return new WrapperIterator(out,
                                   iterateAccessibleTriggerMethods(session));
    }

    /**
     * Retrieves the set of distinct, visible sessions connected to this
     * object's database, as a list. <p>
     *
     * @param session The context in which to produce the list
     * @return the set of distinct, visible sessions connected
     *        to this object's database, as a list.
     */
    Session[] listVisibleSessions(Session session) {
        return database.sessionManager.getVisibleSessions(session);
    }

    /**
     * Retrieves whether this object is reporting catalog qualifiers.
     * @return true if this object is reporting catalog qualifiers, else false.
     */
    boolean isReportCatalogs() {
        return database.getProperties().isPropertyTrue("hsqldb.catalogs");
    }

    /**
     * Retrieves whether this object is reporting schema qualifiers.
     * @return true if this object is reporting schema qualifiers, else false.
     */
    boolean isReportSchemas() {
        return database.getProperties().isPropertyTrue("hsqldb.schemas");
    }
}
