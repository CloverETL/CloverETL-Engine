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

import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.HsqlNameManager.HsqlName;

// fredt@users 20021103 - patch 1.7.2 - fix bug in revokeAll()
// fredt@users 20021103 - patch 1.7.2 - allow for drop table, etc.
// when tables are dropped or renamed, changes are reflected in the
// permissions held in User objects.
// boucherb@users 200208-200212 - doc 1.7.2 - update
// boucherb@users 200208-200212 - patch 1.7.2 - metadata

/**
 * A User Object holds the name, password, role and access rights for a
 * particular database user.<p>
 * It supplies the methods used to grant, revoke, test
 * and check a user's access rights to other database objects.
 * It also holds a reference to the common PUBLIC User Object,
 * which represent the special user refered to in
 * GRANT ... TO PUBLIC statements.<p>
 * The check(), isAccessible() and getGrantedClassNames() methods check the
 * rights granted to the PUBLIC User Object, in addition to individually
 * granted rights, in order to decide which rights exist for the user.
 * @version 1.7.2
 */
class User {

    /** true if this user has database administrator role. */
    private boolean isAdministrator;

    /** true if this user is the sys user. */
    private boolean isSys;

    /** true if this user is the public user. */
    private boolean isPublic;

    /** map with database object identifier keys and access privileges values */
    private IntValueHashMap rightsMap;

    /** user name. */
    private String sName;

    /** password. */
    private String sPassword;

    /**
     * A reference to the common, PUBLIC User object held by UserManager.
     * For the special PUBLIC and SYS user objects, this attribute is null.
     */
    private User uPublic;

    /**
     * Constructor, with a argument reference to the PUBLIC User Object which
     * is null if this is the SYS or PUBLIC user.
     */
    User(String name, String password, boolean admin, User pub) {

        rightsMap = new IntValueHashMap();
        sName     = name;

        setPassword(password);

        isAdministrator = admin;
        isSys           = name.equals(UserManager.SYS_USER_NAME);
        isPublic        = name.equals(UserManager.PUBLIC_USER_NAME);
        uPublic         = pub;
    }

    String getName() {
        return sName;
    }

    String getPassword() {

        // necessary to create the script
        return sPassword;
    }

    /**
     * Retrieves the map object that represents the rights that have been
     * granted on database objects.  <p>
     *
     * The map has keys and values with the following interpretation: <P>
     *
     * <UL>
     * <LI> The keys are generally (but not limited to) objects having
     *      an attribute or value equal to the name of an actual database
     *      object.
     *
     * <LI> Specifically, the keys act as database object identifiers.
     *
     * <LI> The values are always Integer objects, each formed by combining
     *      a set of flags, one for each of the access rights defined in
     *      UserManager: {SELECT, INSERT, UPDATE and DELETE}.
     * </UL>
     */
    IntValueHashMap getRights() {

        // necessary to create the script
        return rightsMap;
    }

    void setPassword(String password) {

        // TODO:
        // checkComplexity(password);
        // requires: UserManager.createSAUser(), UserManager.createPublicUser()
        sPassword = password;
    }

    /**
     * Checks if this object's password attibute equals
     * specified argument, else throws.
     */
    void checkPassword(String test) throws HsqlException {
        Trace.check(test.equals(sPassword), Trace.ACCESS_IS_DENIED);
    }

    /**
     * Grants the specified rights on the specified database object. <p>
     *
     * Keys stored in rightsMap for database tables are their HsqlName
     * attribute. This allows rights to persist when a table is renamed. <p>
     */
    void grant(Object dbobject, int rights) {

        if (rights == 0) {
            return;
        }

        int n = rightsMap.get(dbobject, 0);

        n |= rights;

        rightsMap.put(dbobject, n);
    }

    /**
     * Revokes the specified rights on the specified database object. <p>
     *
     * If, after removing the specified rights, no rights remain on the
     * database object, then the key/value pair for that object is removed
     * from the rights map
     */
    void revoke(Object dbobject, int rights) {

        if (rights == 0) {
            return;
        }

        int n = rightsMap.get(dbobject, 0);

        if (n == 0) {
            return;
        }

        rights = n & (UserManager.ALL - rights);

        if (rights == 0) {
            rightsMap.remove(dbobject);
        } else {
            rightsMap.put(dbobject, rights);
        }
    }

    /**
     * Revokes all rights on the specified database object.<p>
     *
     * This method removes any existing mapping from the rights map
     */
    void revokeDbObject(Object dbobject) {
        rightsMap.remove(dbobject);
    }

    /**
     * Revokes all rights from this User object.  The map is cleared and
     * the database administrator role attribute is set false.
     */
    void revokeAll() {

        rightsMap.clear();

        isAdministrator = false;
    }

    /**
     * Checks if any of the rights represented by the rights
     * argument have been granted on the specified database object. <p>
     *
     * This is done by checking that a mapping exists in the rights map
     * from the dbobject argument for at least one of the rights
     * contained in the rights argument. Otherwise, it throws.
     */
    void check(Object dbobject, int rights) throws HsqlException {

        if (!isAccessible(dbobject, rights)) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }
    }

    /**
     * Returns true if any of the rights represented by the
     * rights argument has been granted on the database object identified
     * by the dbobject argument. <p>
     *
     * This is done by checking that a mapping exists in the rights map
     * from the dbobject argument for at least one of the rights
     * contained in the rights argument.
     */
    boolean isAccessible(Object dbobject, int rights) {

        if (isAdministrator) {
            return true;
        }

        if (dbobject instanceof String) {
            if (((String) dbobject).startsWith("org.hsqldb.Library")
                    || ((String) dbobject).startsWith("java.lang.Math")) {
                return true;
            }
        }

        int n = rightsMap.get(dbobject, 0);

        if (n != 0) {
            return (n & rights) != 0;
        }

        return (uPublic == null) ? false
                                 : uPublic.isAccessible(dbobject, rights);
    }

    /**
     * Returns true if any right at all has been granted to this User object
     * on the database object identified by the dbobject argument.
     */
    boolean isAccessible(Object dbobject) {
        return isAccessible(dbobject, UserManager.ALL);
    }

    /**
     * Checks that this User object is for a user with the
     * database administrator role. Otherwise it throws.
     */
    void checkAdmin() throws HsqlException {
        Trace.check(isAdmin(), Trace.ACCESS_IS_DENIED);
    }

    /**
     * Returns true if this User object is for a user with the
     * database administrator role.
     */
    boolean isAdmin() {
        return isAdministrator;
    }

    /**
     * Returns true if this User object is for a user with the
     * database administrator role.
     */
    boolean isSys() {
        return isSys;
    }

    /**
     * Returns true if this User object represents the PUBLIC user
     */
    boolean isPublic() {
        return isPublic;
    }

    /**
     * Retrieves the distinct set of Java <code>Class</code> FQNs
     * for which this <code>User</code> object has been
     * granted <code>ALL</code> (the Class execution privilege). <p>
     * @param andToPublic if <code>true</code>, then the set includes the
     *        names of classes accessible to this <code>User</code> object
     *        through grants to its <code>PUBLIC</code> <code>User</code>
     *        object attribute, else only direct grants are inlcuded.
     * @return the distinct set of Java Class FQNs for which this
     *        this <code>User</code> object has been granted
     *        <code>ALL</code>.
     * @since HSQLDB 1.7.2
     *
     */
    HashSet getGrantedClassNames(boolean andToPublic) {

        IntValueHashMap rights;
        HashSet         out;
        HashSet         pub;
        Object          key;
        int             right;
        Iterator        i;

        rights = rightsMap;
        out    = new HashSet();
        i      = rightsMap.keySet().iterator();

        while (i.hasNext()) {
            key = i.next();

            if (key instanceof String) {
                right = rights.get(key, 0);

                if (right == UserManager.ALL) {
                    out.add(key);
                }
            }
        }

        if (andToPublic && uPublic != null) {
            rights = uPublic.rightsMap;
            i      = rights.keySet().iterator();

            while (i.hasNext()) {
                key = i.next();

                if (key instanceof String) {
                    right = rights.get(key, 0);

                    if (right == UserManager.ALL) {
                        out.add(key);
                    }
                }
            }
        }

        return out;
    }

    /**
     * Retrieves a string[] whose elements are the names, of the rights
     * explicitly granted with the GRANT command to this <code>User</code>
     * object on the <code>Table</code> object identified by the
     * <code>name</code> argument.
     * * @return array of Strings naming the rights granted to this
     *        <code>User</code> object on the <code>Table</code> object
     *        identified by the <code>name</code> argument.
     * @param name a <code>Table</code> object identifier
     * @since HSQLDB 1.7.2
     *
     */
    String[] listGrantedTablePrivileges(HsqlName name) {
        return UserManager.getRightsArray(rightsMap.get(name, 0));
    }

    /**
     * Returns the ALTER USER DDL character sequence that preserves the
     * this user's current password value and mode. <p>
     *
     * @return  the DDL
     */
    String getAlterUserDDL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Token.T_ALTER).append(' ');
        sb.append(Token.T_USER).append(' ');
        sb.append(sName).append(' ');
        sb.append(Token.T_SET).append(' ');
        sb.append(Token.T_PASSWORD).append(' ');
        sb.append('"').append(sPassword).append('"');

        return sb.toString();
    }

    /**
     * Appends to the specified StringBuffer the DDL character
     * sequence that creates this user.
     *
     */
    String getCreateUserDDL() {

        StringBuffer sb = new StringBuffer(64);

        sb.append(Token.T_CREATE).append(' ');
        sb.append(Token.T_USER).append(' ');
        sb.append(sName).append(' ');
        sb.append(Token.T_PASSWORD).append(' ');
        sb.append('"').append(sPassword).append('"');

        if (isAdministrator) {
            sb.append(' ').append(Token.T_ADMIN);
        }

        return sb.toString();
    }

    /**
     * Retrieves the redo log character sequence for connecting
     * this user
     *
     * @return the redo log character sequence for connecting
     *      this user
     */
    String getConnectStatement() {

        StringBuffer sb = new StringBuffer();

        sb.append(Token.T_CONNECT).append(' ');
        sb.append(Token.T_USER).append(' ');
        sb.append(sName);

        return sb.toString();
    }
}
