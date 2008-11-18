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

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Collator;

import org.hsqldb.lib.StringConverter;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Binary;
import org.hsqldb.types.JavaObject;
import org.hsqldb.HsqlNameManager.HsqlName;

// fredt@users 20020130 - patch 491987 by jimbag@users
// fredt@users 20020320 - doc 1.7.0 - update
// fredt@users 20020401 - patch 442993 by fredt - arithmetic expressions
// to allow mixed type arithmetic expressions beginning with a narrower type
// changes applied to several lines of code and not marked separately
// consists of changes to arithmatic functions to allow promotion of
// java.lang.Number values and new functions to choose type for promotion
// fredt@users 20020401 - patch 455757 by galena@users (Michiel de Roo)
// interpretation of TINYINT as Byte instead of Short
// fredt@users 20020130 - patch 505356 by daniel_fiser@users
// use of the current locale for string comparison (instead of posix)
// turned off by default but can be applied accross the database by defining
// sql.compare_in_locale=true in database.properties file
// changes marked separately
// fredt@users 20020130 - patch 491987 by jimbag@users
// support for sql standard char and varchar. size is maintained as
// defined in the DDL and trimming and padding takes place accordingly
// modified by fredt - trimming and padding are turned off by default but
// can be applied accross the database by defining sql.enforce_size=true in
// database.properties file
// fredt@users 20020215 - patch 1.7.0 by fredt - quoted identifiers
// applied to different parts to support the sql standard for
// naming of columns and tables (use of quoted identifiers as names)
// fredt@users 20020328 - patch 1.7.0 by fredt - change REAL to Double
// fredt@users 20020402 - patch 1.7.0 by fredt - type conversions
// frequently used type conversions are done without creating temporary
// Strings to reduce execution time and garbage collection
// fredt@users 20021013 - patch 1.7.1 by fredt - type conversions
// scripting of Double.Nan and infinity values
// fredt@users 20020825 - patch 1.7.1 - ByteArray.java converted to static
// methods
// BINARY objest are now represented internally as byte[] and use the static
// methods in this class to compare or convert the byte[] objects
// fredt@users 20021110 - patch 1.7.2 - ByteArray.java removed and methods
// moved here

/**
 *  Implementation of SQL table columns as defined in DDL statements with
 *  static methods to process their values.
 *
 * @version    1.7.0
 */
public class Column {

// --------------------------------------------------
    // DDL name, size, scale, null, identity and default values
    // most variables are final but not declared so because of a bug in
    // JDK 1.1.8 compiler
    public HsqlName         columnName;
    private int             colType;
    private int             colSize;
    private int             colScale;
    private boolean         isNullable;
    private boolean         isIdentity;
    private boolean         isPrimaryKey;
    private Expression      defaultExpression;
    long                    identityStart;
    long                    identityIncrement;
    static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    static final BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);
    static final BigInteger MAX_INT = BigInteger.valueOf(Integer.MAX_VALUE);
    static final BigInteger MIN_INT = BigInteger.valueOf(Integer.MIN_VALUE);
    static final BigDecimal BIG_DECIMAL_0 = new BigDecimal(0);
    static final BigDecimal BIG_DECIMAL_1 = new BigDecimal(1);

    /**
     *  Creates a column defined in DDL statement.
     *
     * @param  name
     * @param  nullable
     * @param  type
     * @param  size
     * @param  scale
     * @param  identity
     * @param  startvalue
     * @param  increment
     * @param  primarykey
     * @param  defstring
     */
    Column(HsqlName name, boolean nullable, int type, int size, int scale,
            boolean identity, long startvalue, long increment,
            boolean primarykey,
            Expression defexpression) throws HsqlException {

        columnName        = name;
        isNullable        = nullable;
        colType           = type;
        colSize           = size;
        colScale          = scale;
        isIdentity        = identity;
        identityStart     = startvalue;
        identityIncrement = increment;
        isPrimaryKey      = primarykey;
        defaultExpression = defexpression;

        if (isIdentity) {
            if (type == Types.INTEGER) {
                if (identityStart > Integer.MAX_VALUE
                        || identityIncrement > Integer.MAX_VALUE) {
                    throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE,
                                      name.statementName);
                }
            }
        }
    }

    /**
     *  Is this the identity column in the table.
     *
     * @return boolean
     */
    boolean isIdentity() {
        return isIdentity;
    }

    /**
     *  Is column nullable.
     *
     * @return boolean
     */
    boolean isNullable() {
        return isNullable;
    }

    /**
     *  Set nullable.
     *
     */
    void setNullable(boolean value) {
        isNullable = value;
    }

    /**
     *  Is this single column primary key of the table.
     *
     * @return boolean
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     *  Set primary key.
     *
     */
    void setPrimaryKey(boolean value) {
        isPrimaryKey = value;
    }

    /**
     *  Returns default value in the session context.
     */
    Object getDefaultValue(Session session) throws HsqlException {

        return defaultExpression == null ? null
                                         : defaultExpression.getValue(session,
                                         colType);
    }

    /**
     *  Returns DDL for default value.
     */
    String getDefaultDDL() {

        String ddl = null;

        try {
            ddl = defaultExpression == null ? null
                                            : defaultExpression.getDDL();
        } catch (HsqlException e) {}

        return ddl;
    }

    /**
     *  Returns default expression for the column.
     */
    Expression getDefaultExpression() {
        return defaultExpression;
    }

    void setDefaultExpression(Expression expr) {
        defaultExpression = expr;
    }

    /**
     *  Type of the column.
     *
     * @return java.sql.Types int value for the column
     */
    int getType() {
        return colType;
    }

    int getDIType() {
        return colType == Types.VARCHAR_IGNORECASE ? Types.VARCHAR
                                                   : colType;
    }

    int getDITypeSub() {

        if (colType == Types.VARCHAR_IGNORECASE) {
            return Types.TYPE_SUB_IGNORECASE;
        }

        return Types.TYPE_SUB_DEFAULT;
    }

    /**
     *  Size of the column in DDL (0 if not defined).
     *
     * @return DDL size of column
     */
    int getSize() {
        return colSize;
    }

    /**
     *  Scale of the column in DDL (0 if not defined).
     *
     * @return DDL scale of column
     */
    int getScale() {
        return colScale;
    }

    /**
     *  Add two object of a given type
     *
     * @param  a
     * @param  b
     * @param  type
     * @return result
     * @throws  HsqlException
     */
    static Object add(Object a, Object b, int type) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.NULL :
                return null;

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad + bd));

//                return new Double(ad + bd);
            }
            case Types.VARCHAR :
            case Types.CHAR :
            case Types.LONGVARCHAR :
            case Types.VARCHAR_IGNORECASE :
                return (String) a + (String) b;

            case Types.NUMERIC :
            case Types.DECIMAL : {
                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                return abd.add(bbd);
            }
            case Types.TINYINT :
            case Types.SMALLINT :
            case Types.INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai + bi);
            }
            case Types.BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa + longb);
            }
            default :
                throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED,
                                  Types.getTypeString(type));
        }
    }

    /**
     *  Concat two objects by turning them into strings first.
     *
     * @param  a
     * @param  b
     * @return result
     * @throws  HsqlException
     */
    static Object concat(Object a, Object b) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        return convertObject(a) + convertObject(b);
    }

    /**
     *  Negate a numeric object.
     *
     * @param  a
     * @param  type
     * @return result
     * @throws  HsqlException
     */
    static Object negate(Object a, int type) throws HsqlException {

        if (a == null) {
            return null;
        }

        switch (type) {

            case Types.NULL :
                return null;

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE : {
                double ad = -((Number) a).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad));
            }
            case Types.NUMERIC :
            case Types.DECIMAL :
                return ((BigDecimal) a).negate();

            case Types.TINYINT :
            case Types.SMALLINT :
            case Types.INTEGER :
                return ValuePool.getInt(-((Number) a).intValue());

            case Types.BIGINT :
                return ValuePool.getLong(-((Number) a).longValue());

            default :
                throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED,
                                  Types.getTypeString(type));
        }
    }

    /**
     *  Multiply two numeric objects.
     *
     * @param  a
     * @param  b
     * @param  type
     * @return result
     * @throws  HsqlException
     */
    static Object multiply(Object a, Object b,
                           int type) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

// fredt@users - type conversion - may need to apply to other arithmetic operations too
        if (!(a instanceof Number && b instanceof Number)) {
            a = Column.convertObject(a, type);
            b = Column.convertObject(b, type);
        }

        switch (type) {

            case Types.NULL :
                return null;

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad * bd));
            }
            case Types.NUMERIC :
            case Types.DECIMAL : {
                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                return abd.multiply(bbd);
            }
            case Types.TINYINT :
            case Types.SMALLINT :
            case Types.INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai * bi);
            }
            case Types.BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa * longb);
            }
            default :
                throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED,
                                  Types.getTypeString(type));
        }
    }

    /**
     *  Divide numeric object a by object b.
     *
     * @param  a
     * @param  b
     * @param  type
     * @return result
     * @throws  HsqlException
     */
    static Object divide(Object a, Object b, int type) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.NULL :
                return null;

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad / bd));
            }
            case Types.NUMERIC :
            case Types.DECIMAL : {
                BigDecimal abd   = (BigDecimal) a;
                BigDecimal bbd   = (BigDecimal) b;
                int        scale = abd.scale() > bbd.scale() ? abd.scale()
                                                             : bbd.scale();

                return (bbd.signum() == 0) ? null
                                           : abd.divide(bbd, scale,
                                           BigDecimal.ROUND_HALF_DOWN);
            }
            case Types.TINYINT :
            case Types.SMALLINT :
            case Types.INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                Trace.check(bi != 0, Trace.DIVISION_BY_ZERO);

                return ValuePool.getInt(ai / bi);
            }
            case Types.BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return (longb == 0) ? null
                                    : ValuePool.getLong(longa / longb);
            }
            default :
                throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED,
                                  Types.getTypeString(type));
        }
    }

    /**
     *  Subtract numeric object b from object a.
     *
     * @param  a
     * @param  b
     * @param  type
     * @return result
     * @throws  HsqlException
     */
    static Object subtract(Object a, Object b,
                           int type) throws HsqlException {

        if (a == null || b == null) {
            return null;
        }

        switch (type) {

            case Types.NULL :
                return null;

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return ValuePool.getDouble(Double.doubleToLongBits(ad - bd));
            }
            case Types.NUMERIC :
            case Types.DECIMAL : {
                BigDecimal abd = (BigDecimal) a;
                BigDecimal bbd = (BigDecimal) b;

                return abd.subtract(bbd);
            }
            case Types.TINYINT :
            case Types.SMALLINT :
            case Types.INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return ValuePool.getInt(ai - bi);
            }
            case Types.BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return ValuePool.getLong(longa - longb);
            }
            default :
                throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED,
                                  Types.getTypeString(type));
        }
    }

// fredt@users 20020130 - patch 505356 by daniel_fiser@users
// modified for performance and made optional
    private static Collator i18nCollator          = Collator.getInstance();
    static boolean          sql_compare_in_locale = false;

    static void setCompareInLocal(boolean value) {
        sql_compare_in_locale = value;
    }

    /**
     *  Compare a with b and return int value as result.
     *
     * @param  a instance of Java wrapper, depending on type, but always same for a & b (can be null)
     * @param  b instance of Java wrapper, depending on type, but always same for a & b (can be null)
     * @param  type one of the java.sql.Types
     * @return result 1 if a>b, 0 if a=b, -1 if b>a
     * @throws  HsqlException
     */
    static int compare(Object a, Object b, int type) throws HsqlException {

        int i = 0;

        if (a == b) {
            return 0;
        }

        // Current null handling here: null==null and smaller any value
        // Note, standard SQL null handling is handled by Expression.test() calling testNull() instead of this!
        // Attention, this is also used for grouping ('null' is one group)
        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        switch (type) {

            case Types.NULL :
                return 0;

            case Types.VARCHAR :
            case Types.LONGVARCHAR :
                if (sql_compare_in_locale) {
                    i = i18nCollator.compare((String) a, (String) b);
                } else {
                    i = ((String) a).compareTo((String) b);
                }
                break;

// fredt@users 20020130 - patch 418022 by deforest@users
// use of rtrim() to mimic SQL92 behaviour
            case Types.CHAR :
                if (sql_compare_in_locale) {
                    i = i18nCollator.compare(Library.rtrim((String) a),
                                             Library.rtrim((String) b));
                } else {
                    i = (Library.rtrim((String) a)).compareTo(
                        Library.rtrim((String) b));
                }
                break;

// boucherb@users 2003-09-25
// TODO:  Maybe use //#ifdef tag or reflective static method attribute
// instantiation to take advantage of String.compareToIgnoreCase when
// available (JDK 1.2 and greater) during ANT build. That or perhaps
// consider using either local character-wise comparison or first converting
// to lower case and then to upper case. Sun states that the JDK 1.2 introduced
// String.compareToIngnorCase() comparison involves calling
// Character.toLowerCase(Character.toUpperCase()) on compared characters,
// to correctly handle some caveats concering using only the one operation or
// the other outside the ascii character range.
            case Types.VARCHAR_IGNORECASE :
                if (sql_compare_in_locale) {
                    i = i18nCollator.compare(((String) a).toUpperCase(),
                                             ((String) b).toUpperCase());
                } else {
                    i = ((String) a).toUpperCase().compareTo(
                        ((String) b).toUpperCase());
                }
                break;

            case Types.TINYINT :
            case Types.SMALLINT :
            case Types.INTEGER : {
                int ai = ((Number) a).intValue();
                int bi = ((Number) b).intValue();

                return (ai > bi) ? 1
                                 : (bi > ai ? -1
                                            : 0);
            }
            case Types.BIGINT : {
                long longa = ((Number) a).longValue();
                long longb = ((Number) b).longValue();

                return (longa > longb) ? 1
                                       : (longb > longa ? -1
                                                        : 0);
            }
            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE : {
                double ad = ((Number) a).doubleValue();
                double bd = ((Number) b).doubleValue();

                return (ad > bd) ? 1
                                 : (bd > ad ? -1
                                            : 0);
            }
            case Types.NUMERIC :
            case Types.DECIMAL :
                i = ((BigDecimal) a).compareTo((BigDecimal) b);
                break;

            case Types.DATE :
                if (((java.sql.Date) a).after((java.sql.Date) b)) {
                    return 1;
                } else if (((java.sql.Date) a).before((java.sql.Date) b)) {
                    return -1;
                } else {
                    return 0;
                }
            case Types.TIME :
                return HsqlDateTime.compare((Time) a, (Time) b);

            case Types.TIMESTAMP :
                if (((Timestamp) a).after((Timestamp) b)) {
                    return 1;
                } else if (((Timestamp) a).before((Timestamp) b)) {
                    return -1;
                } else {
                    return 0;
                }
            case Types.BOOLEAN : {
                boolean boola = ((Boolean) a).booleanValue();
                boolean boolb = ((Boolean) b).booleanValue();

                return (boola == boolb) ? 0
                                        : (boolb ? -1
                                                 : 1);
            }
            case Types.BINARY :
            case Types.VARBINARY :
            case Types.LONGVARBINARY :
                if (a instanceof Binary && b instanceof Binary) {
                    i = compareTo(((Binary) a).getBytes(),
                                  ((Binary) b).getBytes());
                } else {
                    throw Trace.error(Trace.INVALID_CONVERSION,
                                      Types.getTypeString(type));
                }
                break;

            case Types.OTHER :
                return 0;

            default :
                throw Trace.error(Trace.INVALID_CONVERSION,
                                  Types.getTypeString(type));
        }

        return (i == 0) ? 0
                        : (i < 0 ? -1
                                 : 1);
    }

    /**
     *  Return a java string representation of a java object.
     *
     * @param  o
     * @return result (null value for null object)
     */
    static String convertObject(Object o) {

        if (o == null) {
            return null;
        }

        return o.toString();
    }

    /**
     *  Convert an object into a Java object that represents its SQL type.<p>
     *  All internal type conversion operations start with
     *  this method. If a direct conversion doesn't take place, the object
     *  is converted into a string first and an attempt is made to convert
     *  the string into the target type.<br>
     *
     *  One objective of this mehod is to ensure the Object can be converted
     *  to the given SQL type. For example, a very large BIGINT
     *  value cannot be narrowed down to an INTEGER or SMALLINT.<br>
     *
     *  Type conversion performed by this method has gradually evolved in 1.7.2
     *  to allow narrowing of numeric types in all cases according to the SQL
     *  standard.<br>
     *
     *  Another new objective is to normalize DATETIME values.<br>
     *
     *  Objects set via JDBC PreparedStatement use this method to convert
     *  the data to the Java type that is required for custom serialization
     *  by the engine. <br>
     *
     * @param  o
     * @param  type
     * @return result
     * @throws  HsqlException
     */
    public static Object convertObject(Object o,
                                       int type) throws HsqlException {

        try {
            if (o == null) {
                return null;
            }

            switch (type) {

                case Types.NULL :
                    return null;

                case Types.TINYINT :
                    if (o instanceof java.lang.Boolean) {
                        return ((Boolean) o).booleanValue()
                               ? ValuePool.getInt(1)
                               : ValuePool.getInt(0);
                    }

                    if (o instanceof java.lang.String) {
                        o = Library.trim((String) o, " ", true, true);

                        int val = Integer.parseInt((String) o);

                        o = ValuePool.getInt(val);
                    }

                    if (o instanceof java.lang.Integer) {
                        int temp = ((Number) o).intValue();

                        if (Byte.MAX_VALUE < temp || temp < Byte.MIN_VALUE) {
                            throw Trace.error(
                                Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                        }

                        return o;
                    }

                    if (o instanceof java.lang.Long) {
                        long temp = ((Number) o).longValue();

                        if (Byte.MAX_VALUE < temp || temp < Byte.MIN_VALUE) {
                            throw Trace.error(
                                Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                        }

                        return ValuePool.getInt(((Number) o).intValue());
                    }

                    // fredt@users - direct conversion to optimise JDBC setObject(Byte)
                    if (o instanceof java.lang.Byte) {
                        return ValuePool.getInt(((Number) o).intValue());
                    }

                    // fredt@users - returns to this method for range checking
                    if (o instanceof java.lang.Number) {
                        return convertObject(convertToInt(o), type);
                    }
                    break;

                case Types.SMALLINT :
                    if (o instanceof java.lang.Boolean) {
                        return ((Boolean) o).booleanValue()
                               ? ValuePool.getInt(1)
                               : ValuePool.getInt(0);
                    }

                    if (o instanceof java.lang.String) {
                        o = Library.trim((String) o, " ", true, true);

                        int val = Integer.parseInt((String) o);

                        o = ValuePool.getInt(val);
                    }

                    if (o instanceof java.lang.Integer) {
                        int temp = ((Number) o).intValue();

                        if (Short.MAX_VALUE < temp
                                || temp < Short.MIN_VALUE) {
                            throw Trace.error(
                                Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                        }

                        return o;
                    }

                    if (o instanceof java.lang.Long) {
                        long temp = ((Number) o).longValue();

                        if (Short.MAX_VALUE < temp
                                || temp < Short.MIN_VALUE) {
                            throw Trace.error(
                                Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                        }

                        return ValuePool.getInt(((Number) o).intValue());
                    }

                    // fredt@users - direct conversion for JDBC setObject(Short), etc.
                    if (o instanceof java.lang.Byte
                            || o instanceof java.lang.Short) {
                        return ValuePool.getInt(((Number) o).intValue());
                    }

                    // fredt@users - returns to this method for range checking
                    if (o instanceof java.lang.Number) {
                        return convertObject(convertToInt(o), type);
                    }
                    break;

                case Types.INTEGER :
                    if (o instanceof java.lang.Boolean) {
                        return ((Boolean) o).booleanValue()
                               ? ValuePool.getInt(1)
                               : ValuePool.getInt(0);
                    }

                    if (o instanceof java.lang.Integer) {
                        return o;
                    }

                    if (o instanceof java.lang.String) {
                        o = Library.trim((String) o, " ", true, true);

                        int val = Integer.parseInt((String) o);

                        return ValuePool.getInt(val);
                    }

                    if (o instanceof java.lang.Long) {
                        long temp = ((Number) o).longValue();

                        if (Integer.MAX_VALUE < temp
                                || temp < Integer.MIN_VALUE) {
                            throw Trace.error(
                                Trace.NUMERIC_VALUE_OUT_OF_RANGE);
                        }

                        return ValuePool.getInt(((Number) o).intValue());
                    }

                    if (o instanceof java.lang.Number) {
                        return convertToInt(o);
                    }

                    if (o instanceof java.lang.Boolean) {
                        return ValuePool.getInt(((Boolean) o).booleanValue()
                                                ? 1
                                                : 0);
                    }
                    break;

                case Types.BIGINT :
                    if (o instanceof java.lang.Boolean) {
                        return ((Boolean) o).booleanValue()
                               ? ValuePool.getLong(1)
                               : ValuePool.getLong(0);
                    }

                    if (o instanceof java.lang.Long) {
                        return o;
                    }

                    if (o instanceof java.lang.String) {
                        o = Library.trim((String) o, " ", true, true);

                        long val = Long.parseLong((String) o);

                        return ValuePool.getLong(val);
                    }

                    if (o instanceof java.lang.Integer) {
                        return ValuePool.getLong(((Integer) o).longValue());
                    }

                    if (o instanceof java.lang.Number) {
                        return convertToLong(o);
                    }
                    break;

                case Types.REAL :
                case Types.FLOAT :
                case Types.DOUBLE :
                    if (o instanceof java.lang.Boolean) {
                        return ((Boolean) o).booleanValue()
                               ? ValuePool.getDouble(1)
                               : ValuePool.getDouble(0);
                    }

                    if (o instanceof java.lang.Double) {
                        return o;
                    }

                    if (o instanceof java.lang.String) {
                        o = Library.trim((String) o, " ", true, true);

                        // jdk 1.1 compat
                        // double d = Double.parseDouble((String) o);
                        double d = new Double((String) o).doubleValue();
                        long   l = Double.doubleToLongBits(d);

                        return ValuePool.getDouble(l);
                    }

                    if (o instanceof java.lang.Number) {
                        return convertToDouble(o);
                    }
                    break;

                case Types.NUMERIC :
                case Types.DECIMAL :
                    if (o instanceof java.lang.Boolean) {
                        return ((Boolean) o).booleanValue() ? BIG_DECIMAL_1
                                                            : BIG_DECIMAL_0;
                    }

                    if (o instanceof java.math.BigDecimal) {
                        return o;
                    }
                    break;

                case Types.BOOLEAN :
                    if (o instanceof java.lang.Boolean) {
                        return (Boolean) o;
                    }

                    if (o instanceof java.lang.String) {
                        o = Library.trim((String) o, " ", true, true);

                        return org.hsqldb.lib.BooleanConverter.getBoolean(
                            (String) o);
                    }

                    if (o instanceof Integer) {
                        return org.hsqldb.lib.BooleanConverter.getBoolean(
                            (Integer) o);
                    }

                    if (o instanceof Long) {
                        return org.hsqldb.lib.BooleanConverter.getBoolean(
                            (Long) o);
                    }

                    if (o instanceof java.lang.Double) {
                        return org.hsqldb.lib.BooleanConverter.getBoolean(
                            (Double) o);
                    }

                    if (o instanceof java.math.BigDecimal) {
                        return org.hsqldb.lib.BooleanConverter.getBoolean(
                            (java.math.BigDecimal) o);
                    }

                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                case Types.VARCHAR_IGNORECASE :
                case Types.VARCHAR :
                case Types.CHAR :
                case Types.LONGVARCHAR :
                    if (o instanceof java.lang.String) {
                        return o;
                    }

                    if (o instanceof java.sql.Time) {
                        return HsqlDateTime.getTimeString((java.sql.Time) o,
                                                          null);
                    }

                    if (o instanceof java.sql.Timestamp) {
                        return HsqlDateTime.getTimestampString(
                            (java.sql.Timestamp) o, null);
                    }

                    if (o instanceof java.sql.Date) {
                        return HsqlDateTime.getDateString((java.sql.Date) o,
                                                          null);
                    }

                    if (o instanceof byte[]) {
                        return StringConverter.byteToHex((byte[]) o);
                    }
                    break;

                case Types.TIME :
                    if (o instanceof java.sql.Time) {
                        return HsqlDateTime.getNormalisedTime((Time) o);
                    }

                    if (o instanceof java.sql.Timestamp) {
                        return HsqlDateTime.getNormalisedTime((Timestamp) o);
                    }

                    if (o instanceof String) {
                        return HsqlDateTime.timeValue((String) o);
                    }

                    if (o instanceof java.sql.Date) {
                        throw Trace.error(Trace.INVALID_CONVERSION,
                                          Types.getTypeString(type));
                    }
                    break;

                case Types.TIMESTAMP :
                    if (o instanceof java.sql.Timestamp) {
                        return o;
                    }

                    if (o instanceof java.sql.Time) {
                        return HsqlDateTime.getNormalisedTimestamp((Time) o);
                    }

                    if (o instanceof java.sql.Date) {
                        return HsqlDateTime.getNormalisedTimestamp(
                            (java.sql.Date) o);
                    }

                    if (o instanceof String) {
                        return HsqlDateTime.timestampValue((String) o);
                    }
                    break;

                case Types.DATE :
                    if (o instanceof java.sql.Date) {
                        return HsqlDateTime.getNormalisedDate(
                            (java.sql.Date) o);
                    }

                    if (o instanceof java.sql.Timestamp) {
                        return HsqlDateTime.getNormalisedDate((Timestamp) o);
                    }

                    if (o instanceof String) {
                        return HsqlDateTime.dateValue((String) o);
                    }

                    if (o instanceof java.sql.Time) {
                        throw Trace.error(Trace.INVALID_CONVERSION,
                                          Types.getTypeString(type));
                    }
                    break;

                case Types.BINARY :
                case Types.VARBINARY :
                case Types.LONGVARBINARY :
                    if (o instanceof Binary) {
                        return o;
                    } else if (o instanceof byte[]) {
                        return new Binary((byte[]) o, false);
                    } else if (o instanceof String) {

                        /**
                         * @todo fredt - we need this for script processing only
                         *  handle the script separately and process normal
                         *  conversion according to rules in SQL
                         *  standard
                         */
                        return new Binary(
                            StringConverter.hexToByte((String) o), false);
                    }

                    throw Trace.error(Trace.INVALID_CONVERSION,
                                      Types.getTypeString(type));

// fredt@users 20030708 -  patch 1.7.2 - OBJECT handling - superseded
                case Types.OTHER :
                    if (o instanceof JavaObject) {
                        return o;
                    } else if (o instanceof String) {

                        /**
                         * @todo fredt - we need this for script processing only
                         *  handle the script separately and allow normal Sting
                         *  objects to be stored as JavaObject
                         */
                        return new JavaObject(
                            StringConverter.hexToByte((String) o));
                    } else if (o instanceof Binary) {
                        return new JavaObject(((Binary) o).getBytes());
                    }

                    return new JavaObject((Serializable) o);

                default :
            }

            if (o instanceof JavaObject) {
                o = ((JavaObject) o).getObject();

                return convertObject(o, type);
            }

            return convertString(o.toString(), type);
        } catch (HsqlException e) {
            throw e;
        } catch (Exception e) {
            throw Trace.error(Trace.WRONG_DATA_TYPE, e.getMessage());
        }
    }

    /**
     *  Return a java object based on a SQL string. This is called from
     *  convertObject(Object o, int type).
     *
     * @param  s
     * @param  type
     * @return
     * @throws  HsqlException
     */
    private static Object convertString(String s,
                                        int type) throws HsqlException {

        switch (type) {

            case Types.TINYINT :
            case Types.SMALLINT :

                // fredt - do maximumm / minimum checks on each type
                return convertObject(s, type);

            case Types.INTEGER :
                int val = Integer.parseInt(s);

                return ValuePool.getInt(val);

            case Types.BIGINT :
                return ValuePool.getLong(Long.parseLong(s));

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE :

                // jdk 1.1 compat
                // double d = Double.parseDouble((s);
                double d = new Double(s).doubleValue();
                long   l = Double.doubleToLongBits(d);

                return ValuePool.getDouble(l);

            case Types.VARCHAR_IGNORECASE :
            case Types.VARCHAR :
            case Types.CHAR :
            case Types.LONGVARCHAR :
                return s;

            case Types.DATE :
                return HsqlDateTime.dateValue(s);

            case Types.TIME :
                return HsqlDateTime.timeValue(s);

            case Types.TIMESTAMP :
                return HsqlDateTime.timestampValue(s);

            case Types.NUMERIC :
            case Types.DECIMAL :
                s = Library.trim(s, " ", true, true);

                return new BigDecimal(s);

            case Types.BOOLEAN :
                return org.hsqldb.lib.BooleanConverter.getBoolean(s);

            case Types.BINARY :
            case Types.VARBINARY :
            case Types.LONGVARBINARY :
            case Types.OTHER :
            default :
                throw Trace.error(Trace.INVALID_CONVERSION,
                                  Types.getTypeString(type));
        }
    }

    /**
     *  Return an SQL representation of an object. Strings will be quoted
     *  with single quotes, other objects will represented as in a SQL
     *  statement.
     *
     * @param  o
     * @param  type
     * @return result
     * @throws  HsqlException
     */
    static String createSQLString(Object o, int type) throws HsqlException {

        if (o == null) {
            return "NULL";
        }

        switch (type) {

            case Types.NULL :
                return "NULL";

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE :
                return createSQLString(((Number) o).doubleValue());

            case Types.DATE :
            case Types.TIME :
            case Types.TIMESTAMP :
                return StringConverter.toQuotedString(o.toString(), '\'',
                                                      false);

            case Types.BINARY :
            case Types.VARBINARY :
            case Types.LONGVARBINARY :
                if (!(o instanceof Binary)) {
                    throw Trace.error(Trace.INVALID_CONVERSION);
                }

                return StringConverter.toQuotedString(
                    StringConverter.byteToHex(((Binary) o).getBytes()), '\'',
                    false);

            case Types.OTHER :
                if (!(o instanceof JavaObject)) {
                    throw Trace.error(Trace.SERIALIZATION_FAILURE);
                }

                return StringConverter.toQuotedString(
                    StringConverter.byteToHex(((JavaObject) o).getBytes()),
                    '\'', false);

            case Types.VARCHAR_IGNORECASE :
            case Types.VARCHAR :
            case Types.CHAR :
            case Types.LONGVARCHAR :
                return createSQLString((String) o);

            default :
                return o.toString();
        }
    }

    public static String createSQLString(double x) {

        if (x == Double.NEGATIVE_INFINITY) {
            return "-1E0/0";
        }

        if (x == Double.POSITIVE_INFINITY) {
            return "1E0/0";
        }

        if (Double.isNaN(x)) {
            return "0E0/0E0";
        }

        String s = Double.toString(x);

        // ensure the engine treats the value as a DOUBLE, not DECIMAL
        if (s.indexOf('E') < 0) {
            s = s.concat("E0");
        }

        return s;
    }

    /**
     *  Turns a java string into a quoted SQL string
     *
     * @param  s java string
     * @return quoted SQL string
     */
    public static String createSQLString(String s) {

        if (s == null) {
            return "NULL";
        }

        return StringConverter.toQuotedString(s, '\'', true);
    }

// fredt@users 20030715 - patch 1.7.2 - type narrowing for numeric values

    /**
     * Type narrowing from DOUBLE/DECIMAL/NUMERIC to BIGINT / INT / SMALLINT / TINYINT
     * following the SQL rules. When conversion is from a non-integral type,
     * digits to the right of the decimal point are lost.
     */

    /**
     * Converter from a numeric object to Integer. Input is checked to be
     * within range represented by Integer.
     */
    static Integer convertToInt(Object o) throws HsqlException {

        int val = ((Number) o).intValue();

        if (o instanceof BigDecimal) {
            BigInteger bi = ((BigDecimal) o).toBigInteger();

            if (bi.compareTo(MAX_INT) > 0 || bi.compareTo(MIN_INT) < 0) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            return ValuePool.getInt(val);
        }

        if (o instanceof Double || o instanceof Float) {
            double d = ((Number) o).doubleValue();

            if (Double.isNaN(d) || d >= (double) Integer.MAX_VALUE + 1
                    || d <= (double) Integer.MIN_VALUE - 1) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            return ValuePool.getInt(val);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    /**
     * Converter from a numeric object to Long. Input is checked to be
     * within range represented by Long.
     */
    static Long convertToLong(Object o) throws HsqlException {

        long val = ((Number) o).longValue();

        if (o instanceof BigDecimal) {
            BigInteger bi = ((BigDecimal) o).toBigInteger();

            if (bi.compareTo(MAX_LONG) > 0 || bi.compareTo(MIN_LONG) < 0) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            return ValuePool.getLong(val);
        }

        if (o instanceof Double || o instanceof Float) {
            double d = ((Number) o).doubleValue();

            if (Double.isNaN(d) || d >= (double) Long.MAX_VALUE + 1
                    || d <= (double) Long.MIN_VALUE - 1) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }

            return ValuePool.getLong(val);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    /**
     * Converter from a numeric object to Double. Input is checked to be
     * within range represented by Double
     */
    static Double convertToDouble(Object o) throws HsqlException {

        double val = ((Number) o).doubleValue();

        if (o instanceof BigDecimal) {
            BigDecimal bd     = (BigDecimal) o;
            int        signum = bd.signum();
            BigDecimal bo     = new BigDecimal(val + signum);

            if (bo.compareTo(bd) != signum) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }

        return ValuePool.getDouble(Double.doubleToLongBits(val));
    }

// fredt@users 20020408 - patch 442993 by fredt - arithmetic expressions

    /**
     *  Arithmetic expressions terms are promoted to a type that can
     *  represent the resulting values and avoid incorrect results.<p>
     *  When the result or the expression is converted to the
     *  type of the target column for storage, an exception is thrown if the
     *  resulting value cannot be stored in the column<p>
     *  Returns a SQL type "wide" enough to represent the result of the
     *  expression.<br>
     *  A type is "wider" than the other if it can represent all its
     *  numeric values.<BR>
     *  Types narrower than INTEGER (int) are promoted to
     *  INTEGER. The order is as follows<p>
     *
     *  INTEGER, BIGINT, DOUBLE, DECIMAL<p>
     *
     *  TINYINT and SMALLINT in any combination return INTEGER<br>
     *  INTEGER and INTEGER return BIGINT<br>
     *  BIGINT and INTEGER return NUMERIC/DECIMAL<br>
     *  BIGINT and BIGINT return NUMERIC/DECIMAL<br>
     *  DOUBLE and INTEGER return DOUBLE<br>
     *  DOUBLE and BIGINT return DOUBLE<br>
     *  NUMERIC/DECIMAL and any type returns NUMERIC/DECIMAL<br>
     *
     * @author fredt@users
     * @param  type1  java.sql.Types value for the first numeric type
     * @param  type2  java.sql.Types value for the second numeric type
     * @return        either type1 or type2 on the basis of the above order
     */
    static int getCombinedNumberType(int type1, int type2, int expType) {

        int typeWidth1 = getNumTypeWidth(type1);
        int typeWidth2 = getNumTypeWidth(type2);

        if (typeWidth1 == 16 || typeWidth2 == 16) {
            return Types.DOUBLE;
        }

        switch (expType) {

            case Expression.ALTERNATIVE :
            case Expression.DIVIDE :
                return (typeWidth1 > typeWidth2) ? type1
                                                 : type2;

            default :
                int sum = typeWidth1 + typeWidth2;

                if (sum <= 4) {
                    return Types.INTEGER;
                }

                if (sum <= 8) {
                    return Types.BIGINT;
                }

                return Types.NUMERIC;
        }
    }

    /**
     * @param  type java.sql.Types int for a numeric type
     * @return relative width
     */
    private static int getNumTypeWidth(int type) {

        switch (type) {

            case Types.TINYINT :
                return 1;

            case Types.SMALLINT :
                return 2;

            case Types.INTEGER :
                return 4;

            case Types.BIGINT :
                return 8;

            case Types.REAL :
            case Types.FLOAT :
            case Types.DOUBLE :
                return 16;

            case Types.NUMERIC :
            case Types.DECIMAL :
                return 32;

            default :
                return 32;
        }
    }

    /**
     * Converts the specified hexadecimal digit <CODE>String</CODE>
     * to an equivalent array of bytes.
     *
     * @param hexString a <CODE>String</CODE> of hexadecimal digits
     * @throws HsqlException if the specified string contains non-hexadecimal digits.
     * @return a byte array equivalent to the specified string of hexadecimal digits
     */
    public static byte[] hexToByteArray(String hexString)
    throws HsqlException {

        try {
            return StringConverter.hexToByte(hexString);
        } catch (IOException e) {
            throw Trace.error(Trace.INVALID_CHARACTER_ENCODING);
        }
    }

    /**
     * Compares a <CODE>byte[]</CODE> with another specified
     * <CODE>byte[]</CODE> for order.  Returns a negative integer, zero,
     * or a positive integer as the first object is less than, equal to, or
     * greater than the specified second <CODE>byte[]</CODE>.<p>
     *
     * @param o1 the first byte[] to be compared
     * @param o2 the second byte[] to be compared
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    static int compareTo(byte[] o1, byte[] o2) {

        int len  = o1.length;
        int lenb = o2.length;

        for (int i = 0; ; i++) {
            int a = 0;
            int b = 0;

            if (i < len) {
                a = ((int) o1[i]) & 0xff;
            } else if (i >= lenb) {
                return 0;
            }

            if (i < lenb) {
                b = ((int) o2[i]) & 0xff;
            }

            if (a > b) {
                return 1;
            }

            if (b > a) {
                return -1;
            }
        }
    }
}
