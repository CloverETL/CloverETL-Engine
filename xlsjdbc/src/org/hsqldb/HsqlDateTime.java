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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

// fredt@users 20020130 - patch 1.7.0 by fredt - new class
// replaces patch by deforest@users
// fredt@users 20020414 - patch 517028 by peterhudson@users - use of calendar
// fredt@users 20020414 - patch 828957 by tjcrowder@users - JDK 1.3 compatibility
// fredt@users 20040105 - patch 870957 by Gerhard Hiller - JDK bug workaround

/**
 *  collection of static methods to convert Date, Time and Timestamp strings
 *  into corresponding Java objects. Also accepts SQL literals such as NOW,
 *  TODAY as valid strings and returns the current date / time / datetime.
 *  Compatible with jdk 1.1.x.<p>
 *
 *  Was reviewed for 1.7.2 resulting in centralising all DATETIME related
 *  operstions.<p>
 *
 *  HSQLDB uses the client and server's default timezone for all DATETIME
 *  operations. It stores the DATETIME values in .log and .script files using
 *  the default locale of the server. The same values are stored as binary
 *  UTC timestamps in .data files. If the database is trasported from one
 *  timezone to another, then the DATETIME values in cached tables will be
 *  handled as UTC but those in other tables will be treated as local. So
 *  a timestamp representing 12 noon stored in Tokyo timezone will be treated
 *  as 9 pm in London when stored in a cached table but the same value stored
 *  in a memory table will be treated as 12 noon.
 *
 * @author  fredt@users
 * @version 1.7.2
 * @since 1.7.0
 */
public class HsqlDateTime {

    /**
     * A reusable static value for today's date. Should only be accessed
     * by getToday()
     */
    private static Calendar today          = new GregorianCalendar();
    private static Calendar tempCalDefault = new GregorianCalendar();
    private static Calendar tempCalGMT =
        new GregorianCalendar(TimeZone.getTimeZone("GMT"));
    private static Date tempDate = new Date(0);
    private static Date currentDate;

    static {
        resetToday(System.currentTimeMillis());
    }

    static final String zerodatetime = "1970-01-01 00:00:00.000000000";

    /**
     *  Converts a string in JDBC timestamp escape format to a
     *  <code>Timestamp</code> value.
     *
     * @param s timestamp in format <code>yyyy-mm-dd hh:mm:ss.fffffffff</code>
     *      where end part can be omitted, or "NOW" (case insensitive)
     * @return  corresponding <code>Timestamp</code> value
     * @exception java.lang.IllegalArgumentException if the given argument
     * does not have the format <code>yyyy-mm-dd hh:mm:ss.fffffffff</code>
     */
    public static Timestamp timestampValue(String s) {

        if (s == null) {
            throw new java.lang.IllegalArgumentException(
                Trace.getMessage(Trace.HsqlDateTime_null_string));
        }

        s = s + zerodatetime.substring(s.length());

        return Timestamp.valueOf(s);
    }

    /**
     * For use with .script file, simpler than above
     */
    public static Timestamp simpleTimestampValue(String s) {
        return Timestamp.valueOf(s);
    }

    /**
     * @param  time milliseconds
     * @param  nano nanoseconds
     * @return  Timestamp object
     */
    public static Timestamp timestampValue(long time, int nano) {

        Timestamp ts = new Timestamp(time);

        ts.setNanos(nano);

        return ts;
    }

    /**
     *  Converts a string in JDBC date escape format to a <code>Date</code>
     *  value. Also accepts Timestamp values.
     *
     * @param s date in format <code>yyyy-mm-dd</code>,
     *  'TODAY', 'NOW', 'CURRENT_DATE', 'SYSDATE' (case independent)
     * @return  corresponding <code>Date</code> value
     * @exception java.lang.IllegalArgumentException if the given argument
     * does not have the format <code>yyyy-mm-dd</code>
     */
    public static Date dateValue(String s) {

        if (s == null) {
            throw new java.lang.IllegalArgumentException(
                Trace.getMessage(Trace.HsqlDateTime_null_date));
        }

        if (s.length() > sdfdPattern.length()) {
            return Date.valueOf(s.substring(0, sdfdPattern.length()));
        }

        return Date.valueOf(s);
    }

    /**
     * Converts a string in JDBC date escape format to a
     * <code>Time</code> value.
     *
     * @param s date in format <code>hh:mm:ss</code>
     * 'CURRENT_TIME' (case independent)
     * @return  corresponding <code>Time</code> value
     * @exception java.lang.IllegalArgumentException if the given argument
     * does not have the format <code>hh:mm:ss</code>
     */
    public static Time timeValue(String s) {

        if (s == null) {
            throw new java.lang.IllegalArgumentException(
                Trace.getMessage(Trace.HsqlDateTime_null_string));
        }

        return Time.valueOf(s);
    }

    static int compare(Time a, Time b) throws HsqlException {

        if (a.getTime() == b.getTime()) {
            return 0;
        }

        return a.getTime() > b.getTime() ? 1
                                         : -1;
    }

    public synchronized static Date getCurrentDate(long millis) {

        getToday(millis);

        return currentDate;
    }

    public static Timestamp getTimestamp(long millis) {
        return new Timestamp(millis);
    }

    private static final String sdftPattern  = "HH:mm:ss";
    private static final String sdfdPattern  = "yyyy-MM-dd";
    private static final String sdftsPattern = "yyyy-MM-dd HH:mm:ss.";

    static java.sql.Date getDate(String dateString,
                                 Calendar cal) throws Exception {

        synchronized (sdfd) {
            sdfd.setCalendar(cal);

            java.util.Date d = sdfd.parse(dateString);

            return new java.sql.Date(d.getTime());
        }
    }

    static Time getTime(String timeString, Calendar cal) throws Exception {

        synchronized (sdft) {
            sdft.setCalendar(cal);

            java.util.Date d = sdft.parse(timeString);

            return new java.sql.Time(d.getTime());
        }
    }

    static Timestamp getTimestamp(String dateString,
                                  Calendar cal) throws Exception {

        synchronized (sdfts) {
            sdfts.setCalendar(cal);

            java.util.Date d = sdfts.parse(dateString.substring(0,
                sdftsPattern.length()));
            String nanostring = dateString.substring(sdftsPattern.length(),
                dateString.length());
            java.sql.Timestamp ts = new java.sql.Timestamp(d.getTime());

            ts.setNanos(Integer.parseInt(nanostring));

            return ts;
        }
    }

    static SimpleDateFormat sdfd  = new SimpleDateFormat(sdfdPattern);
    static SimpleDateFormat sdft  = new SimpleDateFormat(sdftPattern);
    static SimpleDateFormat sdfts = new SimpleDateFormat(sdftsPattern);

    public static String getTimestampString(Timestamp x,
            Calendar cal) throws Exception {

        synchronized (sdfts) {
            sdfts.setCalendar(cal == null ? tempCalDefault
                                          : cal);

            String s = sdfts.format(new java.util.Date(x.getTime()))
                       + x.getNanos();

            return s + zerodatetime.substring(s.length());
        }
    }

    public static String getTimeString(Time x,
                                       Calendar cal) throws Exception {

        synchronized (sdft) {
            sdft.setCalendar(cal == null ? tempCalDefault
                                         : cal);

            return sdft.format(x);
        }
    }

    public static String getDateString(Date x,
                                       Calendar cal) throws Exception {

        synchronized (sdfd) {
            sdfd.setCalendar(cal == null ? tempCalDefault
                                         : cal);

            return sdfd.format(x);
        }
    }

    /**
     * Returns the same Date Object. This object should be treated as
     * read-only.
     */
    static synchronized Calendar getToday(long millis) {

        if (millis - getTimeInMillis(today) >= 24 * 3600 * 1000) {
            resetToday(millis);
        }

        return today;
    }

    public static void resetToDate(Calendar cal) {

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
    }

    public static void resetToTime(Calendar cal) {

        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MILLISECOND, 0);
    }

    /**
     * resets the static reusable value today
     */
    synchronized private static void resetToday(long millis) {

//#ifdef JDBC3
        // Use method directly
        today.setTimeInMillis(millis);

//#else
/*
        // Have to go indirect
        tempDate.setTime(millis);
        today.setTime(tempDate);
*/

//#endif JDBC3
        resetToDate(today);

        currentDate = new java.sql.Date(getTimeInMillis(today));
    }

    /**
     * Sets the time in the given Calendar using the given milliseconds value; wrapper method to
     * allow use of more efficient JDK1.4 method on JDK1.4 (was protected in earlier versions).
     *
     * @param       cal                             the Calendar
     * @param       millis                  the time value in milliseconds
     */
    private static void setTimeInMillis(Calendar cal, long millis) {

//#ifdef JDBC3
        // Use method directly
        cal.setTimeInMillis(millis);

//#else
/*
        // Have to go indirect
        synchronized(tempDate){
            tempDate.setTime(millis);
            cal.setTime(tempDate);
        }
*/

//#endif JDBC3
    }

    /**
     * Gets the time from the given Calendar as a milliseconds value; wrapper method to
     * allow use of more efficient JDK1.4 method on JDK1.4 (was protected in earlier versions).
     *
     * @param       cal                             the Calendar
     * @return      the time value in milliseconds
     */
    public static long getTimeInMillis(Calendar cal) {

//#ifdef JDBC3
        // Use method directly
        return (cal.getTimeInMillis());

//#else
/*
        // Have to go indirect
        return (cal.getTime().getTime());
*/

//#endif JDBC3
    }

    public static Time getNormalisedTime(long t) {

        synchronized (tempCalDefault) {
            setTimeInMillis(tempCalDefault, t);
            resetToTime(tempCalDefault);

            long value = getTimeInMillis(tempCalDefault);

            return new Time(value);
        }
    }

    public static Time getNormalisedTime(Time t) {
        return getNormalisedTime(t.getTime());
    }

    public static Time getNormalisedTime(Timestamp ts) {
        return getNormalisedTime(ts.getTime());
    }

    public static long getNormalisedDate(long d) {

        synchronized (tempCalDefault) {
            setTimeInMillis(tempCalDefault, d);
            resetToDate(tempCalDefault);

            return getTimeInMillis(tempCalDefault);
        }
    }

    public static Date getNormalisedDate(Timestamp ts) {

        synchronized (tempCalDefault) {
            setTimeInMillis(tempCalDefault, ts.getTime());
            resetToDate(tempCalDefault);

            long value = getTimeInMillis(tempCalDefault);

            return new Date(value);
        }
    }

    public static Date getNormalisedDate(Date d) {

        synchronized (tempCalDefault) {
            setTimeInMillis(tempCalDefault, d.getTime());
            resetToDate(tempCalDefault);

            long value = getTimeInMillis(tempCalDefault);

            return new Date(value);
        }
    }

    public static Timestamp getNormalisedTimestamp(Time t) {

        synchronized (tempCalGMT) {
            setTimeInMillis(tempCalGMT, System.currentTimeMillis());
            resetToDate(tempCalGMT);

            long value = getTimeInMillis(tempCalGMT) + t.getTime();

            return new Timestamp(value);
        }
    }

    public static Timestamp getNormalisedTimestamp(Date d) {

        synchronized (tempCalDefault) {
            setTimeInMillis(tempCalDefault, d.getTime());
            resetToDate(tempCalDefault);

            long value = getTimeInMillis(tempCalDefault);

            return new Timestamp(value);
        }
    }

    /**
     * Returns the indicated part of the given <code>java.util.Date</code> object.
     * @param d the <code>Date</code> object from which to extract the indicated part
     * @param part an integer code corresponding to the desired date part
     * @return the indicated part of the given <code>java.util.Date</code> object
     */
    static int getDateTimePart(java.util.Date d, int part) {

        synchronized (tempCalDefault) {
            tempCalDefault.setTime(d);

            return tempCalDefault.get(part);
        }
    }
}
