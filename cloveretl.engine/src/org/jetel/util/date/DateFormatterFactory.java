package org.jetel.util.date;

import java.util.Locale;

import org.jetel.util.MiscUtils;

/**
 * Factory for internally used date formatters.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 *
 * @version 16th April 2010
 * @created 17th August 2009
 */
public final class DateFormatterFactory {

	/** the Java prefix specifying date format strings used by the Java's DateFormat class */
	public static final String JAVA_FORMAT_PREFIX = "java:";
	/** the Joda-Time prefix specifying date format strings used by the Joda-Time's DateTimeFormatter class */
	public static final String JODA_FORMAT_PREFIX = "joda:";

	public static DateFormatter createFormatter(String formatString, Locale locale) {
		if (locale == null) {
			locale = Locale.getDefault();
		}

		if (formatString == null) {
			return new JavaDateFormatter(locale);
		} else if (formatString.startsWith(JODA_FORMAT_PREFIX)) {
			return new JodaDateFormatter(formatString.substring(JODA_FORMAT_PREFIX.length()), locale);
		} else {
			if (formatString.startsWith(JAVA_FORMAT_PREFIX)) {
				formatString = formatString.substring(JAVA_FORMAT_PREFIX.length());
			}

			return new JavaDateFormatter(formatString, locale);
		}
	}

	public static DateFormatter createFormatter(String formatString, String localeString) {
		return createFormatter(formatString, MiscUtils.createLocale(localeString));
	}

	public static DateFormatter createFormatter(String formatString) {
		return createFormatter(formatString, Locale.getDefault());
	}

	public static DateFormatter createFormatter(Locale locale) {
		return new JavaDateFormatter(locale);
	}

	public static DateFormatter createFormatter() {
		return new JavaDateFormatter();
	}

	private DateFormatterFactory() {
		throw new UnsupportedOperationException();
	}

}
