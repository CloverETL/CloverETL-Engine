package org.jetel.util.date;

import org.jetel.util.formatter.TimeZoneProvider;

public class DateFieldExtractorFactory {

	private DateFieldExtractorFactory() {
		// not available
	}
	
	public static DateFieldExtractor getExtractor(String timeZoneString) {
		TimeZoneProvider p = new TimeZoneProvider(timeZoneString);

		if (p.hasJodaTimeZone()) {
			return new JodaDateFieldExtractor(p.getJodaTimeZone());
		} 
		
		return new JavaDateFieldExtractor(p.getJavaTimeZone());
	}
}
