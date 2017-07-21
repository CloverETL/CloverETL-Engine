package org.jetel.data.primitive;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;

import org.jetel.test.CloverTestCase;

public class NumericFormatTest extends CloverTestCase {
	
	NumericFormat format = new NumericFormat();
	DecimalFormat dFormat = new DecimalFormat();

	public void testParseCharSequence() throws ParseException{
		assertEquals(new BigDecimal("123"), format.parse("123"));
		assertEquals(new BigDecimal("123.45"), format.parse(dFormat.format(123.45)));
		
		String pattern = "00000";
		format.applyPattern(pattern);
		assertEquals(new BigDecimal("00123"), format.parse("00123"));
		assertEquals(new BigDecimal("123.45"), format.parse(dFormat.format(123.45)));
		
		pattern = "00000.000";
		format.applyPattern(pattern);
		dFormat.applyPattern(pattern);
		assertEquals(new BigDecimal("00123.000"), format.parse(dFormat.format(123)));
		assertEquals(new BigDecimal("00123.450"), format.parse(dFormat.format(123.45)));
		
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		symbols.setDecimalSeparator(':');
		format.setDecimalFormatSymbols(symbols);
		dFormat.setDecimalFormatSymbols(symbols);
		assertEquals(new BigDecimal("00123.000"), format.parse(dFormat.format(123)));
		assertEquals(new BigDecimal("00123.450"), format.parse(dFormat.format(123.45)));
		assertEquals(new BigDecimal("00123.450"), format.parse("00123:450"));
		
		symbols.setGroupingSeparator('\'');
		format.setDecimalFormatSymbols(symbols);
		dFormat.setDecimalFormatSymbols(symbols);
		assertEquals(new BigDecimal("00123.000"), format.parse(dFormat.format(123)));
		assertEquals(new BigDecimal("00123.450"), format.parse(dFormat.format(123.45)));
		assertEquals(new BigDecimal("00123.450"), format.parse("00'123:450"));

		pattern = "#,##0.0#";
		format.applyPattern(pattern);
		dFormat.applyPattern(pattern);
		assertEquals(new BigDecimal("00123.0"), format.parse(dFormat.format(123)));
		assertEquals(new BigDecimal("00123.45"), format.parse(dFormat.format(123.45)));
		assertEquals(new BigDecimal("00123.45"), format.parse("00'123:45"));
	}

	public void testFormatBigDecimal() {
		assertEquals(dFormat.format(123), format.format(new BigDecimal(123)));
		assertEquals(dFormat.format(123.45), format.format(new BigDecimal("123.45")));
		
		String pattern = "00000";
		format.applyPattern(pattern);
		dFormat.applyPattern(pattern);
		assertEquals(dFormat.format(123), format.format(new BigDecimal(123)));
		assertEquals(dFormat.format(123.45), format.format(new BigDecimal(123.45)));
		assertEquals(dFormat.format(3.45), format.format(new BigDecimal(3.45)));
		
		pattern = "00000.000";
		format.applyPattern(pattern);
		dFormat.applyPattern(pattern);
		assertEquals(dFormat.format(123), format.format(new BigDecimal(123)));
		assertEquals(dFormat.format(123.45), format.format(new BigDecimal(123.45)));
		
		
		DecimalFormatSymbols symbols = new DecimalFormatSymbols();
		symbols.setDecimalSeparator(':');
		format.setDecimalFormatSymbols(symbols);
		dFormat.setDecimalFormatSymbols(symbols);
		assertEquals(dFormat.format(123), format.format(new BigDecimal(123)));
		assertEquals(dFormat.format(123.45), format.format(new BigDecimal(123.45)));
		assertEquals("00123:450", format.format(new BigDecimal(123.45)));
		
		pattern = "00,000.000";
		format.applyPattern(pattern);
		dFormat.applyPattern(pattern);
		symbols.setGroupingSeparator('\'');
		format.setDecimalFormatSymbols(symbols);
		dFormat.setDecimalFormatSymbols(symbols);
		assertEquals(dFormat.format(123), format.format(new BigDecimal(123)));
		assertEquals(dFormat.format(123.45), format.format(new BigDecimal(123.45)));
		assertEquals("00'123:450", format.format(new BigDecimal(123.45)));

		pattern = "#,##0.0#";
		format.applyPattern(pattern);
		dFormat.applyPattern(pattern);
		assertEquals(dFormat.format(123), format.format(new BigDecimal(123)));
		assertEquals(dFormat.format(123.45), format.format(new BigDecimal(123.45)));
		assertEquals("123:45", format.format(new BigDecimal(123.45)));
		assertEquals("4'123:45", format.format(new BigDecimal("4123.45")));
		assertEquals("4'123:4", format.format(new BigDecimal("4123.4")));
		assertEquals("4'123:45", format.format(new BigDecimal("4123.456")));
		
		pattern = "0.0#";
		format.applyPattern(pattern);
		dFormat.applyPattern(pattern);
		assertEquals(dFormat.format(22.6), format.format(new BigDecimal(22.6)));
	}

}
