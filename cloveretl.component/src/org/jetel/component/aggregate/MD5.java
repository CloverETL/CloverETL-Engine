package org.jetel.component.aggregate;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.Base64;

/**
 * Calculates MD5 on the aggregation group.
 * 
 * Output field type must be String and nullable if input is nullable.
 * 
 * @author Jaroslav Urban
 *
 */
public class MD5 extends AggregateFunction {
	private static final String NAME = "MD5";
	
	private int loopCount = 0;
	private MessageDigest md5;
	ByteBuffer dataBuffer = ByteBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
	private CharsetEncoder encoder;

	// Is input nullable?
	private boolean nullableInput;


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregateProcessorException {
		nullableInput = inputField.isNullable();
		return;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void  checkOutputFieldType(DataFieldMetadata outputField) throws AggregateProcessorException {
		if (nullableInput && !outputField.isNullable()) {
			throw new AggregateProcessorException(AggregateFunction.ERROR_NULLABLE_BECAUSE_INPUT);
		}
		if (outputField.getType() != DataFieldMetadata.STRING_FIELD) {
			throw new AggregateProcessorException(AggregateFunction.ERROR_STRING);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#init()
	 */
	@Override
	public void init() {
		try {
			md5 = MessageDigest.getInstance("MD5");
			md5.reset();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 algorithm is not available", e);
		}
		
        if(charset == null) {
            encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
        } else {
            encoder = Charset.forName(charset).newEncoder();
        }
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#requiresInputField()
	 */
	@Override
	public boolean requiresInputField() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#storeResult(org.jetel.data.DataField)
	 */
	@Override
	public void storeResult(DataField outputField) {
		if (loopCount == 0) {
			outputField.setNull(true);
		} else {
			outputField.setValue(Base64.encodeBytes(md5.digest()));
		}
	}


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) throws Exception {
		DataField input = record.getField(inputFieldIndex);
		if (input.isNull()) {
			return;
		}
		
		dataBuffer.clear();
		input.toByteBuffer(dataBuffer, encoder);
		md5.update(dataBuffer.array());
		loopCount++;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#getName()
	 */
	@Override
	public String getName() {
		return NAME;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#clear()
	 */
	@Override
	public void clear() {
		md5.reset();
		loopCount = 0;
	}
}
