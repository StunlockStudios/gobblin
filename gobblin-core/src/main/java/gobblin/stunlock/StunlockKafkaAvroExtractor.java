package gobblin.stunlock;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.source.extractor.extract.kafka.KafkaAvroExtractor;

public class StunlockKafkaAvroExtractor extends KafkaAvroExtractor<Integer>
{
	private static final Logger LOG = LoggerFactory.getLogger(StunlockKafkaAvroExtractor.class);

	private static final byte MAGIC_BYTE = (byte) 0xFF;

	public StunlockKafkaAvroExtractor(WorkUnitState state)
	{
		super(state);
		LOG.info("StunlockKafkaAvroExtractor Constructor");
	}

	@Override
	protected Decoder getDecoder(byte[] payload)
	{
		//LOG.info("StunlockKafkaAvroExtractor getDecoder");
		return DecoderFactory.get().binaryDecoder(payload, 1 + 4, payload.length - 1 - 4, null);
	}

	@Override
	protected Schema getRecordSchema(byte[] payload)
	{
		//LOG.info("StunlockKafkaAvroExtractor getRecordSchema");
		if (payload[0] != MAGIC_BYTE)
		{
			return null;
		}

		int schemaId = byteArrayToInt(payload, 1);
		try
		{
			Schema schema = super.schemaRegistry.get().getSchemaByKey(schemaId);
			if (schema == null)
			{
				return null;
			}
			return schema;
		}
		catch (SchemaRegistryException e)
		{
			LOG.error("getRecordSchema EXCEPTION:", e);
			return null;
		}
	}

	private static int byteArrayToInt(byte[] b, int index)
	{
		return b[0 + index] & 0xFF | (b[1 + index] & 0xFF) << 8 | (b[2 + index] & 0xFF) << 16 | (b[3 + index] & 0xFF) << 24;
	}

}
