package gobblin.stunlock;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Throwables;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.hive.HiveSerDeWrapper;
import gobblin.instrumented.converter.InstrumentedConverter;
import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

/**
 * STUNLOCK MODIFICATIONS: WRAP RESULT AND KEEP SOURCE RECORD! Needed further
 * down the line to get partition data and schema id. Source Version is
 * gobblin.converter.serde.HiveSerDeConverter (which was a generic multi-purpose
 * way of doing it)
 */
@SuppressWarnings("deprecation")
@Slf4j
public class AvroToOrcConverter {
	private SerDe serializer;
	private SerDe deserializer;

	State state;

	public void init(State properties) {
		try {
			this.state = properties;
			this.serializer = HiveSerDeWrapper.getSerializer(state).getSerDe();
			this.deserializer = HiveSerDeWrapper.getDeserializer(state).getSerDe();
		} catch (IOException e) {
			log.error("Failed to instantiate serializer and deserializer", e);
			throw Throwables.propagate(e);
		}
	}

	public Writable convertRecord(AvroGenericRecordWritable inputRecord) throws DataConversionException {

		// Added by Zec, set dynamic avro schema property.
		try {
			Configuration conf = HadoopUtils.getConfFromState(state);
			
			Properties properties = state.getProperties();
			properties.setProperty("avro.schema.literal", inputRecord.getRecord().getSchema().toString());

			this.serializer.initialize(conf, properties);
			this.deserializer.initialize(conf, properties);
		} catch (SerDeException e) {
			log.error("Failed to initialize serializer and deserializer", e);
			throw new DataConversionException(e);
		}

		try {
			Object deserialized = this.deserializer.deserialize((Writable) inputRecord);
			Writable convertedRecord = this.serializer.serialize(deserialized, this.deserializer.getObjectInspector());

			return convertedRecord;
		} catch (SerDeException e) {
			log.error("Failed to convert record", e);
			throw new DataConversionException(e);
		}
	}
}
