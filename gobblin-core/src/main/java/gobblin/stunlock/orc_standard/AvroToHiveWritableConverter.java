package gobblin.stunlock;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Throwables;

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
public class AvroToHiveWritableConverter
		extends InstrumentedConverter<Object, Object, AvroGenericRecordWritable, AvroHiveWritableRecord> {

	private SerDe serializer;
	private SerDe deserializer;

	@Override
	public AvroToHiveWritableConverter init(WorkUnitState state) {
		super.init(state);
		// Configuration conf = HadoopUtils.getConfFromState(state);

		try {
			this.serializer = HiveSerDeWrapper.getSerializer(state).getSerDe();
			this.deserializer = HiveSerDeWrapper.getDeserializer(state).getSerDe();
			//this.serializer.initialize(conf, state.getProperties()); // Reinitialized below
			//this.deserializer.initialize(conf, state.getProperties()); // Reinitialized below
		} catch (IOException e) {
			log.error("Failed to instantiate serializer and deserializer", e);
			throw Throwables.propagate(e);
		}

		return this;
	}

	@Override
	public Iterable<AvroHiveWritableRecord> convertRecordImpl(Object outputSchema,
			AvroGenericRecordWritable inputRecord, WorkUnitState workUnit) throws DataConversionException {

		// Added by Zec, set dynamic avro schema property.
		try {
			Configuration conf = HadoopUtils.getConfFromState(workUnit);
			
			Properties properties = workUnit.getProperties();
			properties.setProperty("avro.schema.literal", inputRecord.getRecord().getSchema().toString());

			this.serializer.initialize(conf, properties);
			this.deserializer.initialize(conf, properties);
		} catch (SerDeException e) {
			log.error("Failed to initialize serializer and deserializer", e);
			throw Throwables.propagate(e);
		}

		try {
			Object deserialized = this.deserializer.deserialize((Writable) inputRecord);
			Writable convertedRecord = this.serializer.serialize(deserialized, this.deserializer.getObjectInspector());

			AvroHiveWritableRecord wrappedRecord = new AvroHiveWritableRecord(inputRecord.getRecord(), convertedRecord);
			return new SingleRecordIterable<>(wrappedRecord);
		} catch (SerDeException e) {
			throw new DataConversionException(e);
		}
	}

	@Override
	public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
		return inputSchema;
	}

}
