package gobblin.stunlock;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;



public class StunlockKafkaAvroSource extends KafkaSource<Schema, GenericRecord> {

	@Override
	public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
		return new StunlockKafkaAvroExtractor(state);
	}
}