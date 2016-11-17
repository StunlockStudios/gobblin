package gobblin.stunlock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;

import java.util.Optional;
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

/*
 * Wrapper to keep the original avro record as well as the converted orc record until partitioning has been done.
 */
public class AvroHiveWritableRecord {
	public GenericRecord avroSourceRecord;
	public Writable hiveWritableRecord;

	  public AvroHiveWritableRecord(GenericRecord avroSourceRecord, Writable hiveWritableRecord) {
	    this.avroSourceRecord = avroSourceRecord;
	    this.hiveWritableRecord = hiveWritableRecord;
	  }
}