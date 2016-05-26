/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


import java.io.IOException;


/**
 * A abstract {@link DataWriterBuilder} for building {@link DataWriter}s that write to
 * {@link org.apache.hadoop.fs.FileSystem}s.
 *
 * @param <S> schema type
 * @param <S> data record type
 *
 * @author ziliu
 */
public class ParquetDataWriterBuilder extends PartitionAwareDataWriterBuilder<Schema, GenericRecord> {

  public static final String WRITER_INCLUDE_PARTITION_IN_FILE_NAMES =
      ConfigurationKeys.WRITER_PREFIX + ".include.partition.in.file.names";
  public static final String WRITER_REPLACE_PATH_SEPARATORS_IN_PARTITIONS =
      ConfigurationKeys.WRITER_PREFIX + ".replace.path.separators.in.partitions";

  /**
   * Get the file name to be used by the writer. If a {@link gobblin.writer.partitioner.WriterPartioner} is used,
   * the partition will be added as part of the file name.
   */
  public String getFileName(State properties) {

    String extension =
        this.format.equals(WriterOutputFormat.OTHER) ? getExtension(properties) : this.format.getExtension();
    String fileName = WriterUtils.getWriterFileName(properties, this.branches, this.branch, this.writerId, extension);

    if (this.partition.isPresent()) {
      fileName = getPartitionedFileName(properties, fileName);
    }

    return fileName;
  }

  private String getExtension(State properties) {
    return properties.getProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, StringUtils.EMPTY);
  }

  protected String getPartitionedFileName(State properties, String originalFileName) {
    boolean includePartitionerFieldNames = properties.getPropAsBoolean(
        ForkOperatorUtils.getPropertyNameForBranch(WRITER_INCLUDE_PARTITION_IN_FILE_NAMES, this.branches, this.branch),
        false);
    boolean removePathSeparators = properties.getPropAsBoolean(ForkOperatorUtils
        .getPropertyNameForBranch(WRITER_REPLACE_PATH_SEPARATORS_IN_PARTITIONS, this.branches, this.branch), false);

    return new Path(
        AvroUtils.serializeAsPath(this.partition.get(), includePartitionerFieldNames, removePathSeparators).toString(),
        originalFileName).toString();
  }

  @Override
  public boolean validatePartitionSchema(Schema partitionSchema) {
    return true;
  }
  @Override
  public DataWriter<GenericRecord> build() throws IOException {
    Preconditions.checkNotNull(this.destination);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));
    Preconditions.checkNotNull(this.schema);
    Preconditions.checkArgument(this.format == WriterOutputFormat.PARQUET);

    switch (this.destination.getType()) {
      case HDFS:
        return new ParquetDataWriter(this, this.destination.getProperties());
      default:
        throw new RuntimeException("Unknown destination type: " + this.destination.getType());
    }
  }

}
