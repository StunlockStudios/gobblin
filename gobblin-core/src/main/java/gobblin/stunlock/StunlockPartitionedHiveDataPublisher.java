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

package gobblin.stunlock;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.lang.IllegalArgumentException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.BaseDataPublisher;
import gobblin.publisher.TimePartitionedDataPublisher;
import gobblin.util.FileListUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;

/**
 * For time partition jobs, writer output directory is
 * $GOBBLIN_WORK_DIR/task-output/{extractId}/{tableName}/{partitionPath}, where
 * partition path is the time bucket, e.g., 2015/04/08/15.
 *
 * Publisher output directory is
 * $GOBBLIN_WORK_DIR/job-output/{tableName}/{partitionPath}
 *
 * @author ziliu
 */
public class StunlockPartitionedHiveDataPublisher extends BaseDataPublisher {
	private static final Logger LOG = LoggerFactory.getLogger(StunlockPartitionedHiveDataPublisher.class);

	// @formatter:off
	private static final String CREATE_TABLE_QUERY = "CREATE EXTERNAL TABLE IF NOT EXISTS %1$s "
			+ "PARTITIONED BY (%2$s) "
			+ "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' "
			+ "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' "
			+ "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " + "LOCATION '%3$s' "
			+ "TBLPROPERTIES ('avro.schema.url'='%4$s')";

	private static final String ALTER_TABLE_QUERY = "ALTER TABLE %1$s " + "ADD IF NOT EXISTS "
			+ "PARTITION (%2$s) LOCATION '%3$s'";

	private static final String PUBLISHER_SCHEMANAME = "stun.writer.partitioner.schemaname";
	private static final String HIVE_URL = "stun.publisher.hive.url";
	private static final String HIVE_USER = "stun.publisher.hive.user";
	private static final String HIVE_PASSWORD = "stun.publisher.hive.password";
	// @formatter:on

	public StunlockPartitionedHiveDataPublisher(State state) throws IOException {
		super(state);
	}

	// THIS IS FROM TimePartitionedDataPublisher
	@Override
	protected void addWriterOutputToExistingDir(Path writerOutput, Path publisherOutput, WorkUnitState workUnitState,
			int branchId, ParallelRunner parallelRunner) throws IOException {
		for (FileStatus status : FileListUtils.listFilesRecursively(this.writerFileSystemByBranches.get(branchId),
				writerOutput)) {
			String filePathStr = status.getPath().toString();
			String pathSuffix = filePathStr
					.substring(filePathStr.indexOf(writerOutput.toString()) + writerOutput.toString().length() + 1);
			Path outputPath = new Path(publisherOutput, pathSuffix);

			WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
					outputPath.getParent(), this.permissions.get(branchId));

			movePath(parallelRunner, this.getState(), status.getPath(), outputPath, branchId);
			try {
				RegisterInHive(workUnitState, outputPath.toString(), branchId);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}

	private void RegisterInHive(WorkUnitState state, String pathStr, int branchId) throws SQLException {
		LOG.info("RegisterInHive: " + pathStr);

		// HiveJdbcConnector conn;
		Closer closer = Closer.create();
		try {
			String writerPath = GetProperty(state, branchId, ConfigurationKeys.WRITER_FILE_PATH);
			String outputSchemaName = null;
			int startIndex = writerPath.length() + 1;
			for (int i = startIndex; i < pathStr.length(); i++){
				int chr = pathStr.charAt(i);
				if (startIndex == i && chr == '/') {
					startIndex++;
				} else 	if (chr == '/' || chr == '_') {
					outputSchemaName = pathStr.substring(startIndex, i);
					break;
				}
			}

			Path fsUri = new Path(state.getProp(ConfigurationKeys.DATA_PUBLISHER_FILE_SYSTEM_URI));

			Path filePath = new Path(fsUri, pathStr);

			String[] dirSplit = new Path(pathStr).getParent().toString().split("/");
			Optional<Integer> schemaId = Optional.absent();
 		        String basePart = fsUri.toString() + "/";
			int schemaIdIndex = -1;
			{
				String nameStr = outputSchemaName + "_";
				for (int i = 0; i < dirSplit.length; i++) {

					if (dirSplit[i].startsWith(nameStr)) {
						String idPart = dirSplit[i].substring(nameStr.length(), dirSplit[i].length());

						try {
							schemaId = Optional.of(Integer.parseInt(idPart));
							schemaIdIndex = i;
							break;
						} catch (NumberFormatException numex) {
						}
					}
                                    	basePart += dirSplit[i];
					if (i > 0)
						basePart += "/";
				}
			}

			if (schemaId.isPresent() == false) {
				throw new IllegalArgumentException(
						"Path does not match expected format. No folder for schema id partitions in " + pathStr + " schemaName=" + outputSchemaName);
			}
			
			List<String> partitions = new ArrayList<String>();
			List<String> partitionColumnDefs = new ArrayList<String>();
			List<String> partitionLocationParts = new ArrayList<String>();
			for (int i = 0; i < dirSplit.length; i++) {
				String[] partSplit = dirSplit[i].split("=");
				if (partSplit.length == 2) {
					partitions.add(partSplit[0] + "='" + partSplit[1] + "'");
					partitionColumnDefs.add(partSplit[0] + " string");
				}
				if (i > schemaIdIndex) {
					partitionLocationParts.add(dirSplit[i]);
				}
			}
			
			if (partitions.size() == 0) {
				throw new IllegalArgumentException(
						"Path does not match expected format. No partition key/value pairs in path " + pathStr);
			}
			
			String partitionsString = String.join(", ", partitions);
			String partitionDefString = String.join(", ", partitionColumnDefs);


			String schemaRegistryBaseUrl = GetSchemaRegistryBaseUrl(state);
			String schemaURL = schemaRegistryBaseUrl + "/schemas/ids/plain/" + schemaId.get();
			String tableName = outputSchemaName + "_" + schemaId.get(); 

			String hiveUrl = GetProperty(state, branchId, HIVE_URL);
			String hiveUser = GetProperty(state, branchId, HIVE_USER);
			String hivePassword = GetProperty(state, branchId, HIVE_PASSWORD);

			String hdfsTableRootLocation = basePart + Path.SEPARATOR + tableName;

			String relativeLocation = String.join("/", String.join("/", partitionLocationParts));

			String createTableStmt = String.format(CREATE_TABLE_QUERY, tableName, partitionDefString, hdfsTableRootLocation, schemaURL);
			
			String alterTableStmt = String.format(ALTER_TABLE_QUERY, tableName, partitionsString,
					relativeLocation);

			LOG.info("Time to register, Q1: " + createTableStmt);
			LOG.info("Time to register, Q2: " + alterTableStmt);
			StunHiveClient.ExecuteStatements(hiveUrl, hiveUser, hivePassword, createTableStmt, alterTableStmt);

			// Old
			// conn =
			// closer.register(HiveJdbcConnector.newConnectorWithProps(properties));
			// conn.executeStatements(createTableStmt, alterTableStmt,
			// refreshTableStmt);
			LOG.info("Register done");
		} finally {
			try {
				closer.close();
			} catch (IOException e) {
				LOG.error(e.toString());
			}
		}
	}

	private String GetSchemaRegistryBaseUrl(WorkUnitState state) {
		return state.getProp(ConfluentSchemaRegistry.SCHEMA_REGISTRY_URL);
	}

	private Boolean PropertyExists(WorkUnitState state, int branchId, String propertyName) {
		if (state.contains(ForkOperatorUtils.getPropertyNameForBranch(propertyName, branchId)))
			return true;

		else if (state.contains(propertyName))
			return true;

		return false;
	}

	private String GetProperty(WorkUnitState state, int branchId, String propertyName) {
		String prop = ForkOperatorUtils.getPropertyNameForBranch(propertyName, branchId);

		if (state.contains(prop))
			return state.getProp(prop);

		if (state.contains(propertyName))
			return state.getProp(propertyName);

		LOG.error("EXPECTED PROPERTY '" + prop + "' NOT SET, NEITHER IN BRANCH " + branchId
				+ " NOR AS UNBRANCHED SETTING!");
		return null;
	}
}
