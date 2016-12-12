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
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.BaseDataPublisher;
import gobblin.publisher.TimePartitionedDataPublisher;
import gobblin.util.FileListUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
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
	private static final String Q1_CREATE_AVRO_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS %1$s%2$s "
			+ "PARTITIONED BY (%3$s) " + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' "
			+ "STORED AS AVRO " + "TBLPROPERTIES ('avro.schema.url'='%4$s')";

	private static final String Q2_CREATE_DATA_TABLE_QUERY = "CREATE EXTERNAL TABLE IF NOT EXISTS %1$s%2$s LIKE %1$s%3$s STORED AS %4$s LOCATION '%5$s'";

	private static final String Q3_ALTER_TABLE_QUERY = "ALTER TABLE %1$s%2$s " + "ADD IF NOT EXISTS "
			+ "PARTITION (%3$s) LOCATION '%4$s'";

	private static final String STORAGE_FORMAT = "writer.output.format";
	private static final String HIVE_URL = "stun.publisher.hive.url";
	private static final String HIVE_USER = "stun.publisher.hive.user";
	private static final String HIVE_PASSWORD = "stun.publisher.hive.password";
	private static final String TABLE_POSTFIX = "stun.publisher.table.postfix";
	private static final String SCHEMA_TABLE_POSTFIX = "stun.publisher.schema.table.postfix";
	// @formatter:on

	private List<String> registeredFilePaths;

	public StunlockPartitionedHiveDataPublisher(State state) throws IOException {
		super(state);
		registeredFilePaths = new ArrayList<String>();
	}

	@Override
	protected void publishData(WorkUnitState state, int branchId, boolean publishSingleTaskData,
			Set<Path> writerOutputPathsMoved) throws IOException {
		List<FileStatus> newFiles = new ArrayList<FileStatus>();
		Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);
		Path publisherOutputDir = getPublisherOutputDir(state, branchId);
		ParallelRunner parallelRunner = this.getParallelRunner(this.writerFileSystemByBranches.get(branchId));

		try {
			String allPaths = "";

			if (this.writerFileSystemByBranches.get(branchId).exists(writerOutputDir)) {
				for (FileStatus status : FileListUtils
						.listFilesRecursively(this.writerFileSystemByBranches.get(branchId), writerOutputDir)) {
					Path filePath = getOutputPath(writerOutputDir, publisherOutputDir, status);
					String pathStr = filePath.toString();

					if (registeredFilePaths.contains(pathStr) == false) {
						registeredFilePaths.add(pathStr);
						newFiles.add(status);
						allPaths += pathStr + "\n";
					}
				}
			}

			LOG.info("StunPublish: Total # of unregistered files found: " + newFiles.size() + ". Files:\n" + allPaths
					+ "\n(" + newFiles.size() + " Files)");

			for (FileStatus status : newFiles) {
				Path filePath = getOutputPath(writerOutputDir, publisherOutputDir, status);

				TryMoveWithRetry(parallelRunner, branchId, status, filePath);
				RegisterInHive(state, filePath.toString(), branchId);
			}
			LOG.info("StunPublish: All files published and registered to target location.");
			writerOutputPathsMoved.add(writerOutputDir);
		} catch (IOException e) {
			LOG.error("StunPublish IOException Caught: " + e);
			ClearOnFail(newFiles, writerOutputDir, publisherOutputDir, parallelRunner);
			throw new IOException(e);
		} catch (SQLException e) {
			LOG.error("StunPublish SQLException Caught: " + e);
			ClearOnFail(newFiles, writerOutputDir, publisherOutputDir, parallelRunner);
			throw new IOException(e);
		}
	}

	private void ClearOnFail(List<FileStatus> files, Path writerOutputDir, Path publisherOutputDir,
			ParallelRunner parallelRunner) {
		for (FileStatus status : files) {
			Path filePath = getOutputPath(writerOutputDir, publisherOutputDir, status);
			parallelRunner.deletePath(filePath, false);
		}
	}

	private void TryMoveWithRetry(ParallelRunner parallelRunner, int branchId, FileStatus status, Path filePath)
			throws IOException {
		int retryCount = 20;
		Throwable lastError = null;
		for (int i = 0; i <= retryCount; i++) {
			try {
				Move(parallelRunner, branchId, status, filePath);

				if (lastError != null)
					LOG.info("StunPublish.TryMove succeeded after " + i + " retries. Rejoice!");
				return;
			} catch (Throwable e) {
				LOG.warn("StunPublish.TryMove threw with " + (retryCount - i) + " retries left on file "
						+ filePath.toString() + ". Sleeping 10s. Exception: " + e.toString());
				lastError = e;
				try {
					Thread.sleep(10000);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		}
		LOG.error("StunPublish.TryMove FAILED after max retries (" + retryCount + ")\n* File: " + filePath.toString()
				+ "\n* Last Exception: " + lastError); 
		throw new IOException("StunPublish.TryMove FAILED after max retries (" + retryCount + ")\n* File: "
				+ filePath.toString() + "\n* Last Exception: " + lastError, lastError);
	}

	private void Move(ParallelRunner parallelRunner, int branchId, FileStatus status, Path filePath)
			throws IOException {
		WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
				filePath.getParent(), this.permissions.get(branchId));

		movePath(parallelRunner, this.getState(), status.getPath(), filePath, branchId);
	}

	private ParallelRunner getParallelRunner(FileSystem fs) {
		String uri = fs.getUri().toString();
		if (!this.parallelRunners.containsKey(uri)) {
			this.parallelRunners.put(uri,
					this.parallelRunnerCloser.register(new ParallelRunner(this.parallelRunnerThreads, fs)));
		}
		return this.parallelRunners.get(uri);
	}

	// THIS IS FROM TimePartitionedDataPublisher

	/*
	 * @Override protected void addWriterOutputToExistingDir(Path writerOutput,
	 * Path publisherOutput, WorkUnitState workUnitState, int branchId,
	 * ParallelRunner parallelRunner) throws IOException { try { for (FileStatus
	 * status :
	 * FileListUtils.listFilesRecursively(this.writerFileSystemByBranches.get(
	 * branchId), writerOutput)) { Path outputPath = getOutputPath(writerOutput,
	 * publisherOutput, status);
	 * 
	 * WriterUtils.mkdirsWithRecursivePermission(this.
	 * publisherFileSystemByBranches.get(branchId), outputPath.getParent(),
	 * this.permissions.get(branchId));
	 * 
	 * movePath(parallelRunner, this.getState(), status.getPath(), outputPath,
	 * branchId); RegisterInHive(workUnitState, outputPath.toString(),
	 * branchId); } } catch (SQLException e) { for (FileStatus status :
	 * FileListUtils.listFilesRecursively(this.writerFileSystemByBranches.get(
	 * branchId), writerOutput)) { Path outputPath = getOutputPath(writerOutput,
	 * publisherOutput, status); parallelRunner.deletePath(outputPath, false); }
	 * throw new IOException(e); } }
	 */
	private Path getOutputPath(Path writerOutput, Path publisherOutput, FileStatus status) {
		String filePathStr = status.getPath().toString();
		String pathSuffix = filePathStr
				.substring(filePathStr.indexOf(writerOutput.toString()) + writerOutput.toString().length() + 1);
		return new Path(publisherOutput, pathSuffix);
	}

	private void RegisterInHive(WorkUnitState state, String pathStr, int branchId) throws SQLException {
		LOG.info("StunPublish: RegisterInHive: " + pathStr);

		// HiveJdbcConnector conn;
		Closer closer = Closer.create();
		try {
			String writerPath = GetProperty(state, branchId, ConfigurationKeys.WRITER_FILE_PATH);
			String outputSchemaName = null;
			int startIndex = writerPath.length() + 1;
			for (int i = startIndex; i < pathStr.length(); i++) {
				int chr = pathStr.charAt(i);
				if (startIndex == i && chr == '/') {
					startIndex++;
				} else if (chr == '/') {
					break;
				} else if (chr == '_') {
					outputSchemaName = pathStr.substring(startIndex, i);
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
						"Path does not match expected format. No folder for schema id partitions in " + pathStr
								+ " schemaName=" + outputSchemaName);
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

			String tablePostfix = "";
			String schemaTablePostfix = "";

			if (PropertyExists(state, branchId, TABLE_POSTFIX))
				tablePostfix = GetProperty(state, branchId, TABLE_POSTFIX);

			if (PropertyExists(state, branchId, SCHEMA_TABLE_POSTFIX))
				schemaTablePostfix = GetProperty(state, branchId, SCHEMA_TABLE_POSTFIX);

			String partitionsString = String.join(", ", partitions);
			String partitionDefString = String.join(", ", partitionColumnDefs);

			String schemaURL = StunUtils.getSchemaUrl(state, schemaId.get());
			String tableName = outputSchemaName + "_" + schemaId.get();

			String hiveUrl = GetProperty(state, branchId, HIVE_URL);
			String hiveUser = GetProperty(state, branchId, HIVE_USER);
			String hivePassword = GetProperty(state, branchId, HIVE_PASSWORD);
			String storage_format = GetProperty(state, branchId, STORAGE_FORMAT);

			String hdfsTableRootLocation = basePart + Path.SEPARATOR + tableName;

			String relativeLocation = String.join("/", String.join("/", partitionLocationParts));

			String q1_createAvroTableStmt = String.format(Q1_CREATE_AVRO_TABLE_QUERY, tableName, schemaTablePostfix,
					partitionDefString, schemaURL);
			String q2_createDataTableStmt = String.format(Q2_CREATE_DATA_TABLE_QUERY, tableName, tablePostfix,
					schemaTablePostfix, storage_format, hdfsTableRootLocation);
			String q3_alterTableStmt = String.format(Q3_ALTER_TABLE_QUERY, tableName, tablePostfix, partitionsString,
					relativeLocation);

			StunHiveClient.ExecuteStatements(hiveUrl, hiveUser, hivePassword, q1_createAvroTableStmt,
					q2_createDataTableStmt, q3_alterTableStmt);

			// Old
			// conn =
			// closer.register(HiveJdbcConnector.newConnectorWithProps(properties));
			// conn.executeStatements(createTableStmt, alterTableStmt,
			// refreshTableStmt);
			LOG.debug("Register done");
		} finally {
			try {
				closer.close();
			} catch (IOException e) {
				LOG.error(e.toString());
			}
		}
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
