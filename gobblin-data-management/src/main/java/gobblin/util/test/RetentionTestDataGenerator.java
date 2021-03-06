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
package gobblin.util.test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.PathUtils;


/**
 * A class to setup files and folders for a retention test.
 * Class reads a setup-validate.conf file at <code>testSetupConfPath</code> to create data for a retention test.
 * The config file needs to be in HOCON and parsed using {@link ConfigFactory}.
 * All the paths listed at {@link #TEST_DATA_CREATE_KEY} in the config file will be created under a <code>testTempDirPath</code>.
 *  <br>
 * After retention job is run on this data, the {@link #validate()} method can be used to validate deleted files and retained files
 * listed in {@link #TEST_DATA_VALIDATE_DELETED_KEY} and {@link #TEST_DATA_VALIDATE_RETAINED_KEY} respectively.
 *  <br>
 *  Below is the format of the config file this class parses.
 *  <br>
 * <pre>
 * gobblin.test : {

    // Time the system clock is set before starting the test
    currentTime : "02/19/2016 11:00:00"

    // Paths to create for the test
    //{path: path of test file in the dataset, modTime: set the modification time of this path in "MM/dd/yyyy HH:mm:ss"}
    create : [
      {path:"/user/gobblin/Dataset1/Version1", modTime:"02/10/2016 10:00:00"},
      {path:"/user/gobblin/Dataset1/Version2", modTime:"02/11/2016 10:00:00"},
      {path:"/user/gobblin/Dataset1/Version3", modTime:"02/12/2016 10:00:00"},
      {path:"/user/gobblin/Dataset1/Version4", modTime:"02/13/2016 10:00:00"},
      {path:"/user/gobblin/Dataset1/Version5", modTime:"02/14/2016 10:00:00"}
    ]

    // Validation configs to use after the test
    validate : {

      // Paths that should exist after retention is run
      retained : [
        {path:"/user/gobblin/Dataset1/Version4", modTime:"02/13/2016 10:00:00"},
        {path:"/user/gobblin/Dataset1/Version5", modTime:"02/14/2016 10:00:00"}
      ]

      // Paths that should be deleted after retention is run
      deleted : [
        {path:"/user/gobblin/Dataset1/Version1", modTime:"02/10/2016 10:00:00"},
        {path:"/user/gobblin/Dataset1/Version2", modTime:"02/11/2016 10:00:00"},
        {path:"/user/gobblin/Dataset1/Version3", modTime:"02/12/2016 10:00:00"}
      ]
    }
}
 * </pre>
 */
public class RetentionTestDataGenerator {

  private static final String DATA_GENERATOR_KEY = "gobblin.test";
  private static final String DATA_GENERATOR_PREFIX = DATA_GENERATOR_KEY + ".";
  private static final String TEST_CURRENT_TIME_KEY = DATA_GENERATOR_PREFIX + "currentTime";
  private static final String TEST_DATA_CREATE_KEY = DATA_GENERATOR_PREFIX + "create";
  private static final String TEST_DATA_VALIDATE_RETAINED_KEY = DATA_GENERATOR_PREFIX + "validate.retained";
  private static final String TEST_DATA_VALIDATE_DELETED_KEY = DATA_GENERATOR_PREFIX + "validate.deleted";

  private static final String TEST_DATA_PATH_LOCAL_KEY = "path";
  private static final String TEST_DATA_MOD_TIME_LOCAL_KEY = "modTime";

  private static final DateTimeFormatter FORMATTER = DateTimeFormat.
      forPattern("MM/dd/yyyy HH:mm:ss").withZone(DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME));
  private final Path testTempDirPath;
  private final FileSystem fs;
  private final Config setupConfig;

  /**
   * @param testTempDirPath under which all test files are created on the FileSystem
   * @param testSetupConfPath setup config file path in classpath
   */
  public RetentionTestDataGenerator(Path testTempDirPath, Path testSetupConfPath, FileSystem fs) {
    this.fs = fs;
    this.testTempDirPath = testTempDirPath;
    this.setupConfig =
        ConfigFactory.parseResources(PathUtils.getPathWithoutSchemeAndAuthority(testSetupConfPath).toString());
    if (!this.setupConfig.hasPath(DATA_GENERATOR_KEY)) {
      throw new RuntimeException(String.format("Failed to load setup config at %s", testSetupConfPath.toString()));
    }
  }

  /**
   * Create all the paths listed under {@link #TEST_DATA_CREATE_KEY}. If a path's config has a {@link #TEST_DATA_MOD_TIME_LOCAL_KEY} specified,
   * the modification time of this path is updated to this value.
   */
  public void setup() throws IOException {

    if (this.setupConfig.hasPath(TEST_CURRENT_TIME_KEY)) {
      DateTimeUtils.setCurrentMillisProvider(new FixedThreadLocalMillisProvider(FORMATTER.parseDateTime(
          setupConfig.getString(TEST_CURRENT_TIME_KEY)).getMillis()));
    }

    List<? extends Config> createConfigs = setupConfig.getConfigList(TEST_DATA_CREATE_KEY);

    Collections.sort(createConfigs, new Comparator<Config>() {
      @Override
      public int compare(Config o1, Config o2) {
        return o1.getString(TEST_DATA_PATH_LOCAL_KEY).compareTo(o2.getString(TEST_DATA_PATH_LOCAL_KEY));
      }
    });

    for (Config fileToCreate : createConfigs) {
      Path fullFilePath =
          new Path(testTempDirPath, PathUtils.withoutLeadingSeparator(new Path(fileToCreate
              .getString(TEST_DATA_PATH_LOCAL_KEY))));

      if (!this.fs.mkdirs(fullFilePath)) {
        throw new RuntimeException("Failed to create test file " + fullFilePath);
      }
      if (fileToCreate.hasPath(TEST_DATA_MOD_TIME_LOCAL_KEY)) {
        File file = new File(PathUtils.getPathWithoutSchemeAndAuthority(fullFilePath).toString());
        boolean modifiedFile =
            file.setLastModified(FORMATTER.parseMillis(fileToCreate.getString(TEST_DATA_MOD_TIME_LOCAL_KEY)));
        if (!modifiedFile) {
          throw new IOException(String.format("Unable to set the last modified time for file %s!", file));
        }
      }
    }
  }

  /**
   * Validate that all paths in {@link #TEST_DATA_VALIDATE_DELETED_KEY} are deleted and
   * all paths in {@link #TEST_DATA_VALIDATE_RETAINED_KEY} still exist
   */
  public void validate() throws IOException {

    List<? extends Config> retainedConfigs = setupConfig.getConfigList(TEST_DATA_VALIDATE_RETAINED_KEY);
    for (Config retainedConfig : retainedConfigs) {
      Path fullFilePath =
          new Path(testTempDirPath, PathUtils.withoutLeadingSeparator(new Path(retainedConfig
              .getString(TEST_DATA_PATH_LOCAL_KEY))));
      Assert.assertTrue(this.fs.exists(fullFilePath),
          String.format("%s should not be deleted", fullFilePath.toString()));
    }

    List<? extends Config> deletedConfigs = setupConfig.getConfigList(TEST_DATA_VALIDATE_DELETED_KEY);
    for (Config retainedConfig : deletedConfigs) {
      Path fullFilePath =
          new Path(testTempDirPath, PathUtils.withoutLeadingSeparator(new Path(retainedConfig
              .getString(TEST_DATA_PATH_LOCAL_KEY))));
      Assert.assertFalse(this.fs.exists(fullFilePath), String.format("%s should be deleted", fullFilePath.toString()));
    }

    this.cleanup();
  }

  public void cleanup() throws IOException {
    if (this.fs.exists(testTempDirPath)) {
      if (!this.fs.delete(testTempDirPath, true)) {
        throw new IOException("Failed to clean up path " + this.testTempDirPath);
      }
    }
  }

  /**
   * A Joda time {@link MillisProvider} used provide a mock fixed current time.
   * Needs to be thread local as TestNg tests may run in parallel
   */
  public static class FixedThreadLocalMillisProvider implements MillisProvider {

    private ThreadLocal<Long> currentTimeThreadLocal = new ThreadLocal<Long>() {
      @Override
      protected Long initialValue() {
        return System.currentTimeMillis();
      }
    };

    public FixedThreadLocalMillisProvider(Long millis) {
      currentTimeThreadLocal.set(millis);
    }

    @Override
    public long getMillis() {
      return currentTimeThreadLocal.get();
    }
  }
}
