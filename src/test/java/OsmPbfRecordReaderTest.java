package io.github.gballet.mapreduce.osmpbf.test;

import io.github.gballet.mapreduce.input.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.security.Credentials;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.io.IOException;
import java.io.File;

public class OsmPbfRecordReaderTest {
	private static double testLon = 8.6726849;
	private static double testLat = 45.4668769;

	private static String testFileName = "temp.dat";

	/* Mocks a map-reduce environment and return the correct JobConf */
	private class DummyContext implements TaskAttemptContext {
		private JobConf conf;
	
		public DummyContext(JobConf conf_) {
			conf = conf_;
		}

		@Override
		public Path[] getArchiveClassPaths() {
			return null;
		}

		@Override
		public String[] getArchiveTimestamps() {
			return null;
		}

		@Override
		public URI[] getCacheArchives() throws IOException {
			return null;
		}

		@Override
		public URI[] getCacheFiles() throws IOException {
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public Configuration getConfiguration() {
			return conf;
		}

		@Override
		public Credentials getCredentials() {
			return null;
		}

		@Override
		public Path[] getFileClassPaths() {
			return null;
		}

		@Override
		public String[] getFileTimestamps() {
			return null;
		}

		@Override
		public RawComparator<?> getGroupingComparator() {
			return null;
		}

		@Override
		public Class<? extends InputFormat<?, ?>> getInputFormatClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public String getJar() {
			return null;
		}

		@Override
		public JobID getJobID() {
			return null;
		}

		@Override
		public String getJobName() {
			return null;
		}

		@Override
		public boolean getJobSetupCleanupNeeded() {
			return false;
		}

		@Override
		@Deprecated
		public Path[] getLocalCacheArchives() throws IOException {
			return null;
		}

		@Override
		@Deprecated
		public Path[] getLocalCacheFiles() throws IOException {
			return null;
		}

		@Override
		public Class<?> getMapOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return null;
		}

		@Override
		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public int getMaxMapAttempts() {
			return 0;
		}

		@Override
		public int getMaxReduceAttempts() {
			return 0;
		}

		@Override
		public int getNumReduceTasks() {
			return 0;
		}

		@Override
		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<?> getOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getOutputValueClass() {
			return null;
		}

		@Override
		public Class<? extends Partitioner<?, ?>> getPartitionerClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public boolean getProfileEnabled() {
			return false;
		}

		@Override
		public String getProfileParams() {
			return null;
		}

		@Override
		public IntegerRanges getProfileTaskRange(boolean arg0) {
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public RawComparator<?> getSortComparator() {
			return null;
		}

		@Override
		@Deprecated
		public boolean getSymlink() {
			return false;
		}

		@Override
		public boolean getTaskCleanupNeeded() {
			return false;
		}

		@Override
		public String getUser() {
			return null;
		}

		@Override
		public Path getWorkingDirectory() throws IOException {
			return null;
		}

		@Override
		public void progress() {
		}

		@Override
		public Counter getCounter(Enum<?> arg0) {
			return null;
		}

		@Override
		public Counter getCounter(String arg0, String arg1) {
			return null;
		}

		@Override
		public float getProgress() {
			return 0;
		}

		@Override
		public String getStatus() {
			return null;
		}

		@Override
		public TaskAttemptID getTaskAttemptID() {
			return null;
		}

		@Override
		public void setStatus(String arg0) {
	
		}
		
	}

	@Test
	public void testOneDenseEntry() throws Exception {
		Assert.assertNotNull("Test file missing", getClass().getResource("/" + testFileName));

		Configuration config = new Configuration();
		JobConf conf = new JobConf(config, this.getClass());

		OsmPbfRecordReader rr = new OsmPbfRecordReader();

		/* Build the input split from scratch */
		String[] hosts = { "localhost" };
		FileSplit fsplit = new FileSplit(new Path(getClass().getResource("/" + testFileName).toString()), 0, 104944, hosts);

		rr.initialize((InputSplit) fsplit, new DummyContext(conf));

		Assert.assertTrue(rr.nextKeyValue() == true);
		Assert.assertTrue(rr.getCurrentValue().lon == testLon);
		Assert.assertTrue(rr.getCurrentValue().lat == testLat);
		for (int i=0; i<7999; i++)
			Assert.assertTrue(rr.nextKeyValue() == true);

		Assert.assertTrue(rr.nextKeyValue() == false);
	}
}
