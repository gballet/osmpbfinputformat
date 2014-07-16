package io.github.gballet.mapreduce.input;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

import io.github.gballet.osmpbf.OsmPrimitive;
import io.github.gballet.mapreduce.input.*;

public class OsmPbfInputFormat extends FileInputFormat<LongWritable,OsmPrimitive> {

	@Override
	public RecordReader createRecordReader(InputSplit split,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new OsmPbfRecordReader();
	}
}
