package io.github.gballet.pig;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import io.github.gballet.mapreduce.input.OsmPbfInputFormat;
import io.github.gballet.osmpbf.OsmPrimitive;

/**
 * Parses an PBF input file
 *
 */
public class OSMPbfPigLoader extends LoadFunc {
    protected RecordReader in = null;
//    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Tuple getNext() throws IOException {
        try {
        	if (!in.nextKeyValue())
        		return null;
            in.nextKeyValue();
            OsmPrimitive value = (OsmPrimitive) in.getCurrentValue();
            // do something with value
            Tuple t =  mTupleFactory.newTuple(4);
             t.append(value.id);
            t.append(value.lat);
            t.append(value.lon);
            t.append(value.tags);
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    @Override
    public OsmPbfInputFormat getInputFormat() {
        return new OsmPbfInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }
    
    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

}