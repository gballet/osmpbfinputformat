package io.github.gballet.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;

import io.github.gballet.mapreduce.input.OsmPbfInputFormat;
import io.github.gballet.mapreduce.input.OsmPbfRecordReader;
import io.github.gballet.osmpbf.OsmPrimitive;

/**
 * Parses an PBF input file
 *
 */
public class OSMPbfPigLoader extends LoadFunc {
    protected RecordReader in = null;
//    private ArrayList<Object> mProtoTuple = null;
    private int parseType;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    
    public OSMPbfPigLoader(String parseType) {
    	// can only have pig arguments as Strings so convert to int
        this.parseType = Integer.parseInt(parseType);
      }
    
    @Override
    public Tuple getNext() throws IOException {
        try {
        	if (!in.nextKeyValue())
        		return null;
            in.nextKeyValue();
            OsmPrimitive value = (OsmPrimitive) in.getCurrentValue();
            
            if (parseType == OsmPbfRecordReader.OsmPbfReaderParseType.NODE) {
            // do something with value
            	Tuple t =  mTupleFactory.newTuple(4);
             	t.append(value.id);
             	t.append(value.lat);
            	t.append(value.lon);
            	t.append(value.tags);
            return t;
            }
            
            if (parseType == OsmPbfRecordReader.OsmPbfReaderParseType.WAY) {
            	List<Long> nodes = value.wayNodeList;
                DataBag nodesBag = BagFactory.getInstance().newDefaultBag();
                for (int i = 0; i < nodes.size(); i++) {
                	long nodeID = nodes.get(i);
                    int node_pos = (int) (nodesBag.size() + 1);
                    Tuple nodeTuple = TupleFactory.getInstance().newTuple(2);
                    nodeTuple.set(0, node_pos);
                    nodeTuple.set(1, nodeID);
                    nodesBag.add(nodeTuple);
                }
            	Tuple t =  mTupleFactory.newTuple(3);
                t.append(value.id);
                t.append(nodesBag);
                t.append(value.tags);
            return t;
            }
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
		return null;
    }

    @Override
    public OsmPbfInputFormat getInputFormat() {
        return new OsmPbfInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
    	((OsmPbfRecordReader) reader).parseType = parseType;
        in = reader;
    }
    
    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

}