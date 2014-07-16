package io.github.gballet.mapreduce.input;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;

import com.google.protobuf.ByteString;

import crosby.binary.Fileformat.Blob;
import crosby.binary.Fileformat.BlobHeader;
import crosby.binary.Osmformat.PrimitiveBlock;
import crosby.binary.Osmformat.PrimitiveGroup;

import io.github.gballet.osmpbf.OsmPrimitive;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class OsmPbfRecordReader extends RecordReader<LongWritable, OsmPrimitive> {
	
	private FSDataInputStream fileFD;
	private long start;
	private long end;
	private long pos;
	private double lastLon;
	private double lastLat;
	private long lastId;
	private PrimitiveBlock currentPB;
	private PrimitiveGroup currentPG;
	private int currentPGIndex;
	private int nNodes;
	private OsmPrimitive currentPrimitive;

	private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

	@Override
	public void close() throws IOException {
		fileFD.close();			
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return new LongWritable(pos);
	}

	@Override
	public OsmPrimitive getCurrentValue() throws IOException, InterruptedException {
		/* if (currentPB == null)  Rely on NullPointerException to provide enough information */

		/* if (currentPrimitive == null) idem */

		return currentPrimitive;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return ((float)pos-start)/((float)end-start);
	}
	
	/**
	 * Since the split might end up in the middle of an image, there is a need to look
	 * for the start of the next block. The block that is split will be read by the 
	 * RecordReader in charge of the previous split. 
	 * 
	 * @param startOffset The offset in the file where the search begins
	 * @throws IOException
	 */
	private void seekNextFileBlockStart(long startOffset) throws IOException {
		fileFD.seek(startOffset);
		//int bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
		
		final byte[] signature = "OSMData".getBytes();
		int sigPos = 0;
		boolean sigFound = false;
		
		/* Search of 'OSMData' in the stream */
		byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
		
		do {
			long length = fileFD.read(buffer);
			for (int bufPos=0; bufPos<length && !sigFound; bufPos++, pos++)
			{
				if (signature[sigPos] == buffer[bufPos])
					sigPos++;
				else
					sigPos = 0;
				
				if (sigPos == signature.length) {
					sigFound = true;
					pos -= signature.length + 6;
				}
			}
		} while (fileFD.available() > 0 && !sigFound);


		/*
		 * The buffer will be discarded, but it's not a big deal as it should still be present
		 * in the buffer cache of the OS.
		 */
		fileFD.seek(pos);
	}
	
	/**
	 * Get the file block header. There is no real need to get that block, except for
	 * checking that this is an "OSMData" file block, that contains primitives. 
	 * 
	 * @return
	 * @throws IOException
	 */
	private BlobHeader readHeader() throws IOException
	{
		int headerSize = fileFD.readInt();
		byte[] buf = new byte[headerSize];
		fileFD.readFully(buf);
		
		BlobHeader header = BlobHeader.parseFrom(buf);
		
		return header;
	}
	
	/**
	 * Decode a File Block at @pos and prepare internal data structures to read
	 * primitives from it.
	 */
	private void loadFileBlock() throws IOException, DataFormatException {

		/* At this point, pos is at the beginning of a File block */
		
		/* Read the file block header */
		final BlobHeader header = readHeader();
		
		// TODO be resilient for invalid headers, a while loop with hasDataSize and seekNextFileBlockStart
		byte[] buf = new byte[header.getDatasize()];
		fileFD.readFully(buf);
		Blob blob = Blob.parseFrom(buf);

		// TODO Implement other compression formats
		if (blob.hasZlibData())
		{
			ByteString zlibData = blob.getZlibData();
			Inflater inflater = new Inflater();
			inflater.setInput(zlibData.toByteArray());
			byte[] output = new byte[blob.getRawSize()];
			inflater.inflate(output);
			
			currentPB = PrimitiveBlock.parseFrom(output);
			currentPG = currentPB.getPrimitivegroup(0);
			currentPGIndex = 0;
			nNodes 	  = 0;
		}
		
		pos = fileFD.getPos();
	}

	private int count;
	
	private boolean loadDenseNode() {
		/* If we have reached the end of a primitive group, indicate it */
		if (currentPG.getDense().getIdCount() == 0 || currentPG.getDense().getIdCount() <= nNodes) {
			return false;
		}

		/* Since dense nodes are delta-encoded, reset the initial values */
		if (nNodes == 0)
		{
			lastId  = 0;
			lastLon = 0;
			lastLat = 0;
		}

		lastLon += 0.000000001 * (currentPB.getLonOffset() + currentPB.getGranularity() * currentPG.getDense().getLon(nNodes));
		lastLat += 0.000000001 * (currentPB.getLatOffset() + currentPB.getGranularity() * currentPG.getDense().getLat(nNodes));
		lastId  += currentPG.getDense().getId(nNodes);
		currentPrimitive = new OsmPrimitive(lastId, lastLon, lastLat);

		nNodes++;

		return true;
	}

	private boolean loadPrimitiveGroup() {
		currentPG = currentPB.getPrimitivegroup(currentPGIndex);
		nNodes = 0;
		return loadDenseNode();
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		FileSplit split = (FileSplit) genericSplit;

		Configuration conf = context.getConfiguration();
		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(conf);
		
		start  = split.getStart();
		end    = start + split.getLength();
		pos    = start;
		fileFD = fs.open(file);
		
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
		if (codec != null) {
			System.err.println("Error! We are using a compressed codec!");
		}
		
		try {
			/*
			 * In all probability, the split will cross a File block boundary. Because
			 * of that - and unless the split starts indeed with an OSM File Block -
			 * the first block of the split is ignored. 
			 */
			seekNextFileBlockStart(pos);

			/* Only attempt to load if a block has been found */
			if (pos < end)
				loadFileBlock(); /* if the block overflows the split, pos will be greater than end after this */

				currentPGIndex = 0;
				currentPG = currentPB.getPrimitivegroup(currentPGIndex);
				nNodes = 0;

		} catch (DataFormatException e) {
			// TODO Let the system report it to the user
			e.printStackTrace();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		/* For the moment, ignore all blocks that do not have dense nodes */
		if (currentPG.getDense().getIdCount() > 0 && currentPG.getDense().getIdCount() > nNodes) {
			if (loadDenseNode()) {
				return true;
			}
		}
		
		/* Move to the next primitive group, if available */
		while (++currentPGIndex < currentPB.getPrimitivegroupCount()) {
			if (loadPrimitiveGroup())
				return true;
		}

		/*
		 * At this stage, there are two possibilities: either the cursor is past the end of
		 * the split, and therefore the decoder is done with this split. Otherwise, loadFieldBlock()
		 * has been called and we know for sure that there is at least one node to process.
		 */
		while (pos < end)	{
			/* The following file block is right after this one, so no need to seek it. */
			try {
				loadFileBlock();

				currentPGIndex = 0;

				/* A new Primitive group has just been started, so read the first dense node */
				if (loadPrimitiveGroup()) {
					return true;
				}
			} catch(Exception e) {
				System.err.println("Error loading the next file block, position=" + pos);
				return false;
			}
		}

		System.out.println("returning false");
		return false;
	}
}
