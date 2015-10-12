OsmPbfInputFormat
=================

An Hadoop API Inputformat for reading Open Street Map Protobuf Files. Currently parsing Nodes and Ways is implemented as a Hadoop Inpput Format and using Pig. Reading relations are not currently implemented. 

Installation
------------

This library relies on the Hadoop client library, Google protobuf (>= 2.5.0) and the osmpbf library. All dependencies are summarized in the `pom.xml` file, so that maven takes care of them for you. Simply type:

	mvn package

You should then find all the relevant jars in the `build` subdirectory.

Usage
-----

Simply set the input format in your job configuration:

	jobConf.setInputFormat(FirstMapReduceInputFormat.class);

Then, make sure your mapper class has `LongWritable` as its key class and `OsmPrimitive` as its value class. For instance, here is the example of a class that divides the map in a grid of 2048x2048 blocks and returns the block number as key with the node count (always 1) as value:

	public class UselessExample extends Mapper<LongWritable, OsmPrimitive, LongWritable, LongWritable> {
		private static final SIDE_BREAKDOWN = 2048;

		public void map(LongWritable key, OsmPrimitive value, Context context) {
			long x = (value.lon + 180) * SIDE_BREAKDOWN / 360;
			long y = (value.lat + 90) * SIDE_BREAKDOWN / 180;

			context.write(new LongWritable(y*SIDE_BREAKDOWN + x), new LongWritable(1));
		}
	}

In the reducer, one can then count the number of nodes per tile.

	
There is a public integer member `parseType` which is one of:
- 1 : Node
- 2 : Way
- 3 : Relation (**not yet implemented**)


This will parse the file according to the desired schema.

Pig
---

A Pig Loader has been included which will enable loading of PBF datasets into Pig. For example:

`pbf_nodes = LOAD '$inputFile' USING io.github.gballet.pig.OSMPbfPigLoader('1') AS (id:long, lat:double, lon:double, nodeTags:map[]);`

OR

`pbf_ways = LOAD '$inputFile' USING io.github.gballet.pig.OSMPbfPigLoader('2') AS (id:long, nodes:bag{(pos:int, nodeid:long)}, tags:map[]);`

Notes
-----

 * This package relies on [crosby.binary.file](https://github.com/scrosby/OSM-binary), which isn't yet configured to work as a Maven repository. While the pull request gets written and integrated, a pre-compiled version of the library is kept in this repository.

Contributing
------------

Simply fork the repository, create a new branch, work in that branch then send me a pull request.
