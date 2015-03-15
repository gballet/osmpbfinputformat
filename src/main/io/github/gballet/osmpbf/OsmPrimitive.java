package io.github.gballet.osmpbf;

public class OsmPrimitive {
	public boolean isNode;
	public double lon;
	public double lat;
	public long id;
	public String tags;
	
	public boolean isNode()
	{
		return isNode;
	}
	
	public OsmPrimitive(long id_, double lon_, double lat_, String tags_)
	{
		id = id_;
		lon = lon_;
		lat = lat_;
		tags = tags_;
		
		isNode = true;
	}
	
	public OsmPrimitive() {
		isNode = false;
	}
}
