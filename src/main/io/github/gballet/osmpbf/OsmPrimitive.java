package io.github.gballet.osmpbf;

import java.util.List;

public class OsmPrimitive {
	public boolean isNode;
	public boolean isWay;
	public double lon;
	public double lat;
	public long id;
	public String tags;
	public List<Long> wayNodeList;
	
	public boolean isNode()
	{
		return isNode;
	}
	
	public boolean isWay()
	{
		return isWay;
	}
	
	public OsmPrimitive(long id_, double lon_, double lat_, String tags_)
	{
		id = id_;
		lon = lon_;
		lat = lat_;
		tags = tags_;
		
		isNode = true;
		isWay = false;
	}
	
	//way contstructor	
	public OsmPrimitive(long id_, List<Long> nodeIDList, String allWayTags) {
		id = id_;
		wayNodeList = nodeIDList;
		tags = allWayTags;
		
		isNode = false;
		isWay = true;
	}
	
	public OsmPrimitive() {
		isNode = false;
	}


}
