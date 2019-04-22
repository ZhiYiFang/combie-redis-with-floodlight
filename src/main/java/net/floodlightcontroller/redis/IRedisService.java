package net.floodlightcontroller.redis;

import java.util.List;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.types.NodePortTuple;

public interface IRedisService extends IFloodlightService{
	public List<NodePortTuple> get(FlowKey key);
	public boolean set(FlowKey key, List<NodePortTuple> path);
	public boolean delete();
}
