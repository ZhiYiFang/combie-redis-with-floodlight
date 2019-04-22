package net.floodlightcontroller.redis;

import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.MacAddress;

public class FlowKey {

	private IPv4Address srcIP;
	private IPv4Address dstIP;
	private MacAddress srcMac;
	private MacAddress dstMac;
	
	@Override
	public String toString() {
		return "FlowKey [srcIP=" + srcIP + ", dstIP=" + dstIP + ", srcMac=" + srcMac + ", dstMac=" + dstMac + "]";
	}
	public String getKey() {
		return srcIP+":"+dstIP+":"+srcMac+":"+dstMac;
	}
	public FlowKey(IPv4Address srcIP, IPv4Address dstIP, MacAddress srcMac, MacAddress dstMac) {
		super();
		this.srcIP = srcIP;
		this.dstIP = dstIP;
		this.srcMac = srcMac;
		this.dstMac = dstMac;
	}
	public IPv4Address getSrcIP() {
		return srcIP;
	}
	public void setSrcIP(IPv4Address srcIP) {
		this.srcIP = srcIP;
	}
	public IPv4Address getDstIP() {
		return dstIP;
	}
	public void setDstIP(IPv4Address dstIP) {
		this.dstIP = dstIP;
	}
	public MacAddress getSrcMac() {
		return srcMac;
	}
	public void setSrcMac(MacAddress srcMac) {
		this.srcMac = srcMac;
	}
	public MacAddress getDstMac() {
		return dstMac;
	}
	public void setDstMac(MacAddress dstMac) {
		this.dstMac = dstMac;
	}
	
	
	
}
