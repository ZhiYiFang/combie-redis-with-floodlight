package net.floodlightcontroller.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFlowMod;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.CmdLineSettings;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.core.util.AppCookie;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.routing.ForwardingBase;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.util.FlowModUtils;
import net.floodlightcontroller.util.OFMessageDamper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Redis implements IFloodlightModule, IOFMessageListener, ITopologyListener, IRedisService {
	protected static Logger log = LoggerFactory.getLogger(Redis.class);
	protected IFloodlightProviderService floodlightProvider; // 用来将自身注册成Packet-in消息的监听器
	protected OFMessageDamper messageDamper;// 用来下发flow mod消息
	protected IOFSwitchService switchService;// 用来根据具体的DPID获取相应的switch对象从而下发flow mod消息
	protected ITopologyService topology;// 用来将自身注册成拓扑信息变化的监听器
	protected JedisPool jedisPool;// 用来初始化jedis客户端
	protected static long REDIS_APP_ID = 99;
	private long flowID = 0;

	private static int OFMESSAGE_DAMPER_CAPACITY = 10000;
	private static int OFMESSAGE_DAMPER_TIMEOUT = 250; // ms

	static {
		AppCookie.registerApp(REDIS_APP_ID, "redis");
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IRedisService.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		m.put(IRedisService.class, this);
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IOFSwitchService.class);
		l.add(ITopologyService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		switchService = context.getServiceImpl(IOFSwitchService.class);
		topology = context.getServiceImpl(ITopologyService.class);
		
		// 初始化messageDamper 给switch下发flowmod
		messageDamper = new OFMessageDamper(OFMESSAGE_DAMPER_CAPACITY, EnumSet.of(OFType.FLOW_MOD),
				OFMESSAGE_DAMPER_TIMEOUT);
		try {
			jedisPool = initJedisPool();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
		// 监听packet in消息
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		// 监听拓扑变化消息
		topology.addListener(this);
	}

	// IOFMessageListener

	@Override
	public String getName() {
		return "redis";
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return name.equals("forwarding");// 让redis模块处于forwarding之前来处理这个packet in消息
	}

	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		MacAddress srcMac = null;
		MacAddress dstMac = null;
		IPv4Address srcIp = null;
		IPv4Address dstIp = null;
		switch (msg.getType()) {
		case PACKET_IN:
			// 处理packet in消息
			// 构建FlowKey
			srcMac = eth.getSourceMACAddress();
			dstMac = eth.getDestinationMACAddress();
			if (eth.getEtherType() == EthType.IPv4) {
				IPv4 ip = (IPv4) eth.getPayload();
				srcIp = ip.getSourceAddress();
				dstIp = ip.getDestinationAddress();
			}
			FlowKey key = new FlowKey(srcIp, dstIp, srcMac, dstMac);

			// 从缓存中取出路径消息
			List<NodePortTuple> path = get(key);
			if (path != null) {
				// 如果缓存中存在路径信息就直接下发流表 并停止对这个packet in消息向后续的监听者转发
				sendFlowMod(key, path);
				log.info("Send flowMod messages by using Redis");
				return Command.STOP;
			} else {
				// 否则不做处理交给forwarding模块来处理
				return Command.CONTINUE;
			}
		default:
			break;
		}
		return Command.CONTINUE;
	}

	// ITopologyListener
	@Override
	public void topologyChanged(List<LDUpdate> linkUpdates) {
		log.info("Topology is changed, flush all values");
		// 当拓扑发生变化的时候 清空缓存的flowmod消息
		delete();
	}

	@Override
	public List<NodePortTuple> get(FlowKey key) {
		String realKey = key.getKey();
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			// 生成真正的key
			String str = jedis.get(realKey);
			if (str == null || str.length() <= 0)
				return null;
			List<NodePortTuple> t = stringToList(str);
			return t;
		} finally {
			returnToPool(jedis);
		}
	}

	@Override
	public boolean set(FlowKey key, List<NodePortTuple> path) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource(); // 通过jedisPool获取jedis对象
			String realKey = key.getKey();// 将FlowKey转化成字符串作为key
			jedis.set(realKey, listToString(path)); // 直接调用jedis的set方法来存值
		} finally {
			returnToPool(jedis);
		}
		return false;
	}

	@Override
	public boolean delete() {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			jedis.flushAll();
			return true;
		} finally {
			returnToPool(jedis);
		}
	}

	private String listToString(List<NodePortTuple> mods) {
		return new Gson().toJson(mods);
	}

	@SuppressWarnings(value = { "unchecked" })
	private List<NodePortTuple> stringToList(String str) {
		JsonParser jp = new JsonParser();
		Gson gson = new Gson();
		try {
			JsonArray array = jp.parse(str).getAsJsonArray();
			Iterator<JsonElement> it = array.iterator();
			List<NodePortTuple> ret = new ArrayList<>();
			while (it.hasNext()) {
				ret.add(gson.fromJson(it.next().getAsJsonObject(), NodePortTuple.class));
			}
			return ret;
		} catch (Exception e) {
			return null;
		}
	}

	private void returnToPool(Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}

	private JedisPool initJedisPool() throws IOException {
		// 初始化JedisPoolConfig
		CmdLineSettings cmdLineSettings = new CmdLineSettings();
		File confFile = new File(cmdLineSettings.getModuleFile());
		FileInputStream fis = null;
		fis = new FileInputStream(confFile);
		if (fis != null) {
			Properties fprop = new Properties();
			fprop.load(fis);
			String host = fprop.getProperty("redis.host");
			String port = fprop.getProperty("redis.port");
			String timeout = fprop.getProperty("redis.timeout");
			String poolMaxTotal = fprop.getProperty("redis.poolMaxTotal");
			String poolMaxIdle = fprop.getProperty("redis.poolMaxIdle");
			String poolMaxWait = fprop.getProperty("redis.poolMaxWait");
			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxIdle(Integer.parseInt(poolMaxIdle));
			poolConfig.setMaxTotal(Integer.parseInt(poolMaxTotal));
			poolConfig.setMaxWaitMillis(Integer.parseInt(poolMaxWait) * 1000);
			JedisPool jp = new JedisPool(poolConfig, host, Integer.parseInt(port), Integer.parseInt(timeout) * 1000);
			fis.close();
			return jp;
		}
		fis.close();
		return null;
	}

	private boolean sendFlowMod(FlowKey key, List<NodePortTuple> switchPortList) {
		for (int indx = switchPortList.size() - 1; indx > 0; indx -= 2) {
			// 一个交换机一个进口一个出口 出现两次
			DatapathId switchDPID = switchPortList.get(indx).getNodeId();
			IOFSwitch sw = switchService.getSwitch(switchDPID);

			// flowMod 消息builder
			OFFlowMod.Builder fmb = sw.getOFFactory().buildFlowAdd();

			// 转发动作builder
			OFActionOutput.Builder aob = sw.getOFFactory().actions().buildOutput();
			List<OFAction> actions = new ArrayList<>();

			// 匹配域builder
			Match.Builder mb = OFFactories.getFactory(sw.getOFFactory().getVersion()).buildMatch();

			// 交换机的出入口
			OFPort outPort = switchPortList.get(indx).getPortId();
			OFPort inPort = switchPortList.get(indx - 1).getPortId();

			// 设置匹配域
			if(key.getSrcIP()!=null && key.getDstIP()!=null) {
				mb.setExact(MatchField.ETH_TYPE, EthType.IPv4);
				mb.setExact(MatchField.IPV4_SRC, key.getSrcIP());
				mb.setExact(MatchField.IPV4_DST, key.getDstIP());
			}
			
			mb.setExact(MatchField.ETH_DST, key.getDstMac());
			mb.setExact(MatchField.ETH_SRC, key.getSrcMac());
			mb.setExact(MatchField.IN_PORT, inPort);

			// 设值转发动作
			aob.setPort(outPort);
			aob.setMaxLen(Integer.MAX_VALUE);
			actions.add(aob.build());

			// 给flowmodbuilder设置匹配域 时间 优先级等
			fmb.setMatch(mb.build()).setIdleTimeout(ForwardingBase.FLOWMOD_DEFAULT_IDLE_TIMEOUT)
					.setHardTimeout(ForwardingBase.FLOWMOD_DEFAULT_HARD_TIMEOUT).setBufferId(OFBufferId.NO_BUFFER)
					.setOutPort(outPort).setPriority(ForwardingBase.FLOWMOD_DEFAULT_PRIORITY)
					.setCookie(AppCookie.makeCookie(REDIS_APP_ID, flowID++));

			// 给flowmodbuilder设置转发动作
			FlowModUtils.setActions(fmb, actions, sw);

			// 如果switch支持的是非1.0的Openflow协议那么就需要设置tableId 默认设置为0
			if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_10) != 0) {
				fmb.setTableId(TableId.ZERO);
			}
			// 下发flowmod
			messageDamper.write(sw, fmb.build());
		}
		return true;
	}

}
