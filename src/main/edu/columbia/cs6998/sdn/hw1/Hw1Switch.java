package edu.columbia.cs6998.sdn.hw1;

import java.util.Timer;
import java.util.TimerTask;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.Data;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.threadpool.ThreadPool;

import org.openflow.protocol.OFError;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFFlowRemoved;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.util.HexString;
import org.openflow.util.LRULinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hw1Switch implements IFloodlightModule, IOFMessageListener {
	ThreadPool tp;
    protected static Logger log = LoggerFactory.getLogger(Hw1Switch.class);
    
    // Module dependencies
    protected IFloodlightProviderService floodlightProvider;
    
    /**
     * @param floodlightProvider the floodlightProvider to set
     */
    public void setFloodlightProvider(IFloodlightProviderService floodlightProvider) {
        this.floodlightProvider = floodlightProvider;
    }
    
    @Override
    public String getName() {
        return "monitoringModule";
    }


    // FINAL PROJECT
    protected static final long NUMBER_OF_TEST_PACKET = 20;
    protected static final long PACKET_LOSS_CHECK_DELAY = 1000;
    protected Map<Long, Map<Long, Map<Long, Map<Long, Boolean>>>> packetLossMap;
    protected Map<Long, Map<Long, Long>> delayMap;
    /**
     * Processes a OFPacketIn message. If the switch has learned the MAC to port mapping
     * for the pair it will write a FlowMod for. If the mapping has not been learned the 
     * we will flood the packet.
     * @param sw
     * @param pi
     * @param cntx
     * @return
     */
    private Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi, FloodlightContext cntx) {
        // Read in packet data headers by using OFMatch
        OFMatch match = new OFMatch();
        match.loadFromPacket(pi.getPacketData(), pi.getInPort());
        Long sourceMac = Ethernet.toLong(match.getDataLayerSource());
        Long destMac = Ethernet.toLong(match.getDataLayerDestination());

        if (match.getNetworkProtocol() != (byte)0xfd) {
            return Command.CONTINUE;
        }

        // Now output flow-mod and/or packet
        //    log.info("===================OWN PACKET==================");
        //        Ethernet packet = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
        //        IPv4 ipPacket = (IPv4) packet.getPayload();
        //        byte[] payload = ((Data) ipPacket.getPayload()).getData();
        //        byte[] dpidLoad = new byte[8];
        //        byte[] timeLoad = new byte[8];
        //        byte[] idLoad = new byte[8];
        //        byte[] countLoad = new byte[8];

        //        System.arraycopy(payload, 0, dpidLoad, 0, 8);
        //        System.arraycopy(payload, 8, timeLoad, 0, 8);
        //        System.arraycopy(payload, 16, idLoad, 0, 8);
        //        System.arraycopy(payload, 24, countLoad, 0, 8);

        //        long dpid = ByteBuffer.wrap(dpidLoad).getLong();
        //        long time = ByteBuffer.wrap(timeLoad).getLong();
        //        long id = ByteBuffer.wrap(idLoad).getLong();
        //        long count = ByteBuffer.wrap(countLoad).getLong();
        //        
        //        if (delayMap.get(sw.getId()) == null) delayMap.put(sw.getId(), new ConcurrentHashMap<Long, Long>());
        //        if (delayMap.get(sw.getId()).get(dpid) == null) delayMap.get(sw.getId()).put(dpid, 0L);
        //        
        //        long currentDelay = delayMap.get(sw.getId()).get(dpid);
        //        double alpha = 0.125;
        //        if (currentDelay == 0L) currentDelay = System.nanoTime() - time;
        //        else currentDelay = (long)((1-alpha) * currentDelay) + (long) (alpha * ((System.nanoTime() - time) - currentDelay));
        //        
        //        System.out.println("currentSwitchId=" + sw.getId() + 
        //                ", dpid=" + dpid + 
        //                ", delay=" + currentDelay +
        //                ", id=" + id +
        //                ", count=" + count);
//      //          log.info(   "currentSwitchId=" + sw.getId() + 
//      //                      ", dpid=" + dpid + 
//      //                      ", delay=" + currentDelay +
//      //                      ", id=" + id +
//      //                      ", count=" + count);

        //        if (!packetLossMap.containsKey(sw.getId())) packetLossMap.put(sw.getId(), new ConcurrentHashMap<Long, Map<Long, Map<Long, Boolean>>>());
        //        if (!packetLossMap.get(sw.getId()).containsKey(dpid)) packetLossMap.get(sw.getId()).put(dpid, new ConcurrentHashMap<Long, Map<Long, Boolean>>());
        //        Map<Long, Map<Long, Boolean>> map = packetLossMap.get(sw.getId()).get(dpid);
        //        if (!map.containsKey(id)) map.put(id, new ConcurrentHashMap<Long, Boolean>());
        //        Map<Long, Boolean> set = map.get(id);
        //        class checkPackLossTimerTask extends TimerTask{
        //            long sw;
        //            long dpid;
        //            long id;
        //            checkPackLossTimerTask(long sw, long dpid, long id) {
        //                this.sw = sw;
        //                this.dpid = dpid;
        //                this.id = id;
        //            }
        //            @Override
        //            public void run() {
        //                Map<Long, Boolean> set = packetLossMap.get(sw).get(dpid).get(id);
        //                log.info("PACKET LOSS from " + sw + " to " + dpid + " = " + (1.0 - (double)set.keySet().size() / (double) NUMBER_OF_TEST_PACKET));
        //                packetLossMap.get(sw).get(dpid).remove(id);
        //            }
        //        }
        //        set.put(count, true);
        //        if (set.keySet().size() == 1) {
        //            new Timer().schedule(new checkPackLossTimerTask(sw.getId(), dpid, id), PACKET_LOSS_CHECK_DELAY);
        //        }
        // it's our own packet
        return Command.CONTINUE;
    }


    /**
     * Processes a flow removed message. 
     * @param sw The switch that sent the flow removed message.
     * @param flowRemovedMessage The flow removed message.
     * @return Whether to continue processing this message or stop.
     */
    private Command processFlowRemovedMessage(IOFSwitch sw, OFFlowRemoved flowRemovedMessage) {
        //if (flowRemovedMessage.getCookie() != Hw1Switch.HW1_SWITCH_COOKIE) {
        //    return Command.CONTINUE;
        //}

        //Long sourceMac = Ethernet.toLong(flowRemovedMessage.getMatch().getDataLayerSource());
        //Long destMac = Ethernet.toLong(flowRemovedMessage.getMatch().getDataLayerDestination());

        //if (log.isTraceEnabled()) {
        //    log.trace("{} flow entry removed {}", sw, flowRemovedMessage);
        //}
        return Command.CONTINUE;
    }
    // IOFMessageListener
    
    @Override
    public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
        switch (msg.getType()) {
            case PACKET_IN:
                return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);
            case FLOW_REMOVED:
                return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);
            case ERROR:
                log.info("received an error {} from switch {}", (OFError) msg, sw);
                return Command.CONTINUE;
            default:
                break;
        }
        log.error("received an unexpected message {} from switch {}", msg, sw);
        return Command.CONTINUE;
    }

    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name) {
        return false;
    }

    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name) {
        return false;
    }

    // IFloodlightModule
    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        Collection<Class<? extends IFloodlightService>> l = 
                new ArrayList<Class<? extends IFloodlightService>>();
        return l;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService>
            getServiceImpls() {
        Map<Class<? extends IFloodlightService>,
            IFloodlightService> m = 
                new HashMap<Class<? extends IFloodlightService>,
                    IFloodlightService>();
        return m;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>>
            getModuleDependencies() {
        Collection<Class<? extends IFloodlightService>> l = 
                new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IFloodlightProviderService.class);
        return l;
    }

    protected Runnable testRun;
    @Override
    public void init(FloodlightModuleContext context)
            throws FloodlightModuleException {
    	tp = new ThreadPool();
    	context.addService(IThreadPoolService.class, tp);
    	tp.init(context);
    	tp.startUp(context);
    	
    	
        
    	
    	
        floodlightProvider =
                context.getServiceImpl(IFloodlightProviderService.class);
//        macToSwitchPortMap = 
//                new ConcurrentHashMap<IOFSwitch, Map<Long, Short>>();
//        destinationMap =
//                new ConcurrentHashMap<Long, Set<Long>>();
        packetLossMap =
                new ConcurrentHashMap<Long, Map<Long, Map<Long, Map<Long, Boolean>>>>();
        delayMap = 
        		new ConcurrentHashMap<Long, Map<Long, Long>>();
    }

    @Override
    public void startUp(FloodlightModuleContext context) {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
        floodlightProvider.addOFMessageListener(OFType.ERROR, this);
    }
}
