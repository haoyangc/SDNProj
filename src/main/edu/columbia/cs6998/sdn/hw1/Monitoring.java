package edu.columbia.cs6998.sdn.hw1;

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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.test.PacketFactory;
import net.floodlightcontroller.packet.Data;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPacket;
import net.floodlightcontroller.packet.IPv4;

import org.openflow.protocol.OFError;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFFlowRemoved;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.statistics.OFFlowStatisticsRequest;
import org.openflow.protocol.statistics.OFStatisticsType;
import org.openflow.util.HexString;
import org.openflow.util.LRULinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitoring implements IFloodlightModule, IOFMessageListener {
    // Module dependencies
    protected IFloodlightProviderService floodlightProvider;

    protected static Logger log = LoggerFactory.getLogger(Monitoring.class);

    boolean increaseTimer;
    boolean decreaseTimer;
    //Timer t;
    //python:
    //from pox.lib.recoco import Timer
    // 
    //def handle_timer_elapse (message):
    //  print "I was told to tell you:", message
    // 
    //Timer(10, handle_timer_elapse, args = ["Hello"])
    // 
    //# Prints out "I was told to tell you: Hello" in 10 seconds
    //https://openflow.stanford.edu/display/ONL/POX+Wiki#POXWiki-Threads%2CTasks%2CandTimers%3Apox.lib.recoco
    Timer timer;
    TimerTask task = new TimerTask() {
        @Override
        public void run() {
                //run monitoring
                runMonitoring();
        }
    };
    long interval;
    /*
     * interval in milisecond
     */
    protected void reschedule() {
        task.cancel();
        timer.schedule(task, this.interval, this.interval);
    }
    protected void adaptiveTimer() {
        boolean changed = false;
        if (this.increaseTimer == true) {
            this.interval /= 2.0;
            changed = true;
        } else if (this.decreaseTimer == true) {
            this.interval *= 1.125;
            changed = true;
        }
        //#maximize the interval
        if (this.interval > 60*1000)
            this.interval = 60*1000;
        //#minimize the interval
        if (this.interval < 1*1000)
            this.interval = 1*1000;
        
        //change interval by rescheduling
        if (changed) {
            reschedule();
            //this.t.cancel();
            //this.t = new TimerTask();
            //timer.schedule(timerTask, delay);
        }
        //#Reset input from received flowstats
        this.increaseTimer = false;
        this.decreaseTimer = true;
    }
    protected void roundRobin() {
        //neglected for now
    }
    protected Map<Long, IOFSwitch> switches;
    class Path {
        long dst; //pid of the last switch
        long src; //pid of the first switch
        short first_port;
    };
    Set<Path> monitored_paths = new HashSet<Path>();
    protected void lastSwitch() {
        Map<Long, Boolean> switchRead = new ConcurrentHashMap<Long, Boolean>();
        for (long dpid : switches.keySet()) {
            switchRead.put(dpid, false);
        }
        
        for (Path p : monitored_paths) {
            if (switchRead.get(p.dst) == false) {
                switchRead.put(p.dst, true);
                OFStatisticsRequest req = new OFStatisticsRequest();
                req.setStatisticType(OFStatisticsType.FLOW);
                //python:
                //msg = of.ofp_stats_request(body=of.ofp_flow_stats_request())
                //switches[p.dst].connection.send(msg)
                IOFSwitch sw = switches.get(p.dst);
                try {
                    sw.write(req, null);
                } catch (IOException e) {
                    log.error("Failed to write {} to switch {}: {}", new Object[] { req, sw, e });
                }
                
                // a more specific request
                ////https://groups.google.com/a/openflowhub.org/forum/#!topic/floodlight-dev/TQ_SZnykFXY
                //int requestLength = req.getLengthU();
                ////TODO: Complete this section for the request
                //short outPort = 1;
                //OFFlowStatisticsRequest specificReq = new OFFlowStatisticsRequest();
                //specificReq.setMatch(new OFMatch().setWildcards(0xffffffff));
                //specificReq.setOutPort(outPort);
            }
            if (switchRead.get(p.src) == false) {
                switchRead.put(p.src, true);
                OFStatisticsRequest req = new OFStatisticsRequest();
                req.setStatisticType(OFStatisticsType.FLOW);

                IOFSwitch sw = switches.get(p.src);
                try {
                    sw.write(req, null);
                } catch (IOException e) {
                    log.error("Failed to write {} to switch {}: {}", new Object[] { req, sw, e });
                }
            }
        }
    }

    public byte[] intToBytes( final int i ) {
        ByteBuffer bb = ByteBuffer.allocate(4); 
        bb.putInt(i); 
        return bb.array();
    }
    public byte[] longToBytes( final long l ) {
        ByteBuffer bb = ByteBuffer.allocate(8); 
        bb.putLong(l); 
        return bb.array();
    }
    
    /*
     * The datapath_id field uniquely identifies a datapath. The lower 48 bits are
     * intended for the switch MAC address, while the top 16 bits are up to the implementer
     */
    byte [] dpidToEthAddr(long dpid) {
        byte [] dpidData = longToBytes(dpid);
        byte [] ethAddrData = new byte[6];
        System.arraycopy(dpidData, 2, ethAddrData, 0, 6);
        return ethAddrData;
    }
    
    private IPv4 genIpPacket(Path p) {
        //https://github.com/wallnerryan/floodlight/blob/master/src/main/java/net/floodlightcontroller/counter/CounterStore.java
        IPv4 ip_pck = new IPv4();
        ip_pck.setProtocol((byte) 0xfd); //protocol 253, for experimental purpose
        ip_pck.setSourceAddress("path.hash"); //TODO: fix this
        ip_pck.setDestinationAddress("224.0.0.255");
        byte [] idData = intToBytes(p.hashCode());
        byte [] timeData = longToBytes(System.currentTimeMillis());
        byte [] data = new byte[idData.length + timeData.length];
        System.arraycopy(idData, 0, data, 0, idData.length);
        System.arraycopy(timeData, 0, data, idData.length, timeData.length);
        ip_pck.setPayload(new Data (data));
        
        return ip_pck;
    }
        /**
         * @param egressPort The switchport to output the PacketOut.
         */
    private void writePacketOut(
                                IPv4 ip_pck, 
                                short egressPort,
                                long srcDpid,
                                long dstDpid) {
        Ethernet eth_packet = new Ethernet();
        eth_packet.setEtherType(Ethernet.TYPE_IPv4);
        eth_packet.setSourceMACAddress(dpidToEthAddr(srcDpid)); //dpid => ethAddress
        eth_packet.setDestinationMACAddress(dpidToEthAddr(dstDpid));
        eth_packet.setPayload(ip_pck);
        
        OFPacketOut packetOutMessage = (OFPacketOut) floodlightProvider.getOFMessageFactory().getMessage(OFType.PACKET_OUT);
        short packetOutLength = (short) OFPacketOut.MINIMUM_LENGTH; // starting length

        // Set buffer_id, in_port, actions_len
        //packetOutMessage.setBufferId(packetInMessage.getBufferId());
        //packetOutMessage.setInPort(packetInMessage.getInPort());
        packetOutMessage.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);
        packetOutLength += OFActionOutput.MINIMUM_LENGTH;
        
        // set actions
        List<OFAction> actions = new ArrayList<OFAction>(1);      

        actions.add(new OFActionOutput(egressPort, (short) 0xFFFF));
        //actions.add(new OFActionOutput(p.first_port, (short) 0));
        //actions.add(new OFActionOutput(OFPort.OFPP_CONTROLLER.getValue(), (short) 0xFFFF));
        packetOutMessage.setActions(actions);

        // set data - only if buffer_id == -1
        //if (packetInMessage.getBufferId() == OFPacketOut.BUFFER_ID_NONE) {
        //    byte[] packetData = packetInMessage.getPacketData();
        //    packetOutMessage.setPacketData(packetData); 
        //    packetOutLength += (short) packetData.length;
        //}
        packetOutMessage.setPacketData(eth_packet.serialize());
        
        // finally, set the total length
        packetOutMessage.setLength(packetOutLength);              
            
        // and write it out
        IOFSwitch sw = switches.get(srcDpid);
        try {
            sw.write(packetOutMessage, null);
        } catch (IOException e) {
            log.error("Failed to write {} to switch {}: {}", new Object[] { packetOutMessage, sw, e });
        }
    }
    protected void measureDelay() {
        for (Path p : monitored_paths) {
            IPv4 ipPacket = genIpPacket(p);
            writePacketOut(ipPacket, p.first_port, p.src, p.dst);
            writePacketOut(ipPacket, OFPort.OFPP_CONTROLLER.getValue(), p.src, p.src);
        }
    }
    public void runMonitoring() {
        adaptiveTimer();
        lastSwitch();
        measureDelay();
    }

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
    public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
        switch (msg.getType()) {
            case PACKET_IN:
                return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);
            //case FLOW_REMOVED:
            //    return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);
            case ERROR:
                log.info("received an error {} from switch {}", (OFError) msg, sw);
                return Command.CONTINUE;
            default:
                break;
        }
        log.error("received an unexpected message {} from switch {}", msg, sw);
        return Command.CONTINUE;
    }

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
        log.info("niubi");
        return Command.CONTINUE;
    }

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		// TODO Auto-generated method stub
        floodlightProvider =
                context.getServiceImpl(IFloodlightProviderService.class);

	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		// TODO Auto-generated method stub
        log.info("Monitoring StartUp");
        //floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        //floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
        //floodlightProvider.addOFMessageListener(OFType.ERROR, this);

	}

    /**
     * @param floodlightProvider the floodlightProvider to set
     */
    public void setFloodlightProvider(IFloodlightProviderService floodlightProvider) {
        this.floodlightProvider = floodlightProvider;
    }
}
