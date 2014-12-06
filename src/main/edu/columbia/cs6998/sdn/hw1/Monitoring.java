/**
*    Copyright 2014, Columbia University.
*    Homework 1, COMS E6998-10 Fall 2014
*    Software Defined Networking
*    Originally created by Shangjin Zhang, Columbia University
* 
*    Licensed under the Apache License, Version 2.0 (the "License"); you may
*    not use this file except in compliance with the License. You may obtain
*    a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
*    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
*    License for the specific language governing permissions and limitations
*    under the License.
**/

/**
 * Floodlight
 * A BSD licensed, Java based OpenFlow controller
 *
 * Floodlight is a Java based OpenFlow controller originally written by David Erickson at Stanford
 * University. It is available under the BSD license.
 *
 * For documentation, forums, issue tracking and more visit:
 *
 * http://www.openflowhub.org/display/Floodlight/Floodlight+Home
 **/

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

public class Monitoring 
    implements IFloodlightModule, IOFMessageListener {
	ThreadPool tp;
	
    protected static Logger log = LoggerFactory.getLogger(Monitoring.class);
    
    // Module dependencies
    protected IFloodlightProviderService floodlightProvider;
    
/* CS6998: data structures for the learning switch feature
    // Stores the learned state for each switch
*/
    protected Map<IOFSwitch, Map<Long, Short>> macToSwitchPortMap;
    protected Map<Long, Set<Long>> destinationMap;

    // flow-mod - for use in the cookie
    public static final int HW1_SWITCH_APP_ID = 10;
    // LOOK! This should probably go in some class that encapsulates
    // the app cookie management
    public static final int APP_ID_BITS = 12;
    public static final int APP_ID_SHIFT = (64 - APP_ID_BITS);
    public static final long HW1_SWITCH_COOKIE = (long) (HW1_SWITCH_APP_ID & ((1 << APP_ID_BITS) - 1)) << APP_ID_SHIFT;
    
    // more flow-mod defaults 
    protected static final short IDLE_TIMEOUT_DEFAULT = 5; //10;
    protected static final short HARD_TIMEOUT_DEFAULT = 5;
    protected static final short PRIORITY_DEFAULT = 100;
    
    // for managing our map sizes
    protected static final int MAX_MACS_PER_SWITCH  = 1000;
    protected static final int MAX_DESTINATION_NUMBER = 1000;  


    // FINAL PROJECT

    protected static final long NUMBER_OF_TEST_PACKET = 1;
    protected static final long PACKET_LOSS_CHECK_DELAY = 1000;
    
    

    protected Map<Long, Map<Long, Map<Long, Map<Long, Boolean>>>> packetLossMap;
    protected Map<Long, Map<Long, Long>> delayMap;
    
    class Path {
    	private Long firstSwitch;
    	private Long lastSwitch;
    	private Map<Long, Set<Long>> linkMap; // switch srcDpid=>destDpid
    	public double firstSwThroughput;
    	public double lastSwThroughput;
    	public Path(Long firstSwitch) {
    		this.linkMap = new ConcurrentHashMap<Long, Set<Long>>();
    		this.firstSwitch = firstSwitch;
    		this.firstSwThroughput = -1.0;
    		this.lastSwThroughput = -1.0;
    	}
    	public Path() {
    		this.linkMap = new ConcurrentHashMap<Long, Set<Long>>();
    		this.firstSwitch = -1L;
    		this.firstSwThroughput = -1.0;
    		this.lastSwThroughput = -1.0;
    	}
    	// destSet = destinationMap.get(sourceMac);
    	public synchronized Boolean checkIfThroughputReady() {
    		// check if the throuput is ready on both first sw and last sw
    		if (this.lastSwThroughput > 0 && this.firstSwThroughput > 0) {
                log.info("=================================================================");
            	log.info("firstSwitch=" + this.getFirstSwitch() + " lastSwitch=" + this.getLastSwitch() +
                		"\nthroughput on firstSw=" + this.firstSwThroughput + ", on lastSw=" + this.lastSwThroughput + " byte/s" + ", packetLoss=" + (1 - this.lastSwThroughput / this.firstSwThroughput));
                log.info("=================================================================");
            	return true;
            }
    		return false;
    	}
        
    	public synchronized void setFirstSwitch(Long dpid) {
    		this.firstSwitch = dpid;
    	}
    	public synchronized Long getFirstSwitch() {
    		return firstSwitch;
    	}
    	public synchronized void setLastSwitch(Long dpid) {
    		this.lastSwitch = dpid;
    	}
    	public synchronized Long getLastSwitch() {
    		return lastSwitch;
    	}
    	public synchronized void addLink(Long srcDpid, Long destDpid) {
    		//add a switch link
    		Set<Long> destSet = linkMap.get(srcDpid);
            if (destSet == null) {
                // May be accessed by REST API so we need to make it thread safe
                destSet = Collections.synchronizedSet(new HashSet<Long>());
                linkMap.put(srcDpid, destSet);
            }
            // add the destMac
            destSet.add(destDpid);
    	}
    	public synchronized Boolean isASrcSwitch(Long dpid) {
    		//log.info("[isASrcSwitch]");
    		//for (Long srcDpid : linkMap.keySet()) {
    		//	log.info("srcSwitch=" + srcDpid);
    		//}
    		return linkMap.containsKey(dpid);
    	}
    }
    synchronized protected void tryAddPath(Long sourceMac, Long destMac) {
    	// check if a srcMac->DestMac entry doesn't exist, create one
    	if (pathMap.get(sourceMac) == null) {
        	pathMap.put(sourceMac, new ConcurrentHashMap<Long, Path>());
        }
        if (pathMap.get(sourceMac).get(destMac) == null) {
        	pathMap.get(sourceMac).put(destMac, new Path());
        	log.info("[AddPath]srcMac:" + sourceMac + " destMac:" + destMac);
        }
    }
    protected Map<Long, Map<Long, Path>> pathMap; // [srcMac, destMac] => [first switch dpid, Map(prev->next)]
    /**
     * @param floodlightProvider the floodlightProvider to set
     */
    public void setFloodlightProvider(IFloodlightProviderService floodlightProvider) {
        this.floodlightProvider = floodlightProvider;
    }
    
    @Override
    public String getName() {
        return "hw1switch";
    }

    /**
     * Adds a host to the MAC->SwitchPort mapping
     * @param sw The switch to add the mapping to
     * @param mac The MAC address of the host to add
     * @param portVal The switchport that the host is on
     */
    protected void addToPortMap(IOFSwitch sw, long mac, short portVal) {
        Map<Long, Short> swMap = macToSwitchPortMap.get(sw);
        
        if (swMap == null) {
            // May be accessed by REST API so we need to make it thread safe
            swMap = Collections.synchronizedMap(new LRULinkedHashMap<Long, Short>(MAX_MACS_PER_SWITCH));
            macToSwitchPortMap.put(sw, swMap);
        }
        if (swMap.get(mac) == null) {
            log.info("[addToPortMap] sw:" + sw.getId() + ", mac:" + mac + "->port:" + portVal);
        }
        swMap.put(mac, portVal);
    }
    
    /**
     * Removes a host from the MAC->SwitchPort mapping
     * @param sw The switch to remove the mapping from
     * @param mac The MAC address of the host to remove
     */
    protected void removeFromPortMap(IOFSwitch sw, long mac) {
        Map<Long, Short> swMap = macToSwitchPortMap.get(sw);
        if (swMap != null)
            swMap.remove(mac);
    }

    /**
     * Get the port that a MAC is associated with
     * @param sw The switch to get the mapping from
     * @param mac The MAC address to get
     * @return The port the host is on
     */
    public Short getFromPortMap(IOFSwitch sw, long mac) {
        Map<Long, Short> swMap = macToSwitchPortMap.get(sw);
        if (swMap != null) {
            return swMap.get(mac);
        }
        return null;
    }

    public void addToDestinationMap(long sourceMac, long destMac) {
        // ignore self to self connection
        if (destMac == sourceMac) return;
        Set<Long> destSet = destinationMap.get(sourceMac);
        if (destSet == null) {
            // May be accessed by REST API so we need to make it thread safe
            destSet = Collections.synchronizedSet(new HashSet<Long>(MAX_DESTINATION_NUMBER+1));
            destinationMap.put(sourceMac, destSet);
        }
        // add the destMac
        destSet.add(destMac);
    }
    /**
     * Writes a OFFlowMod to a switch.
     * @param sw The switch tow rite the flowmod to.
     * @param command The FlowMod actions (add, delete, etc).
     * @param bufferId The buffer ID if the switch has buffered the packet.
     * @param match The OFMatch structure to write.
     * @param outPort The switch port to output it to.
     */
    private void writeFlowMod(IOFSwitch sw, short command, int bufferId,
            OFMatch match, short outPort) {
        // from openflow 1.0 spec - need to set these on a struct ofp_flow_mod:
        // struct ofp_flow_mod {
        //    struct ofp_header header;
        //    struct ofp_match match; /* Fields to match */
        //    uint64_t cookie; /* Opaque controller-issued identifier. */
        //
        //    /* Flow actions. */
        //    uint16_t command; /* One of OFPFC_*. */
        //    uint16_t idle_timeout; /* Idle time before discarding (seconds). */
        //    uint16_t hard_timeout; /* Max time before discarding (seconds). */
        //    uint16_t priority; /* Priority level of flow entry. */
        //    uint32_t buffer_id; /* Buffered packet to apply to (or -1).
        //                           Not meaningful for OFPFC_DELETE*. */
        //    uint16_t out_port; /* For OFPFC_DELETE* commands, require
        //                          matching entries to include this as an
        //                          output port. A value of OFPP_NONE
        //                          indicates no restriction. */
        //    uint16_t flags; /* One of OFPFF_*. */
        //    struct ofp_action_header actions[0]; /* The action length is inferred
        //                                            from the length field in the
        //                                            header. */
        //    };
        OFFlowMod flowMod = (OFFlowMod) floodlightProvider.getOFMessageFactory().getMessage(OFType.FLOW_MOD);
        flowMod.setMatch(match);
        flowMod.setCookie(Monitoring.HW1_SWITCH_COOKIE);
        flowMod.setCommand(command);
        flowMod.setIdleTimeout(Monitoring.IDLE_TIMEOUT_DEFAULT);
        flowMod.setHardTimeout(Monitoring.HARD_TIMEOUT_DEFAULT);
        flowMod.setPriority(Monitoring.PRIORITY_DEFAULT);
        flowMod.setBufferId(bufferId);
        flowMod.setOutPort((command == OFFlowMod.OFPFC_DELETE) ? outPort : OFPort.OFPP_NONE.getValue());
        flowMod.setFlags((command == OFFlowMod.OFPFC_DELETE) ? 0 : (short) (1 << 0)); // OFPFF_SEND_FLOW_REM

        // set the ofp_action_header/out actions:
        // from the openflow 1.0 spec: need to set these on a struct ofp_action_output:
        // uint16_t type; /* OFPAT_OUTPUT. */
        // uint16_t len; /* Length is 8. */
        // uint16_t port; /* Output port. */
        // uint16_t max_len; /* Max length to send to controller. */
        // type/len are set because it is OFActionOutput,
        // and port, max_len are arguments to this constructor
        flowMod.setActions(Arrays.asList((OFAction) new OFActionOutput(outPort, (short) 0xffff)));
        flowMod.setLength((short) (OFFlowMod.MINIMUM_LENGTH + OFActionOutput.MINIMUM_LENGTH));

        if (log.isTraceEnabled()) {
            log.trace("{} {} flow mod {}", 
                      new Object[] { sw, (command == OFFlowMod.OFPFC_DELETE) ? "deleting" : "adding", flowMod });
        }

        // and write it out
        try {
            sw.write(flowMod, null);
        } catch (IOException e) {
            log.error("Failed to write {} to switch {}", new Object[] { flowMod, sw }, e);
        }
    }

    /**
     * Writes an OFPacketOut message to a switch.
     * @param sw The switch to write the PacketOut to.
     * @param packetInMessage The corresponding PacketIn.
     * @param egressPort The switchport to output the PacketOut.
     */
    private void writePacketOutForPacketIn(IOFSwitch sw, 
                                          OFPacketIn packetInMessage, 
                                          short egressPort) {

        // from openflow 1.0 spec - need to set these on a struct ofp_packet_out:
        // uint32_t buffer_id; /* ID assigned by datapath (-1 if none). */
        // uint16_t in_port; /* Packet's input port (OFPP_NONE if none). */
        // uint16_t actions_len; /* Size of action array in bytes. */
        // struct ofp_action_header actions[0]; /* Actions. */
        /* uint8_t data[0]; */ /* Packet data. The length is inferred
                                  from the length field in the header.
                                  (Only meaningful if buffer_id == -1.) */
        
        OFPacketOut packetOutMessage = (OFPacketOut) floodlightProvider.getOFMessageFactory().getMessage(OFType.PACKET_OUT);
        short packetOutLength = (short) OFPacketOut.MINIMUM_LENGTH; // starting length

        // Set buffer_id, in_port, actions_len
        packetOutMessage.setBufferId(packetInMessage.getBufferId());
        packetOutMessage.setInPort(packetInMessage.getInPort());
        packetOutMessage.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);
        packetOutLength += OFActionOutput.MINIMUM_LENGTH;
        
        // set actions
        List<OFAction> actions = new ArrayList<OFAction>(1);      
        actions.add(new OFActionOutput(egressPort, (short) 0));
        packetOutMessage.setActions(actions);

        // set data - only if buffer_id == -1
        if (packetInMessage.getBufferId() == OFPacketOut.BUFFER_ID_NONE) {
            byte[] packetData = packetInMessage.getPacketData();
            packetOutMessage.setPacketData(packetData); 
            packetOutLength += (short) packetData.length;
        }
        
        // finally, set the total length
        packetOutMessage.setLength(packetOutLength);              
            
        // and write it out
        try {
            sw.write(packetOutMessage, null);
        } catch (IOException e) {
            log.error("Failed to write {} to switch {}: {}", new Object[] { packetOutMessage, sw, e });
        }
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

        // Read in packet data headers by using OFMatch
        OFMatch match = new OFMatch();
        match.loadFromPacket(pi.getPacketData(), pi.getInPort());
        Long sourceMac = Ethernet.toLong(match.getDataLayerSource());
        Long destMac = Ethernet.toLong(match.getDataLayerDestination());

        //if (sourceMac > 100 || destMac > 100) {
        if ((sourceMac > 100 && destMac > 100) ||
            (match.getNetworkProtocol() != (byte)0xfd && match.getNetworkProtocol() != 1)) {
            // CS6998: Assume we only deal with mac <= 100. For >100 part, flood it
            this.writePacketOutForPacketIn(sw, pi, OFPort.OFPP_FLOOD.getValue());
            return Command.CONTINUE;
        }
        System.out.println("[PacketIn]sw:" + sw.getId() + " sourceMac:" + sourceMac + ", destMac:" + destMac + ", protocol:" + match.getNetworkProtocol());
        addToPortMap(sw, sourceMac, match.getInputPort());
        addToDestinationMap(sourceMac, destMac);

        // Now output flow-mod and/or packet
        Short outPort = getFromPortMap(sw, destMac);
        
        // FINAL PROJECT
        //log.info("protocol:" + match.getNetworkProtocol());
        if (match.getNetworkProtocol() != (byte)0xfd) {
            if (outPort == null) {
                // If we haven't learned the port for the dest MAC, flood it
                // CS6998: Fill out the following ????
                log.info("[PacketIn] Flood");
                this.writePacketOutForPacketIn(sw, pi, OFPort.OFPP_FLOOD.getValue());
            } else if (outPort == match.getInputPort()) {
                log.info("ignoring packet that arrived on same port as learned destination:"
                        + " switch {} dest MAC {} port {}",
                        new Object[]{ sw, HexString.toHexString(destMac), outPort });
            } else {
                // Add flow table entry matching source MAC, dest MAC and input port
                // that sends to the port we previously learned for the dest MAC.
                match.setWildcards(((Integer)sw.getAttribute(IOFSwitch.PROP_FASTWILDCARDS)).intValue()
                                    & ~OFMatch.OFPFW_IN_PORT
                                    & ~OFMatch.OFPFW_DL_SRC & ~OFMatch.OFPFW_DL_DST
                                    & ~OFMatch.OFPFW_NW_SRC_MASK & ~OFMatch.OFPFW_NW_DST_MASK);
                this.writeFlowMod(sw, OFFlowMod.OFPFC_ADD, pi.getBufferId(), match, outPort);

                // Final PROJECT
                long id = System.nanoTime(); //currentTimeMillis();

                //class MyThread implements Runnable {
                //	IOFSwitch sw;
                //	OFMatch match;
                //	long id;
                //	short outPort;
                //	OFPacketIn pi;
                //	public MyThread(IOFSwitch sw, OFMatch match, long id, short outPort, OFPacketIn pi) {
                //	    // store parameter for later user
                //		this.sw = sw;
                //		this.match = match;
                //		this.id = id;
                //		this.outPort = outPort;
                //		this.pi = pi;
                //	}

                //	public void run() {
                //        for (long i=0; i<NUMBER_OF_TEST_PACKET; i++) {
                //        	//log.info("[RUNNABLE] i=" + i);
                //       	    IPv4 ipPacket = genIpPacket(sw, match, id, i);
                //       	    writePacketOut(sw, ipPacket, outPort, match.getDataLayerSource(), match.getDataLayerDestination(), pi);
                //        }
                //	}
                //}
                //// Test threadPool Runnable
                //testRun = new MyThread(sw, match, id, outPort, pi);
                //long timeout = 0;
                //TimeUnit unit = TimeUnit.SECONDS;
                //tp.getScheduledExecutor().schedule(testRun, timeout, unit);
                //try {
                //    tp.getScheduledExecutor().wait();
                //} catch (Exception e) {}
                for (long i=0; i<NUMBER_OF_TEST_PACKET; i++) {
                    IPv4 ipPacket = genIpPacket(sw, match, id, i);
                    writePacketOut(sw, ipPacket, outPort, match.getDataLayerSource(), match.getDataLayerDestination(), pi);
                }
                //tryAddPath(sourceMac, destMac);
                //Path p = pathMap.get(sourceMac).get(destMac);
                //p.setFirstSwitch(sw.getId());
                if (pathMap.get(sourceMac) == null) {
                    pathMap.put(sourceMac, new ConcurrentHashMap<Long, Path>());
                }
                if (pathMap.get(sourceMac).get(destMac) == null) {
                    pathMap.get(sourceMac).put(destMac, new Path(sw.getId()));
                    log.info("[PacketIn] add path from srcMac:" + sourceMac + "to destMac:" + destMac);
                }
            }
        } else {
            //log.info("===================OWN PACKET==================");
            if (outPort != null) {
                Ethernet packet = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
                IPv4 ipPacket = (IPv4) packet.getPayload();
                byte[] payload = ((Data) ipPacket.getPayload()).getData();
                byte[] dpidLoad = new byte[8];
                byte[] timeLoad = new byte[8];
                byte[] idLoad = new byte[8];
                byte[] countLoad = new byte[8];

                System.arraycopy(payload, 0, dpidLoad, 0, 8);
                System.arraycopy(payload, 8, timeLoad, 0, 8);
                System.arraycopy(payload, 16, idLoad, 0, 8);
                System.arraycopy(payload, 24, countLoad, 0, 8);

                long dpid = ByteBuffer.wrap(dpidLoad).getLong();
                long time = ByteBuffer.wrap(timeLoad).getLong();
                long id = ByteBuffer.wrap(idLoad).getLong();
                long count = ByteBuffer.wrap(countLoad).getLong();

                assert(pathMap.get(sourceMac) != null && pathMap.get(sourceMac).get(destMac) != null);
                Path p = pathMap.get(sourceMac).get(destMac);
                p.addLink(dpid, sw.getId());
                log.info("[PacketIn, own] add link \n" +
                		"srcMac:" + sourceMac + " destMac:" + destMac + "\n" +
                		"prevSw:" + dpid + " currSw:" + sw.getId());
                
                if (delayMap.get(sw.getId()) == null) delayMap.put(sw.getId(), new ConcurrentHashMap<Long, Long>());
                if (delayMap.get(sw.getId()).get(dpid) == null) delayMap.get(sw.getId()).put(dpid, 0L);
                
                long currentDelay = delayMap.get(sw.getId()).get(dpid);
                double alpha = 0.125;
                if (currentDelay == 0L) currentDelay = System.nanoTime() - time;
                else currentDelay = (long)((1-alpha) * currentDelay) + (long) (alpha * (System.nanoTime() - time));
                
                System.out.println("currentSwitchId=" + sw.getId() + 
                        ", dpid=" + dpid + 
                        ", delay=" + ((double)currentDelay/1000000.0) +
                        ", id=" + id +
                        ", count=" + count);
//                log.info(   "currentSwitchId=" + sw.getId() + 
//                            ", dpid=" + dpid + 
//                            ", delay=" + currentDelay +
//                            ", id=" + id +
//                            ", count=" + count);

                if (!packetLossMap.containsKey(sw.getId())) packetLossMap.put(sw.getId(), new ConcurrentHashMap<Long, Map<Long, Map<Long, Boolean>>>());
                if (!packetLossMap.get(sw.getId()).containsKey(dpid)) packetLossMap.get(sw.getId()).put(dpid, new ConcurrentHashMap<Long, Map<Long, Boolean>>());
                Map<Long, Map<Long, Boolean>> map = packetLossMap.get(sw.getId()).get(dpid);
                if (!map.containsKey(id)) map.put(id, new ConcurrentHashMap<Long, Boolean>());
                Map<Long, Boolean> set = map.get(id);
                class checkPackLossTimerTask extends TimerTask{
                    long sw;
                    long dpid;
                    long id;
                    checkPackLossTimerTask(long sw, long dpid, long id) {
                        this.sw = sw;
                        this.dpid = dpid;
                        this.id = id;
                    }
                    @Override
                    public void run() {
                        Map<Long, Boolean> set = packetLossMap.get(sw).get(dpid).get(id);
                        log.info("PACKET LOSS from " + sw + " to " + dpid + " = " + (1.0 - (double)set.keySet().size() / (double) NUMBER_OF_TEST_PACKET));
                        packetLossMap.get(sw).get(dpid).remove(id);
                    }
                }
                set.put(count, true);
                if (set.keySet().size() == 1) {
                    new Timer().schedule(new checkPackLossTimerTask(sw.getId(), dpid, id), PACKET_LOSS_CHECK_DELAY);
                }
            }
        }
        return Command.CONTINUE;
    }

    
    private IPv4 genIpPacket(IOFSwitch sw, OFMatch match, long id, long count) {
    //https://github.com/wallnerryan/floodlight/blob/master/src/main/java/net/floodlightcontroller/counter/CounterStore.jav
        IPv4 ip_pck = new IPv4();
        ip_pck.setProtocol((byte) 0xfd); //protocol 253, for experimental purpose
        ip_pck.setSourceAddress(match.getNetworkSource()); //TODO: fix this
        ip_pck.setDestinationAddress("224.0.0.255"); //match.getNetworkDestination()); //"224.0.0.255");

        // DATA
        byte [] dpidData = longToBytes(sw.getId());
        byte [] timeData = longToBytes(System.nanoTime());
        byte [] idData = longToBytes(id);
        byte [] countData = longToBytes(count);
        
        // DATA OUT
        byte [] data = new byte[32];

        System.arraycopy(dpidData, 0, data, 0, 8);
        System.arraycopy(timeData, 0, data, 8, 8);
        System.arraycopy(idData, 0, data, 16, 8);
        System.arraycopy(countData, 0, data, 24, 8);


        ip_pck.setPayload(new Data (data));
        return ip_pck;
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
        /**
         * @param egressPort The switchport to output the PacketOut.
         */
    private void writePacketOut(IOFSwitch sw,
                                IPv4 ip_pck, 
                                short egressPort,
                                byte[] srcMac,
                                byte[] dstMac,
                                OFPacketIn packetInMessage) {
        Ethernet eth_packet = new Ethernet();
        eth_packet.setEtherType(Ethernet.TYPE_IPv4);
        eth_packet.setSourceMACAddress(srcMac); //dpid => ethAddress
        eth_packet.setDestinationMACAddress(dstMac);
        eth_packet.setPayload(ip_pck);
        
        
        OFPacketOut packetOutMessage = (OFPacketOut) floodlightProvider.getOFMessageFactory().getMessage(OFType.PACKET_OUT);
        short packetOutLength = (short) OFPacketOut.MINIMUM_LENGTH; // starting length

        // Set buffer_id, in_port, actions_len
        //packetOutMessage.setBufferId(packetInMessage.getBufferId());
        packetOutMessage.setBufferId(OFPacketOut.BUFFER_ID_NONE);
        packetOutMessage.setInPort(packetInMessage.getInPort());
        packetOutMessage.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);
        packetOutLength += OFActionOutput.MINIMUM_LENGTH;
        
        // set actions
        List<OFAction> actions = new ArrayList<OFAction>(1);      

        actions.add(new OFActionOutput(egressPort, (short) 0));
        //actions.add(new OFActionOutput(egressPort, (short) 0xFFFF));
        //actions.add(new OFActionOutput(p.first_port, (short) 0));
        //actions.add(new OFActionOutput(OFPort.OFPP_CONTROLLER.getValue(), (short) 0xFFFF));
        packetOutMessage.setActions(actions);

        //set data - only if buffer_id == -1
        //if (packetInMessage.getBufferId() == OFPacketOut.BUFFER_ID_NONE) {
        //    byte[] packetData = packetInMessage.getPacketData();
        //    packetOutMessage.setPacketData(packetData); 
        //    packetOutLength += (short) packetData.length;
        ////packetOutMessage.setPacketData(eth_packet.serialize());
        ////packetOutLength += (short) eth_packet.serialize().length;
        //// finally, set the total length
        //packetOutMessage.setLength(packetOutLength);              

        //OFMatch matchtemp = new OFMatch();
        //matchtemp.loadFromPacket(packetOutMessage.getPacketData(), packetInMessage.getInPort());
        //log.info("[OFMATCH PROTOCOL]" + matchtemp.getNetworkProtocol());
        //} else {
        //    // finally, set the total length
        //    packetOutMessage.setLength(packetOutLength);              
        //}

        packetOutMessage.setPacketData(eth_packet.serialize());
        packetOutLength += (short) eth_packet.serialize().length;
        // finally, set the total length
        packetOutMessage.setLength(packetOutLength);              

        OFMatch matchtemp = new OFMatch();
        matchtemp.loadFromPacket(packetOutMessage.getPacketData(), packetInMessage.getInPort());
            
        // and write it out
        try {
            //log.info("[sending message]writting ");
            sw.write(packetOutMessage, null);
        } catch (IOException e) {
            log.error("Failed to write {} to switch {}: {}", new Object[] { packetOutMessage, sw, e });
        }
    }

    /**
     * Processes a flow removed message. 
     * @param sw The switch that sent the flow removed message.
     * @param flowRemovedMessage The flow removed message.
     * @return Whether to continue processing this message or stop.
     */
    private Command processFlowRemovedMessage(IOFSwitch sw, OFFlowRemoved flowRemovedMessage) {
        if (flowRemovedMessage.getCookie() != Monitoring.HW1_SWITCH_COOKIE) {
            return Command.CONTINUE;
        }

        Long sourceMac = Ethernet.toLong(flowRemovedMessage.getMatch().getDataLayerSource());
        Long destMac = Ethernet.toLong(flowRemovedMessage.getMatch().getDataLayerDestination());

        log.info("[FlowRemoved] on sw:" + sw.getId() + ", srcMac=" + sourceMac + " destMac=" + destMac);
        // check the throughput
        assert(pathMap.get(sourceMac) != null && pathMap.get(sourceMac).get(destMac) != null);

        if (pathMap.get(sourceMac) != null && pathMap.get(sourceMac).get(destMac) != null) {
        	Long byteCount = flowRemovedMessage.getByteCount();
            Integer durationInSec = flowRemovedMessage.getDurationSeconds();
            Double bandwidth = ((double)byteCount) / ((double)durationInSec);
            
            Path p = pathMap.get(sourceMac).get(destMac);
            Boolean throughputReady = false;
            if (!p.isASrcSwitch(sw.getId())) {
                // current sw is never a src sw for the path, thus it's the final switch
                // check for throughput
            	p.setLastSwitch(sw.getId());
                p.lastSwThroughput = bandwidth;
                throughputReady = p.checkIfThroughputReady();

            } else if (p.getFirstSwitch() == sw.getId()) {
            	p.firstSwThroughput = bandwidth;
                throughputReady = p.checkIfThroughputReady();
            }
            

            //// clear the path in the map
            // TODO: when? the order of flowremoved for different sw is undetermined
            if (throughputReady) {
                pathMap.get(sourceMac).remove(destMac);
                if (pathMap.get(sourceMac).isEmpty()) {
                        pathMap.remove(sourceMac);
                }
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("{} flow entry removed {}", sw, flowRemovedMessage);
        }
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
        macToSwitchPortMap = 
                new ConcurrentHashMap<IOFSwitch, Map<Long, Short>>();
        destinationMap =
                new ConcurrentHashMap<Long, Set<Long>>();
        packetLossMap =
                new ConcurrentHashMap<Long, Map<Long, Map<Long, Map<Long, Boolean>>>>();
        delayMap = 
        		new ConcurrentHashMap<Long, Map<Long, Long>>();
        pathMap =
        		new ConcurrentHashMap<Long, Map<Long, Path>>();
    }

    @Override
    public void startUp(FloodlightModuleContext context) {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
        floodlightProvider.addOFMessageListener(OFType.ERROR, this);
    }
}
