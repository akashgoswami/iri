package com.iota.iri.network;
import com.iota.iri.conf.NodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.net.*;
import java.util.*;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.validator.routines.InetAddressValidator;

import static java.util.concurrent.TimeUnit.SECONDS;


public class UDPProtocol implements Runnable, Protocol {

    private DatagramChannel  channel = null;
    private int bufferLen = 4096;

    private static final Logger log = LoggerFactory.getLogger(Node.class);
    Map<String,Peer> myMap = new ConcurrentHashMap<String,Peer>();

    /*Using more than one thread needs protection against concurrency issues*/
    private ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(1);

    public UDPProtocol() {

    }

    public void init(NodeConfig config) throws IOException, UnknownHostException {
        try {
            channel = DatagramChannel.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
        channel.socket().bind(new InetSocketAddress(config.getUdpReceiverPort()));
        final Runnable sendWorker = new SenderThread(this);
        scheduledExecutorService.scheduleWithFixedDelay(sendWorker, 10, 1, SECONDS );

    }

    public void shutdown(){
        scheduledExecutorService.shutdown();
        channel.socket().close();
    }

    /**
     * This function registers the listener on UDP address and port combination
     * @param peer The peer instance whose packets should be delivered

     */
    public void registerListener(Peer peer) {
        // Peer address must be IP address and not hostname
        InetAddressValidator validator = InetAddressValidator.getInstance();

        if (validator.isValid(peer.getAddress())) {
            String url = peer.getAddress() + ":" + peer.getPort();
            // Any old existing mapping will be automatically overwritten
            myMap.put(url,peer);
        }

    }

    /**
     * This function removes the listener on UDP address and port combination
     * @param peer The peer instance whose packets should no longer be listened
     */
    public void deRegisterListener(Peer peer) {
        String url = peer.getAddress() + ":" + peer.getPort();
        // Any old existing mapping will be automatically overwritten
        myMap.remove(url);
    }


    /**
     * This function loops through all registered peers and flushes down the send queue
     */
    private void flushSendQueue() {
        myMap.entrySet().stream().forEach((e) ->{
            Peer peer = e.getValue();
            if (peer.getSendQueueCount() > 0) {
                ByteBuffer buffer = peer.dequeueSendPacket();
                try {
                    channel.send(buffer, new InetSocketAddress(peer.getAddress(), peer.getPort()));
                } catch (IOException er) {
                    log.error("Failure to send UDP packet to peer", e.getKey(), er);
                }
            }
        });

    }


    @Override
    public void run() {

        String remoteString;
        try {

            while (true) {
                ByteBuffer buf = ByteBuffer.allocate(bufferLen);
                InetSocketAddress remoteAddr = (InetSocketAddress)channel.receive(buf);
                remoteString = remoteAddr.getHostString() +":"+ remoteAddr.getPort();
                Peer peer = myMap.get(remoteString);
                if (peer != null) {
                    buf.flip();
                    peer.enqueueRecvPacket(buf);
                }
            }

        } catch (IOException e) {
                log.error("Failure in receiving UDP packet", e);
        }

    }


    private class SenderThread implements  Runnable  {

        private final UDPProtocol master;

        public SenderThread(final UDPProtocol m){
            master = m;
        }

        public void run(){

            master.flushSendQueue();
        }
    }

}
