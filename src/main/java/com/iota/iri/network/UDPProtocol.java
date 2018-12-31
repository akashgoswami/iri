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

public class UDPProtocol implements Runnable, Protocol {

    private DatagramChannel  channel = null;
    private int bufferLen = 4096;

    private static final Logger log = LoggerFactory.getLogger(Node.class);
    Map<String,Peer> myMap = new ConcurrentHashMap<String,Peer>();


    public UDPProtocol() {

    }

    public void init(NodeConfig config) throws IOException, UnknownHostException {
        try {
            channel = DatagramChannel.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
        channel.socket().bind(new InetSocketAddress(config.getUdpReceiverPort()));
    }

    public void shutdown(){


    }

    /**
     * This function registers the listener on TCP incoming socket
     * @param peer The queue instance where packets should be delivered

     */
    public void registerListener(Peer peer) {
        String url = peer.getAddress() + ":" + peer.getPort();
        myMap.put(url,peer);


    }



    @Override
    public void run() {
        log.info("Run called");
        ByteBuffer buf = ByteBuffer.allocate(bufferLen);
        String remoteString;
        try {

            while (true) {
                buf.clear();
                buf.rewind();

                InetSocketAddress remoteAddr =  (InetSocketAddress)channel.receive(buf);
                remoteString = remoteAddr.getHostString() +":"+ remoteAddr.getPort();
                Peer peer = myMap.get(remoteString);
                if (peer != null) {
                    peer.enqueueRecvPacket(cloneByteBuffer(buf));
                }
                buf.flip();
            }

        } catch (IOException e1) {
                e1.printStackTrace();
        }

    }

    public ByteBuffer cloneByteBuffer(final ByteBuffer original) {
        final ByteBuffer clone = (original.isDirect()) ?
                ByteBuffer.allocateDirect(original.capacity()) :
                ByteBuffer.allocate(original.capacity());

        final ByteBuffer readOnlyCopy = original.asReadOnlyBuffer();

        // Flip and read from the original.
        readOnlyCopy.flip();
        clone.put(readOnlyCopy);
        clone.flip();

        return clone;
    }

}
