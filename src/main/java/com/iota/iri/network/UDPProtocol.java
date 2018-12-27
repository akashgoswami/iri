package com.iota.iri.network;
import com.iota.iri.conf.NodeConfig;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.net.*;
import java.util.*;
import java.net.InetSocketAddress;

public class UDPProtocol implements Runnable, Protocol {

    private DatagramChannel  channel = null;
    private int port = 4900;
    private int bufferLen = 4096;
    ByteBuffer buf = ByteBuffer.allocate(bufferLen);

    public UDPProtocol() {

    }

    public void init(NodeConfig config) throws IOException, UnknownHostException {
        channel.socket().bind(new InetSocketAddress(port));
    }

    public void shutdown(){


    }

    /**
     * This function registers the listener on TCP incoming socket
     * @param peer The queue instance where packets should be delivered

     */
    public void registerListener(Peer p) {


    }



    @Override
    public void run() {

        try {

            while (true) {
                channel.receive(buf);
                buf.flip();
                SocketAddress remote = channel.getRemoteAddress();

            }

        } catch (IOException e1) {
                e1.printStackTrace();
        }

    }

}
