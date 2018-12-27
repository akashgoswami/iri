package com.iota.iri.network;
import com.iota.iri.conf.NodeConfig;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TCPProtocol implements Runnable, Protocol {

    private Selector channelSelector = null;
    private ServerSocketChannel serverChannel = null;
    private int port = 4900;
    private int bufferLen = 4096;
    Map<String,Peer> myMap = new ConcurrentHashMap<String,Peer>();

    ByteBuffer buf = ByteBuffer.allocate(bufferLen);

    public TCPProtocol() {
    }

    public void init(NodeConfig config) throws IOException, UnknownHostException {

        channelSelector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        InetAddress ia = InetAddress.getLocalHost();
        InetSocketAddress isa = new InetSocketAddress(ia, port);
        serverChannel.socket().bind(isa);

    }

    public void shutdown(){

    }

    @Override
    public void run() {

        SelectionKey acceptKey = null;
        try {
            acceptKey = serverChannel.register(channelSelector, SelectionKey.OP_ACCEPT);
            while (acceptKey.selector().select() > 0) {

                Set readyKeys = channelSelector.selectedKeys();
                Iterator it = readyKeys.iterator();

                while (it.hasNext()) {
                    SelectionKey key = (SelectionKey) it.next();
                    it.remove();

                    if (key.isAcceptable()) {
                        System.out.println("Key is Acceptable");
                        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
                        SocketChannel sock = (SocketChannel) ssc.accept();
                        sock.configureBlocking(false);
                        SelectionKey another = sock.register(channelSelector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    }
                    if (key.isReadable()) {
                        System.out.println("Key is readable");
                        int ret = readMessage(key, buf);
                    }
                    if (key.isWritable()) {
                        System.out.println("THe key is writable");
                        int ret = writeMessage(key, buf);
                    }
                }

                /*Try to establish connection to other neigbours*/
            }
        } catch (ClosedChannelException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * This function registers the listener on TCP incoming socket
     *
     * @param peer The peer instance where packets should be delivered

     */
    public void registerListener(Peer peer) {
        String url = peer.getAddress() + ":" + peer.getPort();
        myMap.put(url,peer);

    }

    /**
     * The function is responsible for picking up packets from sendQueue of the Peer
     * and flushing them down to the peer.
     */

    public int writeMessage(SelectionKey key, ByteBuffer buffer) {
        System.out.println("Ready to write");
        int nBytes = -1;
        String remote;
        SocketChannel sock = (SocketChannel) key.channel();

        try {
            if (sock.getRemoteAddress() instanceof InetSocketAddress) {
                InetSocketAddress address = (InetSocketAddress) sock.getRemoteAddress();
                remote = address.getAddress().getHostAddress() + ":" + address.getPort();
            } else {
                // Reach here, what's it?
                remote = sock.getRemoteAddress().toString();
            }
            Peer peer = myMap.get(remote);
            if (peer.getSendQueueCount() > 0) {
                ByteBuffer buf = peer.dequeueSendPacket();
                sock.write(buf);
            }
        } catch (Exception e) {
            remote = "";
        }
        return nBytes;
    }

    /**
     * The function is responsible for reading messages from the peer and enqueuing them
     * to the Peer queue.
     */

    public int readMessage(SelectionKey key, ByteBuffer buf) {
        int nBytes = 0;
        String remote ="";
        SocketChannel sock = (SocketChannel) key.channel();
        try {
            nBytes = sock.read(buf);
            try {
                if (sock.getRemoteAddress() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) sock.getRemoteAddress();
                    remote = address.getAddress().getHostAddress() + ":" + address.getPort();
                } else {
                    // Reach here, what's it?
                    remote = sock.getRemoteAddress().toString();
                }
            } catch (Exception e) {
                remote = "";
            }

            buf.flip();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Peer peer = myMap.get(remote);
        peer.enqueueRecvPacket(buf);

        return nBytes;
    }

}
