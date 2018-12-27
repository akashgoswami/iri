package com.iota.iri.network;

import com.iota.iri.controllers.TransactionViewModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Peer {
    private static final Logger log = LoggerFactory.getLogger(Peer.class);
    private int port;
    private Protocol protocol;
    private String transport;
    private InetAddress address;


    private AtomicLong numberOfAllTransactions = new AtomicLong();
    private AtomicLong numberOfNewTransactions = new AtomicLong();
    private AtomicLong numberOfInvalidTransactions = new AtomicLong();
    private AtomicLong randomTransactionRequests = new AtomicLong();
    private AtomicLong numberOfSentTransactions = new AtomicLong();
    private AtomicLong numberOfStaleTransactions = new AtomicLong();
    
    private ArrayBlockingQueue<ByteBuffer> sendQueue = new ArrayBlockingQueue<>(10);
    private ArrayBlockingQueue<ByteBuffer> receiveQueue = new ArrayBlockingQueue<>(10);

    private boolean stopped = false;

    public Peer(String protocol, InetSocketAddress address, boolean isConfigured) {
        this.address = address.getAddress();
        this.port = address.getPort();
        transport = protocol;

        if (protocol.equals("tcp")){
            this.protocol = new TCPProtocol();
        }
        else if (protocol.equals("udp")){
            this.protocol = new UDPProtocol();
        }
    }

    public int getPort() {
        return port;
    }

    public String getAddress() {
        return address.getHostAddress();
    }


    public int getSendQueueCount(){
        return sendQueue.size();
    }

    public int getReceiveQueueCount(){
        return receiveQueue.size();
    }

    public boolean enqueueSendPacket( ByteBuffer packet) {
        sendQueue.add(packet);
        return true;
    }

    public ByteBuffer dequeueSendPacket() {
        ByteBuffer packet = sendQueue.poll();
        return packet;
    }

    public boolean enqueueRecvPacket( ByteBuffer packet) {
        receiveQueue.add(packet);
        return true;
    }

    public ByteBuffer dequeueRecvPacket() {
        ByteBuffer packet = receiveQueue.poll();
        return packet;
    }

    public String connectionType() {
        return transport;
    }


    public boolean matches(SocketAddress item) {
        if (item.toString().contains(address.toString())) {
            if (item.toString().contains(Integer.toString(port))) {
                return true;
            }
        }
        return false;
    }

    public void clear(){

    }

    void incAllTransactions() {
        numberOfAllTransactions.incrementAndGet();
    }

    void incNewTransactions() {
        numberOfNewTransactions.incrementAndGet();
    }

    void incRandomTransactionRequests() {
        randomTransactionRequests.incrementAndGet();
    }

    public void incInvalidTransactions() {
        numberOfInvalidTransactions.incrementAndGet();
    }

    public void incSentTransactions() {
        numberOfSentTransactions.incrementAndGet();
    }

    public long getNumberOfAllTransactions() {
        return numberOfAllTransactions.get();
    }

    public long getNumberOfInvalidTransactions() {
        return numberOfInvalidTransactions.get();
    }

    public long getNumberOfNewTransactions() {
        return numberOfNewTransactions.get();
    }

    public long getNumberOfRandomTransactionRequests() {
        return randomTransactionRequests.get();
    }

    public long getNumberOfSentTransactions() {
        return numberOfSentTransactions.get();
    }

    public long getNumberOfStaleTransactions() {
        return numberOfSentTransactions.get();
    }
    
}
