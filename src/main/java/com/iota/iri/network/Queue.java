package com.iota.iri.network;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Queue {

    ConcurrentLinkedQueue<Byte[]> queue = new ConcurrentLinkedQueue<Byte[]>();

    public boolean enqueue(Byte[] packet){
        return queue.add(packet);
    }

    public Byte[] dequeue(){
        return queue.remove();
    }
}