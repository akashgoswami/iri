package com.iota.iri.network;
import com.iota.iri.conf.NodeConfig;

public interface Protocol {

    void init(NodeConfig config) throws Exception;
    void registerListener(Peer p);
    void shutdown();
    void run();

}
