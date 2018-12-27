package com.iota.iri.service.dto;

import java.util.List;

import com.iota.iri.service.API;

/**
 * 
 * Contains information about the result of a successful {@code getPeers} API call.
 * See {@link API#getPeersStatement} for how this response is created.
 *
 */
public class GetNeighborsResponse extends AbstractResponse {

    /**
     * The Peers you are connected with, as well as their activity counters.
     * This includes the following statistics:
     * <ol>
     *     <li>address</li>
     *     <li>connectionType</li>
     *     <li>numberOfAllTransactions</li>
     *     <li>numberOfRandomTransactionRequests</li>
     *     <li>numberOfNewTransactions</li>
     *     <li>numberOfInvalidTransactions</li>
     *     <li>numberOfSentTransactions</li>
     *     <li>numberOfStaleTransactions</li>
     * </ol>
     * @see {@link com.iota.iri.service.dto.GetPeersResponse.Peer}
     */
    private Peer[] Peers;

    /**
     * 
     * @return {@link #Peers}
     */
    public Peer[] getPeers() {
        return Peers;
    }
    
    /**
     * Creates a new {@link GetPeersResponse}
     * 
     * @param elements {@link com.iota.iri.network.Peer}
     * @return an {@link GetPeersResponse} filled all Peers and their activity.
     */
    public static AbstractResponse create(final List<com.iota.iri.network.Peer> elements) {
        GetNeighborsResponse res = new GetNeighborsResponse();
        res.Peers = new Peer[elements.size()];
        int i = 0;
        for (com.iota.iri.network.Peer n : elements) {
            res.Peers[i++] = Peer.createFrom(n);
        }
        return res;
    }
    
    /**
     * 
     * A plain DTO of an iota Peer.
     * 
     */
    static class Peer {

        private String address;
        public long numberOfAllTransactions,
                numberOfRandomTransactionRequests,
                numberOfNewTransactions,
                numberOfInvalidTransactions,
                numberOfStaleTransactions,
                numberOfSentTransactions;
        public String connectionType;

        /**
         * The address of your Peer
         * 
         * @return the address
         */
        public String getAddress() {
            return address;
        }

        /**
         * Number of all transactions sent (invalid, valid, already-seen)
         * 
         * @return the number
         */
        public long getNumberOfAllTransactions() {
            return numberOfAllTransactions;
        }

        /**
         * New transactions which were transmitted.
         * 
         * @return the number
         */
        public long getNumberOfNewTransactions() {
            return numberOfNewTransactions;
        }

        /**
         * Invalid transactions your Peer has sent you. 
         * These are transactions with invalid signatures or overall schema.
         * 
         * @return the number
         */
        public long getNumberOfInvalidTransactions() {
            return numberOfInvalidTransactions;
        }

        /**
         * Stale transactions your Peer has sent you.
         * These are transactions with a timestamp older than your latest snapshot.
         *
         * @return the number
         */
        public long getNumberOfStaleTransactions() {
            return numberOfStaleTransactions;
        }

        /**
         * Amount of transactions send through your Peer
         * 
         * @return the number
         */
        public long getNumberOfSentTransactions() {
            return numberOfSentTransactions;
        }

        /**
         * The method type your Peer is using to connect (TCP / UDP)
         * 
         * @return the connection type
         */
        public String getConnectionType() {
            return connectionType;
        }

        /**
         * Creates a new Peer DTO from a Peer network instance
         * @param n the Peer currently connected to this node
         * @return a new instance of {@link GetPeersResponse.Peer}
         */
        public static Peer createFrom(com.iota.iri.network.Peer n) {
            Peer ne = new Peer();
            int port = n.getPort();
            ne.address = n.getAddress()+ ":" + port;
            ne.numberOfAllTransactions = n.getNumberOfAllTransactions();
            ne.numberOfInvalidTransactions = n.getNumberOfInvalidTransactions();
            ne.numberOfStaleTransactions = n.getNumberOfStaleTransactions();
            ne.numberOfNewTransactions = n.getNumberOfNewTransactions();
            ne.numberOfRandomTransactionRequests = n.getNumberOfRandomTransactionRequests();
            ne.numberOfSentTransactions = n.getNumberOfSentTransactions();
            ne.connectionType = n.connectionType();
            return ne;
        }
    }
}
