package com.iota.iri.network;

import com.iota.iri.TransactionValidator;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.crypto.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.model.TransactionHash;
import com.iota.iri.storage.Tangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.iota.iri.model.Hash;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.security.SecureRandom;
import java.util.*;
import com.iota.iri.service.snapshot.SnapshotProvider;

public class PeerWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PeerWorker.class);
    private final Node node;
    private Tangle tangle;
    private FIFOCache<ByteBuffer, Hash> recentSeenBytes;
    private final TransactionValidator transactionValidator;
    private final SnapshotProvider snapshotProvider;


    public PeerWorker(Node node, SnapshotProvider snapshotProvider, Tangle tangle) {
        this.node = node;
        this.snapshotProvider = snapshotProvider;
        this.transactionValidator = node.getTransactionValidator();
        this.tangle = tangle;
     }

    @Override
    public void run() {

        /*The worker needs to perform following
        * 1. Choose a peer from the Node. This choice is entirely dependent upon what the Node decides.
        *
        * 2. Check if there is any packet in the chosen peer's Ingress queue. If yes, then transform the packet into a transaction
        * and validate and store that into the tangle. Put the transaction request into the request queue.
        *
        * 3. If the transaction is new, copy it to all other peer's send queue
        *
        * 4. Put a Tip Request packet into the Peer's egress queue
        *
        * 5. Check if there is a new transaction request from the peer, if yes respond to that.
        *
        * */
        long threadId = Thread.currentThread().getId();
        {
            Peer peer = (Peer) node.getPeer(threadId);

            if (peer.getReceiveQueueCount() > 0) {
                ByteBuffer buffer = peer.dequeueRecvPacket();
                processIncomingPacket(peer, buffer);
            }
            ByteBuffer tipRequest = null;
            try {
                tipRequest = ByteBuffer.wrap(node.getTipRequest());
            } catch (Exception e) {
                e.printStackTrace();
            }
            peer.enqueueSendPacket(tipRequest);
        }
    }

    private void  processIncomingPacket (Peer peer, ByteBuffer bytebuf) {
        byte[] buf = new byte[bytebuf.remaining()];
        bytebuf.get(buf);
        Hash receivedTransactionHash = null;
        boolean cached = false;
        TransactionViewModel receivedTransactionViewModel = null;
        boolean stored = false;

        ByteBuffer digest = null;
        try {
            digest = getBytesDigest(buf);
        } catch (NoSuchAlgorithmException e) {
           log.error("Unable to get Byte Digest", e);
        }

        synchronized (recentSeenBytes) {
            cached = (receivedTransactionHash = recentSeenBytes.get(digest)) != null;
        }

        if (!cached) {
            //if not, then validate
            receivedTransactionViewModel = new TransactionViewModel(buf, TransactionHash.calculate(buf, TransactionViewModel.TRINARY_SIZE, SpongeFactory.create(SpongeFactory.Mode.CURLP81)));
            receivedTransactionHash = receivedTransactionViewModel.getHash();
            transactionValidator.runValidation(receivedTransactionViewModel, transactionValidator.getMinWeightMagnitude());

            synchronized (recentSeenBytes) {
                recentSeenBytes.put(digest, receivedTransactionHash);
            }
        }

        Hash requestedHash = HashFactory.TRANSACTION.create(buf, TransactionViewModel.SIZE, node.getHashSize());
        if (requestedHash.equals(receivedTransactionHash)) {
            //requesting a random tip
            requestedHash = Hash.NULL_HASH;
        }

        prepareResponse(requestedHash, peer);



        try {
            stored = receivedTransactionViewModel.store(tangle, snapshotProvider.getInitialSnapshot());
        } catch (Exception e) {
            log.error("Error accessing persistence store.", e);
            peer.incInvalidTransactions();
        }

        if (stored) {
            receivedTransactionViewModel.setArrivalTime(System.currentTimeMillis());
            try {
                transactionValidator.updateStatus(receivedTransactionViewModel);
                receivedTransactionViewModel.updateSender(peer.getAddress().toString());
                receivedTransactionViewModel.update(tangle, snapshotProvider.getInitialSnapshot(),"arrivalTime|sender");
            } catch (Exception e) {
                log.error("Error updating transactions.", e);
            }
            peer.incNewTransactions();
            node.broadcast(receivedTransactionViewModel);
        }


    }



    private ByteBuffer getBytesDigest(byte[] receivedData) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(receivedData, 0, TransactionViewModel.SIZE);
        return ByteBuffer.wrap(digest.digest());
    }


    private void prepareResponse(Hash requestedHash, Peer peer) {


        TransactionViewModel transactionViewModel = null;
        Hash transactionPointer;

        //retrieve requested transaction
        if (requestedHash.equals(Hash.NULL_HASH)) {
            //Random Tip Request
            try {
                    peer.incRandomTransactionRequests();
                    transactionPointer = node.getRandomTipPointer();
                    transactionViewModel = TransactionViewModel.fromHash(tangle, transactionPointer);
            } catch (Exception e) {
                log.error("Error getting random tip.", e);
            }
        } else {
            //find requested trytes
            try {
                //transactionViewModel = TransactionViewModel.find(Arrays.copyOf(requestedHash.bytes(), TransactionRequester.REQUEST_HASH_SIZE));
                transactionViewModel = TransactionViewModel.fromHash(tangle, HashFactory.TRANSACTION.create(requestedHash.bytes(), 0, node.getHashSize()));
                //log.debug("Requested Hash: " + requestedHash + " \nFound: " + transactionViewModel.getHash());
            } catch (Exception e) {
                log.error("Error while searching for transaction.", e);
            }
        }

        if (transactionViewModel != null && transactionViewModel.getType() == TransactionViewModel.FILLED_SLOT) {
            //send trytes back to neighbor
            try {

                byte payload [] = transactionViewModel.getBytes();
                peer.enqueueSendPacket(ByteBuffer.wrap(payload));
                ByteBuffer digest = getBytesDigest(payload);
                synchronized (recentSeenBytes) {
                    recentSeenBytes.put(digest, transactionViewModel.getHash());
                }
            } catch (Exception e) {
                log.error("Error fetching transaction to request.", e);
            }
        } else {
            //trytes not found
            if (!requestedHash.equals(Hash.NULL_HASH)) {
                //request is an actual transaction and missing in request queue add it.
                try {
                    node.addTransactionToRequest(requestedHash);

                } catch (Exception e) {
                    log.error("Error adding transaction to request.", e);
                }

            }
        }


    }

}


class FIFOCache<K, V> {

    private final int capacity;
    private final double dropRate;
    private LinkedHashMap<K, V> map;
    private final SecureRandom rnd = new SecureRandom();

    public FIFOCache(int capacity, double dropRate) {
        this.capacity = capacity;
        this.dropRate = dropRate;
        this.map = new LinkedHashMap<>();
    }

    public V get(K key) {
        V value = this.map.get(key);
        if (value != null && (rnd.nextDouble() < this.dropRate)) {
            this.map.remove(key);
            return null;
        }
        return value;
    }

    public V put(K key, V value) {
        if (this.map.containsKey(key)) {
            return value;
        }
        if (this.map.size() >= this.capacity) {
            Iterator<K> it = this.map.keySet().iterator();
            it.next();
            it.remove();
        }
        return this.map.put(key, value);
    }
}
