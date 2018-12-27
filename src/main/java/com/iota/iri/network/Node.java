package com.iota.iri.network;

import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.TransactionValidator;
import com.iota.iri.conf.NodeConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import org.apache.commons.lang3.tuple.Pair;
import com.iota.iri.zmq.MessageQ;
import com.iota.iri.service.snapshot.SnapshotProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.concurrent.ScheduledExecutorService;

import java.io.IOException;
import java.net.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.util.concurrent.TimeUnit.*;

/**
 * Class Node is the core class for handling IRI gossip protocol packets.
 * Both TCP and UDP receivers will pass incoming packets to this class's object.
 * It is also responsible for validating and storing the received transactions
 * into the Tangle Database. <br>
 *
 * The Gossip protocol is specific to IRI nodes and is used for spamming and requesting
 * new transactions between IRI peers. Every message sent on Gossip protocol consists of two
 * parts - the transaction in binary encoded format followed by a hash of another transaction to
 * be requested. The receiving entity will save the newly received transaction into
 * its own database and will respond with the received requested transaction - if
 * available in its own storgage.
 *
 */
public class Node {
    private static final Logger log = LoggerFactory.getLogger(Node.class);
    private TCPProtocol tcp;
    private UDPProtocol udp;


    private int BROADCAST_QUEUE_SIZE;
    private int RECV_QUEUE_SIZE;
    private int REPLY_QUEUE_SIZE;
    private static final int PAUSE_BETWEEN_TRANSACTIONS = 1;

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final ConcurrentSkipListSet<TransactionViewModel> broadcastQueue = weightQueue();
    private final ConcurrentSkipListSet<Pair<Hash, Peer>> replyQueue = weightQueueHashPair();

    private final List<Peer> peers = new CopyOnWriteArrayList<Peer>();
    private final SnapshotProvider snapshotProvider;

    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    private final NodeConfig configuration;
    private final Tangle tangle;
    private final TipsViewModel tipsViewModel;
    private final TransactionValidator transactionValidator;
    private final LatestMilestoneTracker latestMilestoneTracker;
    private final TransactionRequester transactionRequester;
    private final MessageQ messageQ;
    private final byte[] tipRequestingPacket = new byte[2048];

    private static final SecureRandom rnd = new SecureRandom();

    public Node(final Tangle tangle, SnapshotProvider snapshotProvider, final TransactionValidator transactionValidator, 
                final TransactionRequester transactionRequester,
                final TipsViewModel tipsViewModel,
                final LatestMilestoneTracker milestoneTracker, final MessageQ messageQ, final NodeConfig configuration
    ) {
        this.configuration = configuration;
        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider ;
        this.transactionValidator = transactionValidator;
        this.transactionRequester = transactionRequester;
        this.tipsViewModel = tipsViewModel;
        this.latestMilestoneTracker = milestoneTracker;
        this.messageQ = messageQ;

    }

    public TransactionValidator getTransactionValidator(){
        return transactionValidator;
    }

    private void configureProtocol(){
        try {
            tcp.init(configuration);
            udp.init(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Peer getPeer(final long threadId){
        int index = Math.toIntExact( (peers.size() - 1) % threadId);
        return peers.get(index);
    }

    public List<Peer> getPeers() {
        return peers;
    }
    
    
    public int getHashSize(){
        return configuration.getRequestHashSize();
    }

    /*TBD: Check whether sufficient time has elapsed before every time creating a new copy of TipRequest. */
    public byte[] getTipRequest() throws Exception {

        final TransactionViewModel transactionViewModel = TransactionViewModel.fromHash(tangle, latestMilestoneTracker.getLatestMilestoneHash());
        System.arraycopy(transactionViewModel.getBytes(), 0, tipRequestingPacket, 0, TransactionViewModel.SIZE);
        System.arraycopy(transactionViewModel.getHash().bytes(), 0, tipRequestingPacket, TransactionViewModel.SIZE,
                configuration.getRequestHashSize());
        //Hash.SIZE_IN_BYTES);
        return tipRequestingPacket;

    }

    public boolean isUriValid(final URI uri) {
        if (uri != null) {
            if (uri.getScheme().equals("tcp") || uri.getScheme().equals("udp")) {
                if ((new InetSocketAddress(uri.getHost(), uri.getPort()).getAddress() != null)) {
                    return true;
                }
            }
            log.error("'{}' is not a valid uri schema or resolvable address.", uri);
            return false;
        }
        log.error("Cannot read uri schema, please check neighbor config!");
        return false;
    }

    public Peer newNeighbor(final URI uri, boolean isConfigured) {
        if (isUriValid(uri)) {
                return new Peer(uri.getScheme(), new InetSocketAddress(uri.getHost(), uri.getPort()), isConfigured);
        }
        throw new RuntimeException(uri.toString());
    }

    public boolean removeNeighbor(final URI uri, boolean isConfigured) {
        final Peer neighbor = newNeighbor(uri, isConfigured);
        if (uri.getScheme().equals("tcp")) {
            peers.stream().filter(n -> n.equals(neighbor))
                    .forEach(Peer::clear);
        }
        return peers.remove(neighbor);
    }

    public static Optional<URI> uri(final String uri) {
        try {
            return Optional.of(new URI(uri));
        } catch (URISyntaxException e) {
            log.error("Uri {} raised URI Syntax Exception", uri);
        }
        return Optional.empty();
    }

    public int howManyNeighbors() {
        return peers.size();
    }

    public List<Peer> getNeighbors() {
        return peers;
    }

    public int getBroadcastQueueSize() {
        return 1;
    }

    public int getReceiveQueueSize() {
        return 1;
    }

    public int getReplyQueueSize() {
        return 1;
    }

    public int queuedTransactionsSize() {
        return 1;
    }


    public void addTransactionToRequest(Hash requestedHash){
        try {
            transactionRequester.requestTransaction(requestedHash, false);
        } catch (Exception e) {
            log.error("Error adding transaction to request.", e);
        }

    }

    private void configureNeighbor(){

        configuration.getNeighbors().stream().distinct()
            .filter(s -> !s.isEmpty())
            .map(Node::uri).map(Optional::get)
            .filter(u -> isUriValid(u))
            .map(u -> newNeighbor(u, true))
            .peek(u -> {
                log.info("-> Adding neighbor : {} ", u.getAddress());
                messageQ.publish("-> Adding Neighbor : %s", u.getAddress());
            }).forEach(n -> {
                if (n.connectionType().contains("tcp")) {
                    tcp.registerListener(n);
                }
                else if (n.connectionType().contains("udp")){
                    udp.registerListener(n);
                }
                peers.add(n);
            });
    }

    public void init() throws Exception {

        ScheduledExecutorService scheduledExecutorService =
                Executors.newScheduledThreadPool(4);

        final Runnable peerWorker = new PeerWorker(this, snapshotProvider, tangle);

        scheduledExecutorService.scheduleWithFixedDelay(peerWorker, 10, 1, SECONDS );

        //executor.submit(spawnBroadcasterThread());
        //executor.submit(spawnTipRequesterThread());
        //executor.submit(spawnNeighborDNSRefresherThread());
        //executor.submit(spawnProcessReceivedThread());
        //executor.submit(spawnReplyToRequestThread());
        executor.shutdown();
    }

    public void shutdown() throws Exception {
        tcp.shutdown();
        udp.shutdown();


    }

    public void broadcast(final TransactionViewModel transactionViewModel) {
        broadcastQueue.add(transactionViewModel);
        if (broadcastQueue.size() > BROADCAST_QUEUE_SIZE) {
            broadcastQueue.pollLast();
        }
    }

    public Hash getRandomTipPointer() throws Exception {
        /**TBD not clear what needs to be done here. To be merged with the latest change**/
        //Hash tip = rnd.nextDouble() < configuration.getpSendMilestone() ? milestoneTracker.getLatestMilestoneHash() : tipsViewModel.getRandomSolidTipHash();
        //return tip == null ? Hash.NULL_HASH : tip;
        return Hash.NULL_HASH;
    }


    private static ConcurrentSkipListSet<TransactionViewModel> weightQueue() {
        return new ConcurrentSkipListSet<>((transaction1, transaction2) -> {
            if (transaction1.weightMagnitude == transaction2.weightMagnitude) {
                for (int i = Hash.SIZE_IN_BYTES; i-- > 0; ) {
                    if (transaction1.getHash().bytes()[i] != transaction2.getHash().bytes()[i]) {
                        return transaction2.getHash().bytes()[i] - transaction1.getHash().bytes()[i];
                    }
                }
                return 0;
            }
            return transaction2.weightMagnitude - transaction1.weightMagnitude;
        });
    }

    //TODO generalize these weightQueues
    private static ConcurrentSkipListSet<Pair<Hash, Peer>> weightQueueHashPair() {
        return new ConcurrentSkipListSet<Pair<Hash, Peer>>((transaction1, transaction2) -> {
            Hash tx1 = transaction1.getLeft();
            Hash tx2 = transaction2.getLeft();

            for (int i = Hash.SIZE_IN_BYTES; i-- > 0; ) {
                if (tx1.bytes()[i] != tx2.bytes()[i]) {
                    return tx2.bytes()[i] - tx1.bytes()[i];
                }
            }
            return 0;

        });
    }
}
