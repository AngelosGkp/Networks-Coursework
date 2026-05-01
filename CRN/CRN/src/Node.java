// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Angelos Gkoupidenis
//  230022354
//  angelos.gkoupidenis@city.ac.uk


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {

    private String nodeName;
    private byte[] nodeHashID;
    private DatagramSocket socket;
    private final Random random = new Random();

    //stops us from doing the expensive network exploration, as initially the node search was taking a very long time to complete
    private boolean hasExplored = false;

    //allows for a wider exploration of the network
    private Map<String, String> seenNodes    = new HashMap<>();
    private Map<String, String> addressStore = new HashMap<>();
    private Map<String, String> dataStore    = new HashMap<>();

    //when a request is sent, I put its txid in here with a null value, and when the matching response arrives, we fill in the value
    private Map<String, String> responseMap  = new HashMap<>();
    private Deque<String> relayStack = new ArrayDeque<>();


    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        this.nodeHashID = HashID.getHash(nodeName);
        addressStore.put(nodeName, ""); //this gets overwritten with the real port in openPort()
    }

    public void openPort(int portNumber) throws Exception {
        this.socket = new DatagramSocket(portNumber);
        String ownAddress = InetAddress.getLocalHost().getHostAddress() + ":" + portNumber;
        addressStore.put(nodeName, ownAddress);
    }

    public byte[] getHash() throws Exception {
        return HashID.getHash(this.nodeName);
    }

    private byte[] generateTxID() {
        byte[] txid = new byte[2];
        do {
            random.nextBytes(txid);
        } while (txid[0] == 0x20 || txid[1] == 0x20);
        return txid;
    }

    //encoding for strict formating
    private String encodeString(String s) {
        if (s == null) s = "";
        int spaceCount = 0;
        for (char c : s.toCharArray()) {
            if (c == ' ') spaceCount++;
        }
        return spaceCount + " " + s + " ";
    }

    //reads the next CRN encoded string from `raw` starting at `offset`
    private Object[] decodeNextString(String raw, int offset) {
        try {
            int spaceIdx = raw.indexOf(' ', offset);
            if (spaceIdx < 0) return null;
            int numSpaces = Integer.parseInt(raw.substring(offset, spaceIdx));
            int pos = spaceIdx + 1;
            int spacesFound = 0;
            while (pos < raw.length()) {
                if (raw.charAt(pos) == ' ') {
                    if (spacesFound == numSpaces) {
                        String decoded = raw.substring(spaceIdx + 1, pos);
                        return new Object[]{decoded, pos + 1};
                    }
                    spacesFound++;
                }
                pos++;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    //for single string only
    private String decodeStringRaw(String raw) {
        try {
            int firstSpace = raw.indexOf(' ');

            if (firstSpace < 0)
                return "";

            int numSpaces = Integer.parseInt(raw.substring(0, firstSpace));
            String rest = raw.substring(firstSpace + 1);
            int spacesFound = 0;

            for (int i = 0; i < rest.length(); i++) {
                if (rest.charAt(i) == ' ') {
                    if (spacesFound == numSpaces) return rest.substring(0, i);
                    spacesFound++;
                }
            }
            return rest;
        } catch (Exception e) {
            return "";
        }
    }

    private byte[] hexToBytes(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    //adding a node address to the store
    private void learnAddress(String name, String address) throws Exception {
        if (name.equals(this.nodeName)) return;
        seenNodes.put(name, address);

        if (addressStore.containsKey(name)) {
            addressStore.put(name, address);
            return;
        }

        //check how many nodes I already have at this distance from us
        byte[] targetHash = HashID.getHash(name);
        int dist = 256 - HashID.getDistance(nodeHashID, targetHash);
        int countAtSameDistance = 0;
        for (String n : addressStore.keySet()) {

            if (n.equals(this.nodeName))
                continue;

            byte[] h = HashID.getHash(n);
            int d = 256 - HashID.getDistance(nodeHashID, h);
            if (d == dist) countAtSameDistance++;
        }

        if (countAtSameDistance < 3) {
            addressStore.put(name, address);
        }
    }

    private List<Map.Entry<String, String>> getClosestNodes(byte[] targetHash, int count) throws Exception {
        List<Map.Entry<String, String>> all = new ArrayList<>(addressStore.entrySet());
        all.sort((a, b) -> {
            try {
                int bitsA = HashID.getDistance(targetHash, HashID.getHash(a.getKey()));
                int bitsB = HashID.getDistance(targetHash, HashID.getHash(b.getKey()));
                return Integer.compare(bitsB, bitsA);
            } catch (Exception e) {
                return 0;
            }
        });
        return all.subList(0, Math.min(count, all.size()));
    }

    private boolean isOneOfClosest(byte[] targetHash) throws Exception {
        int ourDistance = HashID.getDistance(nodeHashID, targetHash);
        int strictlyCloser = 0;
        for (String name : addressStore.keySet()) {
            if (name.equals(this.nodeName))
                continue;

            byte[] h = HashID.getHash(name);
            int d = HashID.getDistance(h, targetHash);
            if (d > ourDistance) strictlyCloser++;  //if there are more matching bits, the node is closer
            if (strictlyCloser >= 3) return false;
        }
        return true;
    }

    //sends a message and waits up to 5 seconds for a response. it then retries up to 3 times if no response arrives
    private String sendAndWait(InetAddress address, int port, byte[] txid, String message) throws Exception {
        if (!relayStack.isEmpty()) {
            return sendViaRelay(address, port, txid, message);
        }

        String txKey = new String(txid, StandardCharsets.ISO_8859_1);
        responseMap.put(txKey, null);

        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);

        for (int attempt = 0; attempt < 3; attempt++) { //retries up to 3 times,
            socket.send(new DatagramPacket(msgBytes, msgBytes.length, address, port));

            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                socket.setSoTimeout((int) Math.max(deadline - System.currentTimeMillis(), 1));
                try {
                    byte[] buffer = new byte[65535];
                    DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                    socket.receive(incoming);
                    processMessage(incoming);
                    if (responseMap.containsKey(txKey) && responseMap.get(txKey) != null) {
                        return responseMap.remove(txKey);
                    }
                } catch (SocketTimeoutException e) {
                }
            }
        }
        responseMap.remove(txKey);
        return null;
    }

    //faster version used only during network exploration. used only for nearest requests because initially exploration was taking a long time
    private String sendAndWaitFast(InetAddress address, int port, byte[] txid, String message) throws Exception {
        String txKey = new String(txid, StandardCharsets.ISO_8859_1);
        responseMap.put(txKey, null);
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);

        socket.send(new DatagramPacket(msgBytes, msgBytes.length, address, port));
        long deadline = System.currentTimeMillis() + 500; //500 ms for quick search
        while (System.currentTimeMillis() < deadline) {
            socket.setSoTimeout((int) Math.max(deadline - System.currentTimeMillis(), 1));
            try {
                byte[] buffer = new byte[65535];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                socket.receive(incoming);
                processMessage(incoming);
                if (responseMap.containsKey(txKey) && responseMap.get(txKey) != null) {
                    return responseMap.remove(txKey);
                }
            } catch (SocketTimeoutException e) {}
        }
        responseMap.remove(txKey);
        return null;
    }

    //wraps a message in V frames and sends it through the relay chain
    private String sendViaRelay(InetAddress address, int port, byte[] txid, String message) throws Exception {
        List<String> relays = new ArrayList<>(relayStack);
        Collections.reverse(relays);

        //figuring out the name of the final destination node
        String targetNodeName = null;
        for (Map.Entry<String, String> e : addressStore.entrySet()) {
            if (e.getValue().equals(address.getHostAddress() + ":" + port)) {
                targetNodeName = e.getKey();
                break;
            }
        }

        if (targetNodeName == null)
            return null;

        String currentMessage = message;
        String currentTarget  = targetNodeName;

        for (int i = relays.size() - 1; i >= 0; i--) {
            String relayNode    = relays.get(i);
            byte[] relayTxid    = generateTxID();
            String relayTxidStr = new String(relayTxid, StandardCharsets.ISO_8859_1);
            currentMessage = relayTxidStr + " V " + encodeString(currentTarget) + currentMessage;
            currentTarget  = relayNode;
        }

        //send to the first relay node in the chain
        String firstRelayAddr = addressStore.get(relays.get(0));

        if (firstRelayAddr == null)
            return null;

        String[] parts = firstRelayAddr.split(":");

        if (parts.length != 2)
            return null;

        InetAddress relayAddress = InetAddress.getByName(parts[0]);
        int relayPort = Integer.parseInt(parts[1]);

        String txKey = new String(txid, StandardCharsets.ISO_8859_1);
        responseMap.put(txKey, null);
        byte[] msgBytes = currentMessage.getBytes(StandardCharsets.UTF_8);

        for (int attempt = 0; attempt < 3; attempt++) {
            socket.send(new DatagramPacket(msgBytes, msgBytes.length, relayAddress, relayPort));
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                socket.setSoTimeout((int) Math.max(deadline - System.currentTimeMillis(), 1));
                try {
                    byte[] buffer = new byte[65535];
                    DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                    socket.receive(incoming);
                    processMessage(incoming);
                    if (responseMap.containsKey(txKey) && responseMap.get(txKey) != null) {
                        return responseMap.remove(txKey);
                    }
                } catch (SocketTimeoutException e) {}
            }
        }
        responseMap.remove(txKey);
        return null;
    }

    private void sendNearestRequest(InetAddress address, int port, byte[] targetHash) throws Exception {
        byte[] txid = generateTxID();
        String hashHex = HashID.bytesToHex(targetHash);
        String message = new String(txid, StandardCharsets.ISO_8859_1) + " N " + hashHex + " ";

        String response = sendAndWaitFast(address, port, txid, message);
        //uses the fast version of send and wait since we call this a lot during exploration. again, to reduce excessive wait

        if (response != null) {
            int offset = 0;
            while (offset < response.length()) {
                Object[] kr = decodeNextString(response, offset);
                if (kr == null) break;
                String key = (String) kr[0]; offset = (int) kr[1];

                Object[] vr = decodeNextString(response, offset);
                if (vr == null) break;
                String val = (String) vr[0]; offset = (int) vr[1];

                if (key.startsWith("N:") && val.contains(":")) {
                    learnAddress(key, val);
                }
            }
        }
    }

    //finds closest nodes repeatedly asking the closest nodes we know
    private void findClosestNodes(byte[] targetHash) throws Exception {
        Set<String> queried = new HashSet<>();
        boolean foundNew = true;
        int maxRounds = 3;

        //uses all the addresses the algorithm already knows
        Map<String, String> allCandidates = new HashMap<>(seenNodes);
        allCandidates.putAll(addressStore);

        while (foundNew && maxRounds-- > 0) {
            foundNew = false;

            //sorts by how close each node is to the target
            List<Map.Entry<String, String>> sorted = new ArrayList<>(allCandidates.entrySet());
            sorted.sort((a, b) -> {
                try {
                    int bitsA = HashID.getDistance(targetHash, HashID.getHash(a.getKey()));
                    int bitsB = HashID.getDistance(targetHash, HashID.getHash(b.getKey()));
                    return Integer.compare(bitsB, bitsA);
                } catch (Exception e) {
                    return 0;
                }
            });

            for (Map.Entry<String, String> entry : sorted.subList(0, Math.min(10, sorted.size()))) {
                String name = entry.getKey();
                String addr = entry.getValue();

                if (queried.contains(name))
                    continue;

                if (addr == null || addr.isEmpty())
                    continue;

                if (name.equals(this.nodeName))
                    continue;

                queried.add(name);
                foundNew = true; //keep going as long as there are unqueried nodes

                String[] parts = addr.split(":");
                if (parts.length != 2)
                    continue;

                try {
                    InetAddress address = InetAddress.getByName(parts[0]);
                    int nodePort = Integer.parseInt(parts[1]);
                    int before = seenNodes.size();
                    sendNearestRequest(address, nodePort, targetHash);
                    //if new nodes are found, have them ready for next round
                    if (seenNodes.size() > before) allCandidates.putAll(seenNodes);
                } catch (Exception e) {
                }
            }
        }
    }

    public void handleIncomingMessages(int delay) throws Exception {
        //reset the exploration flag
        if (delay > 0) hasExplored = false;

        long deadline = (delay > 0) ? System.currentTimeMillis() + delay : Long.MAX_VALUE;

        while (System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();
            socket.setSoTimeout(delay > 0 ? (int) Math.max(remaining, 1) : 1000);
            try {
                byte[] buffer = new byte[65535];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                processMessage(packet);
            } catch (SocketTimeoutException e) {
            }
        }
    }

    //for every incoming packet, I parse the header, figure out the type, and hand it off to the right handler
    private void processMessage(DatagramPacket packet) {
        try {
            String raw = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
            if (raw.length() < 4)
                return;

            String txid = raw.substring(0, 2);
            if (raw.charAt(2) != ' ')
                return;

            char type = raw.charAt(3);
            String rest = raw.length() > 5 ? raw.substring(5) : "";

            switch (type) {
                case 'G': handleNameRequest(packet, txid);          break;
                case 'H': handleNameResponse(txid, rest);           break;
                case 'N': handleNearestRequest(packet, txid, rest); break;
                case 'O': handleNearestResponse(txid, rest);        break;
                case 'E': handleExistRequest(packet, txid, rest);   break;
                case 'F': handleExistResponse(txid, rest);          break;
                case 'R': handleReadRequest(packet, txid, rest);    break;
                case 'S': handleReadResponse(txid, rest);           break;
                case 'W': handleWriteRequest(packet, txid, rest);   break;
                case 'X': handleWriteResponse(txid, rest);          break;
                case 'C': handleCASRequest(packet, txid, rest);     break;
                case 'D': handleCASResponse(txid, rest);            break;
                case 'V': handleRelay(packet, txid, rest);          break;
                case 'I': break;
                default:  break; //for any unknown types, discard
            }
        } catch (Exception e) {
        }
    }

    //replying to whichever node asked for the specific name
    //whilst also asking it if any are nearby
    private void handleNameRequest(DatagramPacket packet, String txid) throws Exception {
        String response = txid + " H " + encodeString(nodeName);
        byte[] out = response.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(out, out.length, packet.getAddress(), packet.getPort()));

        try {
            byte[] myTxid = generateTxID();
            String msg = new String(myTxid, StandardCharsets.ISO_8859_1) + " N " + HashID.bytesToHex(nodeHashID) + " ";
            byte[] msgOut = msg.getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(msgOut, msgOut.length, packet.getAddress(), packet.getPort()));
        } catch (Exception e) {}
    }

    private void handleNameResponse(String txid, String rest) {
        if (responseMap.containsKey(txid)) {
            responseMap.put(txid, rest.isEmpty() ? "" : rest);
        }
    }

    private void handleNearestRequest(DatagramPacket packet, String txid, String rest) throws Exception {
        String hex = rest.trim();
        if (hex.length() != 64)
            return;

        byte[] targetHash = hexToBytes(hex);
        List<Map.Entry<String, String>> closest = getClosestNodes(targetHash, 3);

        StringBuilder sb = new StringBuilder();
        sb.append(txid).append(" O ");
        for (Map.Entry<String, String> e : closest) {
            sb.append(encodeString(e.getKey()));
            sb.append(encodeString(e.getValue()));
        }

        byte[] out = sb.toString().getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(out, out.length, packet.getAddress(), packet.getPort()));
    }

    //if a response is recorder, store it in responseMap and analyse
    private void handleNearestResponse(String txid, String rest) {
        if (responseMap.containsKey(txid)) responseMap.put(txid, rest);
        try {
            int offset = 0;
            while (offset < rest.length()) {
                Object[] kr = decodeNextString(rest, offset);
                if (kr == null) break;
                String key = (String) kr[0]; offset = (int) kr[1];

                Object[] vr = decodeNextString(rest, offset);
                if (vr == null) break;
                String val = (String) vr[0]; offset = (int) vr[1];

                if (key.startsWith("N:") && val.contains(":")) learnAddress(key, val);
            }
        } catch (Exception e) {}
    }

    //if being asked whether a key is held
    private void handleExistRequest(DatagramPacket packet, String txid, String rest) throws Exception {
        Object[] kr = decodeNextString(rest, 0);
        if (kr == null)
            return;

        String key = (String) kr[0];
        byte[] targetHash = HashID.getHash(key);
        boolean weHaveIt  = dataStore.containsKey(key) || addressStore.containsKey(key);
        boolean weAreClose = isOneOfClosest(targetHash);

        String code;
        if (weHaveIt) code = "Y";
        else if (weAreClose) code = "N";
        else code = "?";

        String response = txid + " F " + code + " ";
        byte[] out = response.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(out, out.length, packet.getAddress(), packet.getPort()));
    }

    private void handleExistResponse(String txid, String rest) {
        if (responseMap.containsKey(txid)) responseMap.put(txid, rest.trim());
    }

    //for reading the actual value
    private void handleReadRequest(DatagramPacket packet, String txid, String rest) throws Exception {
        Object[] kr = decodeNextString(rest, 0);

        if (kr == null)
            return;

        String key = (String) kr[0];

        byte[] targetHash = HashID.getHash(key);
        String stored = dataStore.containsKey(key)    ? dataStore.get(key) : addressStore.containsKey(key) ? addressStore.get(key) : null;
        boolean weAreClose = isOneOfClosest(targetHash);

        String response;
        if (stored != null)
            response = txid + " S Y " + encodeString(stored);
        else if (weAreClose)
            response = txid + " S N ";
        else
            response = txid + " S ? ";

        byte[] out = response.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(out, out.length, packet.getAddress(), packet.getPort()));
    }

    private void handleReadResponse(String txid, String rest) {
        if (responseMap.containsKey(txid)) responseMap.put(txid, rest.trim());
    }

    //for storing a key or value
    private void handleWriteRequest(DatagramPacket packet, String txid, String rest) throws Exception {
        Object[] kr = decodeNextString(rest, 0);
        if (kr == null) return;
        String key = (String) kr[0]; int offset = (int) kr[1];

        Object[] vr = decodeNextString(rest, offset);
        if (vr == null) return;
        String value = (String) vr[0];

        //ensures that the node addresses we see are learned
        if (key.startsWith("N:") && value.contains(":")) learnAddress(key, value);

        byte[] targetHash = HashID.getHash(key);
        boolean weAlreadyHaveIt = dataStore.containsKey(key)
                || (key.startsWith("N:") && addressStore.containsKey(key));
        boolean weAreClose = isOneOfClosest(targetHash);

        String code;
        if (weAlreadyHaveIt) {
            code = "R";
            if (key.startsWith("N:")) addressStore.put(key, value);
            else dataStore.put(key, value);
        } else if (weAreClose) {
            code = "A";
            if (!key.startsWith("N:")) dataStore.put(key, value);
        } else {
            code = "X";
        }

        String response = txid + " X " + code + " ";
        byte[] out = response.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(out, out.length, packet.getAddress(), packet.getPort()));
    }

    private void handleWriteResponse(String txid, String rest) {
        if (responseMap.containsKey(txid)) responseMap.put(txid, rest.trim());
    }

    //used for instances where if the stored value matches the current value, replace it with the new value
    private void handleCASRequest(DatagramPacket packet, String txid, String rest) throws Exception {
        Object[] kr = decodeNextString(rest, 0);
        if (kr == null) return;
        String key = (String) kr[0]; int offset = (int) kr[1];

        Object[] vr1 = decodeNextString(rest, offset);
        if (vr1 == null) return;
        String currentValue = (String) vr1[0]; offset = (int) vr1[1];

        Object[] vr2 = decodeNextString(rest, offset);
        if (vr2 == null) return;
        String newValue = (String) vr2[0];

        byte[] targetHash  = HashID.getHash(key);
        boolean weHaveIt   = dataStore.containsKey(key)
                || (key.startsWith("N:") && addressStore.containsKey(key));
        boolean weAreClose = isOneOfClosest(targetHash);

        String code;
        if (weHaveIt) {
            String stored = dataStore.containsKey(key) ? dataStore.get(key) : addressStore.get(key);
            if (stored.equals(currentValue)) {
                code = "R";
                if (key.startsWith("N:")) addressStore.put(key, newValue);
                else dataStore.put(key, newValue);
            } else {
                code = "N";
            }
        } else if (weAreClose) {
            code = "A";
            if (key.startsWith("N:")) learnAddress(key, newValue);
            else dataStore.put(key, newValue);
        } else {
            code = "X";
        }

        String response = txid + " D " + code + " ";
        byte[] out = response.getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(out, out.length, packet.getAddress(), packet.getPort()));
    }

    private void handleCASResponse(String txid, String rest) {
        if (responseMap.containsKey(txid)) responseMap.put(txid, rest.trim());
    }

    //if a message needs to be forwarded to us, the txid is swapped so responses come back, then it is swap back
    private void handleRelay(DatagramPacket packet, String txid, String rest) throws Exception {
        Object[] kr = decodeNextString(rest, 0);
        if (kr == null) return;
        String targetNodeName = (String) kr[0];
        int offset = (int) kr[1];

        //message to forward
        String embeddedMessage = rest.substring(offset);
        if (embeddedMessage.isEmpty()) return;

        String targetAddress = addressStore.get(targetNodeName);
        if (targetAddress == null) targetAddress = seenNodes.get(targetNodeName);
        if (targetAddress == null || targetAddress.isEmpty()) return;

        String[] parts = targetAddress.split(":");
        if (parts.length != 2) return;

        InetAddress targetAddr = InetAddress.getByName(parts[0]);
        int targetPort = Integer.parseInt(parts[1]);
        InetAddress originalSender = packet.getAddress();
        int originalPort = packet.getPort();
        String relayTxid = txid;

        byte[] newTxid    = generateTxID();
        String newTxidStr = new String(newTxid, StandardCharsets.ISO_8859_1);

        String forwardedMessage = newTxidStr + embeddedMessage.substring(2);

        //prevent blocking while waiting for a response
        Thread relayThread = new Thread(() -> {
            try {
                byte[] forwardBytes = forwardedMessage.getBytes(StandardCharsets.UTF_8);
                String rawResponse  = null;

                for (int attempt = 0; attempt < 3 && rawResponse == null; attempt++) {
                    socket.send(new DatagramPacket(forwardBytes, forwardBytes.length,
                            targetAddr, targetPort));

                    long deadline = System.currentTimeMillis() + 5000;
                    while (System.currentTimeMillis() < deadline) {
                        socket.setSoTimeout((int) Math.max(deadline - System.currentTimeMillis(), 1));
                        try {
                            byte[] buffer = new byte[65535];
                            DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                            socket.receive(incoming);

                            String raw = new String(incoming.getData(), 0,
                                    incoming.getLength(), StandardCharsets.UTF_8);

                            //checks if this is the response to our message
                            if (raw.length() >= 2 && raw.substring(0, 2).equals(newTxidStr)) {
                                rawResponse = raw;
                                break;
                            } else {
                                processMessage(incoming);
                            }
                        } catch (SocketTimeoutException e) {}
                    }
                }

                if (rawResponse == null) return; // target never responded

                //swap the txid back to the relay txid
                String finalResponse = relayTxid + rawResponse.substring(2);
                byte[] finalBytes    = finalResponse.getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(finalBytes, finalBytes.length,
                        originalSender, originalPort));

            } catch (Exception e) {}
        });
        relayThread.setDaemon(true);
        relayThread.start();
    }

    public boolean isActive(String nodeName) throws Exception {
        String address = addressStore.get(nodeName);
        if (address == null || address.isEmpty()) return false;
        String[] parts = address.split(":");
        if (parts.length != 2) return false;

        InetAddress addr = InetAddress.getByName(parts[0]);
        int port = Integer.parseInt(parts[1]);

        byte[] txid = generateTxID();
        String message = new String(txid, StandardCharsets.ISO_8859_1) + " G ";
        String response = sendAndWait(addr, port, txid, message);

        if (response == null) return false;
        return nodeName.equals(decodeStringRaw(response));
    }

    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName); }

    public void popRelay()  throws Exception {
        if (!relayStack.isEmpty())
            relayStack.pop(); }

    public boolean exists(String key) throws Exception {
        if (dataStore.containsKey(key))
            return true;
        if (addressStore.containsKey(key))
            return true;

        byte[] targetHash = HashID.getHash(key);

        if (!hasExplored) {
            hasExplored = true;
            findClosestNodes(nodeHashID);
        }

        List<Map.Entry<String, String>> allKnown = getClosestNodes(targetHash, addressStore.size());
        for (Map.Entry<String, String> entry : allKnown) {
            String addr = entry.getValue();
            if (addr == null || addr.isEmpty()) continue;
            if (entry.getKey().equals(this.nodeName)) continue;
            String[] parts = addr.split(":");
            if (parts.length != 2) continue;
            try {
                InetAddress address = InetAddress.getByName(parts[0]);
                int port = Integer.parseInt(parts[1]);
                byte[] txid    = generateTxID();
                String message = new String(txid, StandardCharsets.ISO_8859_1) + " E " + encodeString(key);
                String response = sendAndWait(address, port, txid, message);
                if (response == null) continue;
                if (response.trim().equals("Y")) return true;
                if (response.trim().equals("N")) return false;
            } catch (Exception e) {}
        }
        return false;
    }

    public String read(String key) throws Exception {
        if (dataStore.containsKey(key))    return dataStore.get(key);
        if (addressStore.containsKey(key)) return addressStore.get(key);

        byte[] targetHash = HashID.getHash(key);

        //explore the network once per session
        if (!hasExplored) {
            hasExplored = true;
            findClosestNodes(nodeHashID);
        }

        List<Map.Entry<String, String>> allKnown = getClosestNodes(targetHash, addressStore.size());
        for (Map.Entry<String, String> entry : allKnown) {
            String addr = entry.getValue();
            if (addr == null || addr.isEmpty()) continue;
            if (entry.getKey().equals(this.nodeName)) continue;
            String[] parts = addr.split(":");
            if (parts.length != 2) continue;
            try {
                InetAddress address = InetAddress.getByName(parts[0]);
                int port = Integer.parseInt(parts[1]);
                byte[] txid    = generateTxID();
                String message = new String(txid, StandardCharsets.ISO_8859_1) + " R " + encodeString(key);
                String response = sendAndWait(address, port, txid, message);
                if (response == null) continue;
                if (response.startsWith("Y ")) return decodeStringRaw(response.substring(2));
                //if "?" is a response, it is not the right node. ask them for nearest nodes
                if (response.trim().equals("?")) sendNearestRequest(address, port, targetHash);
            } catch (Exception e) {}
        }

        //if the "?" queries above turned up new nodes to try
        allKnown = getClosestNodes(targetHash, addressStore.size());
        for (Map.Entry<String, String> entry : allKnown) {
            String addr = entry.getValue();

            if (addr == null || addr.isEmpty())
                continue;
            if (entry.getKey().equals(this.nodeName))
                continue;

            String[] parts = addr.split(":");
            if (parts.length != 2) continue;
            try {
                InetAddress address = InetAddress.getByName(parts[0]);
                int port = Integer.parseInt(parts[1]);
                byte[] txid = generateTxID();
                String message = new String(txid, StandardCharsets.ISO_8859_1) + " R " + encodeString(key);
                String response = sendAndWait(address, port, txid, message);
                if (response == null)
                    continue;
                if (response.startsWith("Y "))
                    return decodeStringRaw(response.substring(2));
            } catch (Exception e) {}
        }
        return null;
    }

    public boolean write(String key, String value) throws Exception {
        byte[] targetHash = HashID.getHash(key);

        if (isOneOfClosest(targetHash)) {
            if (key.startsWith("N:")) learnAddress(key, value);
            else dataStore.put(key, value);
            return true;
        }

        List<Map.Entry<String, String>> closest = getClosestNodes(targetHash, 3);
        boolean anySuccess = false;
        for (Map.Entry<String, String> entry : closest) {
            String addr = entry.getValue();
            if (addr == null || addr.isEmpty())
                continue;
            String[] parts = addr.split(":");
            if (parts.length != 2)
                continue;
            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            byte[] txid    = generateTxID();
            String message = new String(txid, StandardCharsets.ISO_8859_1)
                    + " W " + encodeString(key) + encodeString(value);
            String response = sendAndWait(address, port, txid, message);
            if (response != null && (response.startsWith("A") || response.startsWith("R"))) {
                anySuccess = true;
            }
        }
        return anySuccess;
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        byte[] targetHash = HashID.getHash(key);

        //handle the local case first
        if (dataStore.containsKey(key)) {
            if (dataStore.get(key).equals(currentValue)) {
                dataStore.put(key, newValue);
                return true;
            }
            return false;
        }

        //send CAS request to the 3 closest nodes
        List<Map.Entry<String, String>> closest = getClosestNodes(targetHash, 3);
        for (Map.Entry<String, String> entry : closest) {
            String addr = entry.getValue();
            if (addr == null || addr.isEmpty())
                continue;
            if (entry.getKey().equals(this.nodeName))
                continue;

            String[] parts = addr.split(":");

            if (parts.length != 2)
                continue;

            InetAddress address = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            byte[] txid = generateTxID();
            String message = new String(txid, StandardCharsets.ISO_8859_1) + " C " + encodeString(key) + encodeString(currentValue) + encodeString(newValue);
            String response = sendAndWait(address, port, txid, message);

            if (response == null)
                continue;

            if (response.trim().equals("R") || response.trim().equals("A"))
                return true;

            if (response.trim().equals("N"))
                return false;
        }
        return false;
    }
}