// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Angelos Gkoupidenis
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
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

    private Map<String, String> addressStore = new HashMap<>();
    private Map<String, String> dataStore = new HashMap<>();
    private Deque<String> relayStack = new ArrayDeque<>();
    private Map<String, long[]> pendingRequests = new HashMap<>();
    private final Random random = new Random();

    private byte[] generateTextID() {
        byte[] txid = new byte[2];
        do {
            random.nextBytes(txid);
        } while (txid[0] == 0x20 || txid[1] == 0x20); // no spaces allowed
        return txid;
    }

    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        this.nodeHashID = HashID.getHash(nodeName);
    }

    public void openPort(int portNumber) throws Exception {
        this.socket = new DatagramSocket(portNumber);
    }

    public byte[] getHash() throws Exception {
        return HashID.getHash(this.nodeName); //this is a helper method for the get hash call local test needs
    }

    private void processMessage(DatagramPacket packet) {
        try {
            String raw = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);

            //used for message format: <2-byte txid> <space> <type>...
            if (raw.length() < 4) return;

            String txid = raw.substring(0, 2);

            if (raw.charAt(2) != ' ') return;

            char type = raw.charAt(3);

            String rest = raw.length() > 5 ? raw.substring(5) : "";

            switch (type) {
                case 'G': handleNameRequest(packet, txid); break;
                case 'H': handleNameResponse(txid, rest); break;
                case 'N': handleNearestRequest(packet, txid, rest); break;
                case 'O': handleNearestResponse(txid, rest); break;
                case 'E': handleExistRequest(packet, txid, rest); break;
                case 'F': handleExistResponse(txid, rest); break;
                case 'R': handleReadRequest(packet, txid, rest); break;
                case 'S': handleReadResponse(txid, rest); break;
                case 'W': handleWriteRequest(packet, txid, rest); break;
                case 'X': handleWriteResponse(txid, rest); break;
                case 'C': handleCASRequest(packet, txid, rest); break;
                case 'D': handleCASResponse(txid, rest); break;
                case 'V': handleRelay(packet, txid, rest); break;
                case 'I': break; // Information messages - discard
                default:  break; // Unknown type - discard safely
            }

        } catch (Exception e) {
        }
    }

    private void handleNameRequest(DatagramPacket packet, String txid) throws Exception {
        String response = txid + " H " + encodeString(nodeName);
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket reply = new DatagramPacket(
                responseBytes, responseBytes.length,
                packet.getAddress(), packet.getPort()
        );
        socket.send(reply);
    }

    private void handleNameResponse(String txid, String rest) {}
    private void handleNearestRequest(DatagramPacket packet, String txid, String rest) throws Exception {}
    private void handleNearestResponse(String txid, String rest) {}
    private void handleExistRequest(DatagramPacket packet, String txid, String rest) throws Exception {}
    private void handleExistResponse(String txid, String rest) {}
    private void handleReadRequest(DatagramPacket packet, String txid, String rest) throws Exception {}
    private void handleReadResponse(String txid, String rest) {}
    private void handleWriteRequest(DatagramPacket packet, String txid, String rest) throws Exception {}
    private void handleWriteResponse(String txid, String rest) {}
    private void handleCASRequest(DatagramPacket packet, String txid, String rest) throws Exception {}
    private void handleCASResponse(String txid, String rest) {}
    private void handleRelay(DatagramPacket packet, String txid, String rest) throws Exception {}

    private String encodeString(String s) { //helper method for strict formatting handling
        if (s == null) s = "";
        int spaceCount = 0;
        for (char c : s.toCharArray()) {
            if (c == ' ') spaceCount++;
        }
        return spaceCount + " " + s + " ";
    }

    //temporary, since ill need to implement working with the raw string directly rather than splitting it
    private String[] decodeString(String[] parts, int pos) { //parses a crn-encoded string starting at position 'pos'
                                                             //returns in the format {decoded string, next index}
        int numSpaces = Integer.parseInt(parts[pos]);
        pos++;
        StringBuilder sb = new StringBuilder();
        int spacesFound = 0;
        while (spacesFound < numSpaces || (sb.length() == 0 && numSpaces == 0)) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(parts[pos]);
            spacesFound += (parts[pos].equals("") ? 0 : 0); //count spaces in token
            pos++;
            if (spacesFound >= numSpaces) break;
        }
        return new String[]{sb.toString(), String.valueOf(pos)};
    }
    
    public boolean isActive(String nodeName) throws Exception {
	throw new Exception("Not implemented");
    }
    
    public void pushRelay(String nodeName) throws Exception {
	throw new Exception("Not implemented");
    }

    public void popRelay() throws Exception {
        throw new Exception("Not implemented");
    }

    public boolean exists(String key) throws Exception {
	throw new Exception("Not implemented");
    }
    
    public String read(String key) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean write(String key, String value) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
	throw new Exception("Not implemented");
    }
}
