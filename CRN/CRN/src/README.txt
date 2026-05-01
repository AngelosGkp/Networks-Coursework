Build Instructions
==================

1. Extract and make sure that everything is on the same folder
2. Locate the directory by running "cd ...." on the terminal to find the correct path
3. Run "javac *.java" to compile all java files (And ensure that the Node.java compiles into both Node and NodeInterface.java)
4. Run "java LocalTest" to make sure that it compiled properly
5. Then, run " java AzureLabTest [email] [azure-ip]". In my instance, I ran "java AzureLabTest angelos.gkoupidenis@city.ac.uk 10.216.34.76"

Working Functionality
=====================

The node can:
- Respond to incoming name requests with its own name and when receiving a name request, the node send a nearest request back to the sender to learn more peers
- Respond to nearest requests by returning up to three of the closest known nodes
- Explore the networks and discovers new peers
- Handle existence checks and respond
- Handle read requests and respond
- When reading from the network, the node interacts with all known nodes closest to the key and passes again to check if any new nodes were discovered
- Accepts write requests if they hold they key or is a close node
- Checks if the value should be stored before moving on to the three closest known nodes writing
- Storing key pairs when it is one of the 3 closest nodes to the key hash
- Handle CAS requests by swapping if it matches with the expected current value, returning all relevant output
- Forwards embedded messages to the target node and returns a response
- Timeout the search after a certain amount of time