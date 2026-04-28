// IN2011 Computer Networks
// Coursework 2024/2025
//
// Construct the hashID for a string

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashID {

	public static byte[] getHash(String s) throws Exception {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(s.getBytes(StandardCharsets.UTF_8));
		return md.digest();
	}


	//calculates distance between two hashes. results are between 0 and 256
	public static int getDistance(byte[] a, byte[] b) {
		if (a == null || b == null || a.length != 32 || b.length != 32) {
			return 0;
		}

		int totalBits = 0;
		for (int i = 0; i < a.length; i++) {
			if (a[i] == b[i]) {
				totalBits += 8; //if bytes are identical, add 8 bits and move to the next byte
			} else { //if bytes differ, find the first differing bit
				int diff = (a[i] ^ b[i]) & 0xFF;

				for (int bit = 7; bit >= 0; bit--) { //check each bit from MSB to LSB
					if (((diff >> bit) & 1) == 0) {
						totalBits++;
					} else {
						return totalBits;
					}
				}
				return totalBits;
			}
		}
		return totalBits;
	}

	//converting an array to a lowercase hex string for the readme.txt file
	public static String bytesToHex(byte[] bytes) {
		StringBuilder sb = new StringBuilder();
		for (byte b : bytes) {
			sb.append(String.format("%02x", b));
		}
		return sb.toString();
	}
}