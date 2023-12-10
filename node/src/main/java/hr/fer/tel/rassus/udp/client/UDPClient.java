package hr.fer.tel.rassus.udp.client;

import hr.fer.tel.rassus.udp.network.*;
import java.io.IOException;
import java.net.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UDPClient {
    public UDPClient() {

    }

    public String sendMessage(String message, int serverPort) throws IOException {
        byte[] rcvBuf = new byte[256]; // received bytes

        // encode this String into a sequence of bytes using the platform's
        // default charset and store it into a new byte array

        // determine the IP address of a host, given the host's name
        InetAddress address = InetAddress.getByName("localhost");

        // create a datagram socket and bind it to any available
        // port on the local host
        //DatagramSocket socket = new SimulatedDatagramSocket(0.2, 1, 200, 50); //SOCKET
        DatagramSocket socket = new SimpleSimulatedDatagramSocket(0.2, 200); //SOCKET

        System.out.print("Client sends: ");
        // send each character as a separate datagram packet
        for (int i = 0; i < message.length(); i++) {
            byte[] sendBuf = new byte[1];// sent bytes
            sendBuf[0] = (byte) message.charAt(i);

            // create a datagram packet for sending data
            DatagramPacket packet = new DatagramPacket(sendBuf, sendBuf.length,
                    address, serverPort);

            // send a datagram packet from this socket
            socket.send(packet); //SENDTO
            System.out.print(new String(sendBuf));
        }
        System.out.println("");

        StringBuffer receiveString = new StringBuffer();

        while (true) {
            // create a datagram packet for receiving data
            DatagramPacket rcvPacket = new DatagramPacket(rcvBuf, rcvBuf.length);

            try {
                // receive a datagram packet from this socket
                socket.receive(rcvPacket); //RECVFROM
            } catch (SocketTimeoutException e) {
                break;
            } catch (IOException ex) {
                Logger.getLogger(UDPClient.class.getName()).log(Level.SEVERE, null, ex);
            }

            // construct a new String by decoding the specified subarray of bytes
            // using the platform's default charset
            receiveString.append(new String(rcvPacket.getData(), rcvPacket.getOffset(), rcvPacket.getLength()));

        }
        System.out.println("Client received: " + receiveString);

        // close the datagram socket
        socket.close(); //CLOSE

        return receiveString.toString();
    }
}
