package hr.fer.tel.rassus.udp.server;

import hr.fer.tel.rassus.udp.network.SimpleSimulatedDatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class UDPServer {
    private final int id;
    private final String address;
    private final int port; // server port

    public UDPServer(int id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public void receive() throws IOException {
        byte[] rcvBuf = new byte[256]; // received bytes
        byte[] sendBuf = new byte[256];// sent bytes
        String rcvStr;

        // create a UDP socket and bind it to the specified port on the local
        // host
        DatagramSocket socket = new SimpleSimulatedDatagramSocket(this.port, 0.2, 200); //SOCKET -> BIND

        while (true) { //OBRADA ZAHTJEVA
            // create a DatagramPacket for receiving packets
            DatagramPacket packet = new DatagramPacket(rcvBuf, rcvBuf.length);

            // receive packet
            socket.receive(packet); //RECVFROM

            // construct a new String by decoding the specified subarray of
            // bytes
            // using the platform's default charset
            rcvStr = new String(packet.getData(), packet.getOffset(),
                    packet.getLength());
            System.out.println("Server received: " + rcvStr);

            // encode a String into a sequence of bytes using the platform's
            // default charset
            sendBuf = rcvStr.toUpperCase().getBytes();
            System.out.println("Server sends: " + rcvStr.toUpperCase());

            // create a DatagramPacket for sending packets
            DatagramPacket sendPacket = new DatagramPacket(sendBuf,
                    sendBuf.length, packet.getAddress(), packet.getPort());

            // send packet
            socket.send(sendPacket); //SENDTO
        }
    }

    public int getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }
}
