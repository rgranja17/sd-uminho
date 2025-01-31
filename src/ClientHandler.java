import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final Server server;
    private DataInputStream in;
    private DataOutputStream out;

    private static final byte CMD_PUT = 1;
    private static final byte CMD_GET = 2;
    private static final byte CMD_EXIT = 3;
    private static final byte CMD_LOGIN = 4;
    private static final byte CMD_REGISTER = 5;
    private static final byte CMD_MULTIPUT = 6;
    private static final byte CMD_MULTIGET = 7;
    private static final byte CMD_GETWHEN = 8;
    
    public ClientHandler(Socket socket, Server server) {
        this.socket = socket;
        this.server = server;
    }

    @Override
    public void run() {
    try {
        
        if (!server.tryAcquireSession()) {
            out.writeBoolean(false);  
            return;
        }

        setupStreams();
        
        while (true) {
            byte command = in.readByte();
            
            switch (command) {
                case CMD_LOGIN:
                    handleLogin();
                    break;
                case CMD_REGISTER:
                    handleRegister();
                    break;
                case CMD_PUT:
                    handlePut();
                    break;
                case CMD_GET:
                    handleGet();
                    break;
                case CMD_MULTIPUT:
                    handleMultiPut();
                    break;
                case CMD_MULTIGET:
                    handleMultiGet();
                    break;
                case CMD_GETWHEN:
                    handleGetWhen();
                    break;
                case CMD_EXIT:
                    return;
            }
        }
    } catch (IOException e) {
        System.err.println("Error handling client: " + e.getMessage());
    } finally {
        cleanup();
        server.releaseSession();
    }
}


    private void setupStreams() throws IOException {
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
    }

    private void handleLogin() throws IOException {
        String username = in.readUTF();
        String password = in.readUTF();
        boolean success = server.authenticateUser(username, password);
        out.writeBoolean(success);
    }

    private void handleRegister() throws IOException {
        String username = in.readUTF();
        String password = in.readUTF();
        boolean success = server.registerUser(username, password);
        out.writeBoolean(success);
    }

    private void handlePut() throws IOException {
        String key = in.readUTF();
        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.readFully(value);
        
        server.put(key, value);
        out.writeBoolean(true);
    }

    private void handleGet() throws IOException {
        String key = in.readUTF();
        byte[] value = server.get(key);
        
        if (value != null) {
            out.writeBoolean(true);
            out.writeInt(value.length);
            out.write(value);
        } else {
            out.writeBoolean(false);
        }
    }

    private void handleMultiPut() throws IOException {
    try {
        int numberOfPairs = in.readInt();
        Map<String, byte[]> pairs = new HashMap<>();
        

        for (int i = 0; i < numberOfPairs; i++) {
            String key = in.readUTF();
            int valueLength = in.readInt();
            byte[] value = new byte[valueLength];
            in.readFully(value);
            pairs.put(key, value);
        }
        
        server.multiPut(pairs);
        
        out.writeBoolean(true);
    } catch (Exception e) {
        e.printStackTrace();
        out.writeBoolean(false);
        }
    }
    
    private void handleMultiGet() throws IOException {
    try {
        int numKeys = in.readInt();
        Set<String> keys = new HashSet<>();

        for (int i = 0; i < numKeys; i++) {
            keys.add(in.readUTF());
        }

        Map<String, byte[]> result = server.multiGet(keys);

        out.writeInt(result.size());

        for (Map.Entry<String, byte[]> entry : result.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }
    } catch (IOException e) {
        e.printStackTrace();
        out.writeInt(0); 
    }
}

    private void handleGetWhen() throws IOException {
        try {
            String key = in.readUTF();
            String keyCond = in.readUTF();
            int valueCondLength = in.readInt();
            byte[] valueCond = new byte[valueCondLength];
            in.readFully(valueCond);

            byte[] result = server.getWhen(key, keyCond, valueCond);

            if (result != null) {
                out.writeBoolean(true);
                out.writeInt(result.length);
                out.write(result);
            } else {
                out.writeBoolean(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            out.writeBoolean(false); 
        }
    }



    private void cleanup() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }
}