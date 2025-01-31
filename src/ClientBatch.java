import java.io.*;
import java.net.*;
import java.util.*;

public class ClientBatch {
    private static final int PORT = 8080;
    private static final String HOST = "localhost";

    private static final byte CMD_PUT = 1;
    private static final byte CMD_GET = 2;
    private static final byte CMD_EXIT = 3;
    private static final byte CMD_LOGIN = 4;
    private static final byte CMD_REGISTER = 5;
    private static final byte CMD_MULTIPUT = 6;
    private static final byte CMD_MULTIGET = 7;
    private static final byte CMD_GETWHEN = 8;

    // Número máximo de iterações para evitar loop infinito
    private static final int MAX_ITERATIONS = 99999999;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Modo de execução e operação não fornecidos.");
            System.out.println("Uso: java ClientBatch <modo> <operação>");
            return;
        }

        String mode = args[0];
        String operation = args[1];

        if ("batch".equalsIgnoreCase(mode)) {
            executeBatchMode(operation);
        } else {
            System.out.println("Modo desconhecido: " + mode);
        }
    }

    private static void executeBatchMode(String operation) {
        List<String> keys = Arrays.asList("key1", "key2", "key3", "key4", "key5");
        List<byte[]> values = Arrays.asList(
                "value1".getBytes(),
                "value2".getBytes(),
                "value3".getBytes(),
                "value4".getBytes(),
                "value5".getBytes()
        );

        Random random = new Random();
        int iteration = 0;

        try (Socket socket = new Socket(HOST, PORT);
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {

            while (iteration < MAX_ITERATIONS) {
                try {
                    switch (operation.toLowerCase()) {
                        case "put":
                            performPutOperation(out, in, keys, values, random);
                            break;

                        case "get":
                            performGetOperation(out, in, keys, random);
                            break;

                        case "multiget":
                            performMultiGetOperation(out, in, keys, random);
                            break;

                        case "multiput":
                            performMultiPutOperation(out, in, keys, values, random);
                            break;

                        default:
                            System.out.println("Operação desconhecida: " + operation);
                            return;
                    }

                    iteration++;

                } catch (IOException e) {
                    System.err.println("Erro de E/S durante a operação: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // Envia comando de saída para o servidor, se aplicável
            sendExitCommand(out, in);

        } catch (IOException e) {
            System.err.println("Erro ao conectar ao servidor: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void performPutOperation(DataOutputStream out, DataInputStream in,
                                            List<String> keys, List<byte[]> values, Random random) throws IOException {
        String randomKeyPut = keys.get(random.nextInt(keys.size()));
        byte[] randomValue = values.get(random.nextInt(values.size()));
        out.writeByte(CMD_PUT);
        out.writeUTF(randomKeyPut);
        out.writeInt(randomValue.length);
        out.write(randomValue);
        out.flush(); // Assegura que os dados sejam enviados imediatamente

        boolean putSuccess = in.readBoolean();
        System.out.println("Put key: " + randomKeyPut + " value: " + new String(randomValue) + ", success: " + putSuccess);
    }

    private static void performGetOperation(DataOutputStream out, DataInputStream in,
                                            List<String> keys, Random random) throws IOException {
        String randomKeyGet = keys.get(random.nextInt(keys.size()));
        out.writeByte(CMD_GET);
        out.writeUTF(randomKeyGet);
        out.flush();

        boolean getSuccess = in.readBoolean();
        if (getSuccess) {
            int valueLength = in.readInt();
            byte[] value = new byte[valueLength];
            in.readFully(value);
            System.out.println("Get key: " + randomKeyGet + " value: " + new String(value));
        } else {
            System.out.println("Key " + randomKeyGet + " not found.");
        }
    }

    private static void performMultiGetOperation(DataOutputStream out, DataInputStream in,
                                                 List<String> keys, Random random) throws IOException {
        out.writeByte(CMD_MULTIGET);
        int numKeys = random.nextInt(keys.size()) + 1;
        out.writeInt(numKeys);

        Set<String> randomKeys = new HashSet<>();
        while (randomKeys.size() < numKeys) {
            String key = keys.get(random.nextInt(keys.size()));
            if (randomKeys.add(key)) { // Adiciona apenas se for único
                out.writeUTF(key);
            }
        }
        out.flush();

        try {
            int responseSize = in.readInt();
            if (responseSize > 0) {
                System.out.println("MultiGet Result: ");
                for (int i = 0; i < responseSize; i++) {
                    String key = in.readUTF();
                    int valueLength = in.readInt();
                    byte[] value = new byte[valueLength];
                    in.readFully(value);
                    System.out.println("Key: " + key + ", Value: " + new String(value));
                }
            } else {
                System.out.println("Nenhuma chave encontrada.");
            }
        } catch (IOException e) {
            System.err.println("Erro ao processar resposta do MultiGet: " + e.getMessage());
        }
    }

    private static void performMultiPutOperation(DataOutputStream out, DataInputStream in,
                                                 List<String> keys, List<byte[]> values, Random random) throws IOException {
        out.writeByte(CMD_MULTIPUT);
        int numPairs = random.nextInt(keys.size()) + 1;
        out.writeInt(numPairs);

        Map<String, byte[]> pairs = new HashMap<>();
        for (int i = 0; i < numPairs; i++) {
            String key = keys.get(random.nextInt(keys.size()));
            byte[] value = values.get(random.nextInt(values.size()));
            pairs.put(key, value);
            out.writeUTF(key);
            out.writeInt(value.length);
            out.write(value);
        }
        out.flush();

        try {
            boolean success = in.readBoolean();
            if (success) {
                System.out.println("MultiPut operation successful for " + numPairs + " pairs.");
            } else {
                System.out.println("MultiPut operation failed.");
            }
        } catch (IOException e) {
            System.err.println("Erro ao processar resposta do MultiPut: " + e.getMessage());
        }
    }

    private static void sendExitCommand(DataOutputStream out, DataInputStream in) {
        try {
            out.writeByte(CMD_EXIT);
            out.flush();
            System.out.println("Comando de saída enviado para o servidor.");
        } catch (IOException e) {
            System.err.println("Erro ao enviar comando de saída: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
