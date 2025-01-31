import java.io.*;
import java.net.*;
import java.util.*;

public class Client {
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private boolean isConnected;
    private boolean isAuthenticated;
    private Scanner scanner;

    // Protocolo de comunicação
    private static final byte CMD_PUT = 1;
    private static final byte CMD_GET = 2;
    private static final byte CMD_EXIT = 3;
    private static final byte CMD_LOGIN = 4;
    private static final byte CMD_REGISTER = 5;
    private static final byte CMD_MULTIPUT = 6;
    private static final byte CMD_MULTIGET = 7;
    private static final byte CMD_GETWHEN = 8;

    public Client(String host, int port) throws IOException {
        connect(host, port);
        scanner = new Scanner(System.in);
        isAuthenticated = false;
    }

    private void connect(String host, int port) throws IOException {
        socket = new Socket(host, port);
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        isConnected = true;
    }

    public void start() {
        try {
            while (!isAuthenticated) {
                showAuthMenu();
            }

            showMainMenu();

        } catch (IOException e) {
            System.err.println("Erro: " + e.getMessage());
        } finally {
            disconnect();
        }
    }

    private void showAuthMenu() throws IOException {
        System.out.println("\n=== Menu de Autenticação ===");
        System.out.println("1. Registrar");
        System.out.println("2. Login");
        System.out.println("3. Sair");
        System.out.print("Escolha uma opção: ");

        String choice = scanner.nextLine();
        switch (choice) {
            case "1":
                handleRegisterMenu();
                break;
            case "2":
                handleLoginMenu();
                break;
            case "3":
                disconnect();
                System.exit(0);
            default:
                System.out.println("Opção inválida!");
        }
    }

    private void handleRegisterMenu() throws IOException {
        System.out.println("\n=== Registro de Novo Usuário ===");
        System.out.print("Username: ");
        String username = scanner.nextLine();
        System.out.print("Password: ");
        String password = scanner.nextLine();

        if (register(username, password)) {
            System.out.println("Registro realizado com sucesso!");
            System.out.println("Por favor, faça login.");
        } else {
            System.out.println("Erro no registro. Username pode já estar em uso.");
        }
    }

    private void handleLoginMenu() throws IOException {
        System.out.println("\n=== Login ===");
        System.out.print("Username: ");
        String username = scanner.nextLine();
        System.out.print("Password: ");
        String password = scanner.nextLine();

        if (login(username, password)) {
            System.out.println("Login realizado com sucesso!");
            isAuthenticated = true;
        } else {
            System.out.println("Login falhou. Verifique suas credenciais.");
        }
    }

    private void showMainMenu() throws IOException {
        while (isAuthenticated) {
            System.out.println("\n=== Menu Principal ===");
            System.out.println("1. PUT");
            System.out.println("2. GET");
            System.out.println("3. MULTI PUT");
            System.out.println("4. MULTI GET");
            System.out.println("5. GET WHEN");
            System.out.println("6. Sair");
            System.out.print("Escolha uma opção: ");

            String choice = scanner.nextLine();
            switch (choice) {
                case "1":
                    handlePut();
                    break;
                case "2":
                    handleGet();
                    break;
                case "3":
                    handleMultiPut();
                    break;
                case "4":
                    handleMultiGet();
                    break;
                case "5":
                    handleGetWhen();
                    break;
                case "6":
                    logout();
                    break;
                default:
                    System.out.println("Opção inválida!");
            }
        }
    }

    private void handlePut() throws IOException {
        System.out.print("Digite a chave: ");
        String key = scanner.nextLine();
        System.out.print("Digite o valor (em bytes): ");
        String value = scanner.nextLine();
        byte[] valueBytes = value.getBytes();
        put(key, valueBytes);
        System.out.println("Chave e valor armazenados com sucesso!");
    }

    private void handleGet() throws IOException {
        System.out.print("Digite a chave para buscar: ");
        String key = scanner.nextLine();
        byte[] value = get(key);
        if (value != null) {
            System.out.println("Valor encontrado: " + new String(value));
        } else {
            System.out.println("Chave não encontrada.");
        }
    }

    private void handleMultiPut() throws IOException {
        Map<String, byte[]> pairs = new HashMap<>();
        System.out.print("Quantas chaves e valores você deseja inserir? ");
        int n = Integer.parseInt(scanner.nextLine());

        for (int i = 0; i < n; i++) {
            System.out.print("Digite a chave: ");
            String key = scanner.nextLine();
            System.out.print("Digite o valor (em bytes): ");
            String value = scanner.nextLine();
            pairs.put(key, value.getBytes());
        }

        multiPut(pairs);
        System.out.println("Múltiplos pares chave-valor armazenados com sucesso!");
    }

    private void handleMultiGet() throws IOException {
        Set<String> keys = new HashSet<>();
        System.out.print("Quantas chaves você deseja buscar? ");
        int n = Integer.parseInt(scanner.nextLine());

        for (int i = 0; i < n; i++) {
            System.out.print("Digite a chave: ");
            String key = scanner.nextLine();
            keys.add(key);
        }

        Map<String, byte[]> values = multiGet(keys);
        if (values.isEmpty()) {
            System.out.println("Nenhuma chave encontrada.");
        } else {
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                System.out.println("Chave: " + entry.getKey() + ", Valor: " + new String(entry.getValue()));
            }
        }
    }

    private void handleGetWhen() throws IOException {
        System.out.print("Digite a chave a ser buscada: ");
        String key = scanner.nextLine();
        System.out.print("Digite a chave condicional: ");
        String keyCond = scanner.nextLine();
        System.out.print("Digite o valor condicional (em bytes): ");
        String valueCondStr = scanner.nextLine();
        byte[] valueCond = valueCondStr.getBytes();

        byte[] result = getWhen(key, keyCond, valueCond);
        if (result != null) {
            System.out.println("Valor encontrado: " + new String(result));
        } else {
            System.out.println("Condição não satisfeita ou valor não encontrado.");
        }
    }


    private boolean register(String username, String password) throws IOException {
        if (!isConnected) throw new IOException("Not connected to server");

        out.writeByte(CMD_REGISTER);
        out.writeUTF(username);
        out.writeUTF(password);
        return in.readBoolean();
    }

    private boolean login(String username, String password) throws IOException {
        if (!isConnected) throw new IOException("Not connected to server");

        out.writeByte(CMD_LOGIN);
        out.writeUTF(username);
        out.writeUTF(password);
        return in.readBoolean();
    }

    private void put(String key, byte[] value) throws IOException {
        if (!isConnected || !isAuthenticated)
            throw new IOException("Not connected or not authenticated");

        out.writeByte(CMD_PUT);
        out.writeUTF(key);
        out.writeInt(value.length);
        out.write(value);
        boolean success = in.readBoolean();
        if (!success) {
            throw new IOException("Failed to put value");
        }
    }

    private byte[] get(String key) throws IOException {
        if (!isConnected || !isAuthenticated)
            throw new IOException("Not connected or not authenticated");

        out.writeByte(CMD_GET);
        out.writeUTF(key);

        if (in.readBoolean()) {
            int length = in.readInt();
            byte[] value = new byte[length];
            in.readFully(value);
            return value;
        }
        return null;
    }

    private void multiPut(Map<String, byte[]> pairs) throws IOException {
        if (!isConnected || !isAuthenticated)
            throw new IOException("Not connected or not authenticated");

        out.writeByte(CMD_MULTIPUT);
        out.writeInt(pairs.size());

        for (Map.Entry<String, byte[]> entry : pairs.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        boolean success = in.readBoolean();
        if (!success) {
            throw new IOException("Failed to perform multi-put operation");
        }
    }

    private Map<String, byte[]> multiGet(Set<String> keys) throws IOException {
        if (!isConnected || !isAuthenticated)
            throw new IOException("Not connected or not authenticated");

        out.writeByte(CMD_MULTIGET);
        out.writeInt(keys.size());

        for (String key : keys) {
            out.writeUTF(key);
        }

        Map<String, byte[]> result = new HashMap<>();
        int numFound = in.readInt();
        for (int i = 0; i < numFound; i++) {
            String key = in.readUTF();
            int length = in.readInt();
            byte[] value = new byte[length];
            in.readFully(value);
            result.put(key, value);
        }
        return result;
    }

    private byte[] getWhen(String key, String keyCond, byte[] valueCond) throws IOException {
        if (!isConnected || !isAuthenticated)
            throw new IOException("Not connected or not authenticated");

        out.writeByte(CMD_GETWHEN);
        out.writeUTF(key);
        out.writeUTF(keyCond);
        out.writeInt(valueCond.length);
        out.write(valueCond);

        // Recebe a resposta do servidor
        boolean success = in.readBoolean();
        if (success) {
            int resultLength = in.readInt();
            byte[] result = new byte[resultLength];
            in.readFully(result);
            return result;
        } else {
            return null;
        }
    }


    private void logout() throws IOException {
        out.writeByte(CMD_EXIT);
        disconnect();
        System.exit(0);
    }

    public void disconnect() {
        try {
            if (isConnected) {
                out.writeByte(CMD_EXIT);
                isConnected = false;
            }
            if (scanner != null) scanner.close();
            if (in != null) in.close();
            if (out != null) out.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error during disconnect: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        try {
            Client client = new Client("localhost", 8080);
            client.start();
        } catch (IOException e) {
            System.err.println("Erro ao iniciar cliente: " + e.getMessage());
        }
    }
}