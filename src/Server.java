import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;

public class Server {
    private static final int PORT = 8080;
    private final int maxSessions;
    private final Map<String, byte[]> storage;
    private final Map<String, String> users;
    private int currentSessions;

    private final ReadWriteLock storageLock;
    private final Lock sessionLock;
    private final Condition sessionAvailable;
    private final Map<String, Condition> keyConditions;

    public Server(int maxSessions) {
        this.maxSessions = maxSessions;
        this.storage = new HashMap<>();
        this.users = new HashMap<>();
        this.currentSessions = 0;

        this.storageLock = new ReentrantReadWriteLock();
        this.sessionLock = new ReentrantLock();
        this.sessionAvailable = sessionLock.newCondition();
        this.keyConditions = new HashMap<>();
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                ClientHandler handler = new ClientHandler(clientSocket, this);
                new Thread(handler).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean tryAcquireSession() {
        sessionLock.lock();
        try {
            while (currentSessions >= maxSessions) {
                try {
                    sessionAvailable.await();
                } catch (InterruptedException e) {
                    return false;
                }
            }
            currentSessions++;
            return true;
        } finally {
            sessionLock.unlock();
        }
    }

    public void releaseSession() {
        sessionLock.lock();
        try {
            if (currentSessions > 0) {
                currentSessions--;
                sessionAvailable.signal();
            }
        } finally {
            sessionLock.unlock();
        }
    }


    public byte[] get(String key) {
        System.out.println("Iniciando GET para chave: " + key);
        storageLock.readLock().lock();
        try {
            byte[] value = storage.get(key);
            if (value != null) {
                System.out.println("Chave " + key + " encontrada, valor: " + new String(value));
            } else {
                System.out.println("Chave " + key + " não encontrada.");
            }
            return value;
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public void put(String key, byte[] value) {
        System.out.println("Iniciando PUT para chave: " + key);
        storageLock.writeLock().lock();  // Bloqueio de escrita
        try {
            storage.put(key, value);
            System.out.println("Chave " + key + " inserida com sucesso.");
            Condition condition = keyConditions.get(key);
            if (condition != null) {
                condition.signalAll();
            }
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public void multiPut(Map<String, byte[]> pairs) {
        System.out.println("Iniciando MULTIPUT...");
        storageLock.writeLock().lock();
        try {
            for (Map.Entry<String, byte[]> entry : pairs.entrySet()) {
                System.out.println("Inserindo chave: " + entry.getKey());
                storage.put(entry.getKey(), entry.getValue());
            }
            System.out.println("Todas as chaves inseridas com sucesso.");
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public Map<String, byte[]> multiGet(Set<String> keys) {
        Map<String, byte[]> result = new HashMap<>();
        System.out.println("Iniciando MULTIGET para " + keys.size() + " chaves...");
        for (String key : keys) {
            storageLock.readLock().lock();
            try {
                byte[] value = storage.get(key);
                if (value != null) {
                    result.put(key, value);
                    System.out.println("Chave " + key + " encontrada, valor: " + new String(value));
                } else {
                    System.out.println("Chave " + key + " não encontrada.");
                }
            } finally {
                storageLock.readLock().unlock();
            }
        }
        System.out.println("MULTIGET finalizado.");
        return result;
    }

    public byte[] getWhen(String key, String keyCond, byte[] valueCond) {
        System.out.println("Iniciando GETWHEN para chave: " + key + " com condição para chave: " + keyCond);
        storageLock.readLock().lock();
        try {
            keyConditions.putIfAbsent(keyCond, storageLock.writeLock().newCondition());
            Condition condition = keyConditions.get(keyCond);

            while (true) {
                byte[] currentValue = storage.get(keyCond);
                if (currentValue != null && Arrays.equals(currentValue, valueCond)) {
                    byte[] resultValue = storage.get(key);
                    System.out.println("Condição satisfeita. Valor para chave " + key + ": " + new String(resultValue));
                    return resultValue;
                }
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Thread interrompida enquanto aguardava a condição", e);
                }
            }
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public boolean registerUser(String username, String password) {
        storageLock.writeLock().lock();
        try {
            if (!users.containsKey(username)) {
                users.put(username, password);
                System.out.println("Usuário " + username + " registrado com sucesso.");
                return true;
            }
            System.out.println("Usuário " + username + " já existe.");
            return false;
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public boolean authenticateUser(String username, String password) {
        storageLock.readLock().lock();
        try {
            boolean isAuthenticated = password.equals(users.get(username));
            if (isAuthenticated) {
                System.out.println("Usuário " + username + " autenticado com sucesso.");
            } else {
                System.out.println("Falha na autenticação de " + username);
            }
            return isAuthenticated;
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Por favor, forneça o número máximo de sessões.");
            return;
        }

        int maxSessions;
        try {
            maxSessions = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("O número máximo de sessões deve ser um número inteiro válido.");
            return;
        }
        Server server = new Server(maxSessions);
        server.start();
    }
}
