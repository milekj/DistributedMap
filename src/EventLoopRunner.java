import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

public class EventLoopRunner {
    private final static String DEFAULT_LAST_IP_OCTET = "66";
    private String lastIpOctet;
    private Scanner scanner;

    public EventLoopRunner(String lastIpOctet) {
        this.lastIpOctet = lastIpOctet;
        scanner = new Scanner(System.in);
    }

    public EventLoopRunner() {
        this(DEFAULT_LAST_IP_OCTET);
    }

    public void run() {
        try(DistributedMap map = new DistributedMap(lastIpOctet)) {
            while (true) {
                try {
                    makeRequestsFromInput(map);
                } catch (Exception e) {
                    System.out.println("Invalid input");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void makeRequestsFromInput(DistributedMap map) {
        String command = scanner
                .next()
                .toUpperCase();
        switch (command) {
            case "PUT" :  handlePut(map); break;
            case "GET" :  handleGet(map); break;
            case "CONTAINS" : handleContains(map); break;
            case "REMOVE" : handleRemove(map); break;
            case "ENTRIES" : handleEntries(map); break;
            default : System.out.println("Unsupported operation"); break;
        }
    }

    private void handlePut(DistributedMap map) {
        String key = scanner.next();
        int value = scanner.nextInt();
        map.put(key, value);
        System.out.format("put %s : %s\n", key, value);
    }

    private void handleGet(DistributedMap map) {
        String key = scanner.next();
        int value = map.get(key);
        System.out.format("%s : %d\n", key, value);
    }

    private void handleContains(DistributedMap map) {
        String key = scanner.next();
        System.out.format("contains %s ? : %b\n", key, map.containsKey(key));
    }

    private void handleRemove(DistributedMap map) {
        String key = scanner.next();
        int value = map.remove(key);
        System.out.format("removed %s : %d\n", key, value);
    }

    private void handleEntries(DistributedMap map) {
        Set<Map.Entry<String, Integer>> entries = map.entrySet();
        String result = entries
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining("\n"));
        System.out.println(result);
    }
}
