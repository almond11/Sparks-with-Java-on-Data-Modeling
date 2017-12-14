import java.io.*;
import java.util.*;

public class valueParis {

    String serverName;
    String clientName;
    int partition;
    String topicName;

    public valueParis(String topicName, int partition, String clientName, String serverName) {
        this.topicName = topicName;
        this.partition = partition;
        this.clientName = clientName;
        this.serverName = serverName;

    }
}
