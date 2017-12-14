import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.security.*;

public class client {

    private class ServerStruct{
        int port;
        String name;
        String ip;
        public ServerStruct(String name, String ip, int port){
            this.name = name;
            this.port = port;
            this.ip = ip;
        }
    }

    int serverSize;
    int clientSize;
    Hashtable<String, ServerStruct> servers;
    Hashtable<String, BufferedReader> readers;
    Hashtable<String, PrintWriter> writers;



    //Library Variables
    private AtomicBoolean wait;
    private int gettedValue;
    private String clientName;
    private Thread t;

    public client(String name) {
        //Initialize Hashtable
        servers = new Hashtable<String, ServerStruct>();
        readers = new Hashtable<String, BufferedReader>();
        writers = new Hashtable<String, PrintWriter>();

        clientName = name;
        clientSize = 1;
        serverSize = 0;
        wait = new AtomicBoolean(true);
    }

    public void add(String servername,String serverip, int serverport){
        serverSize++;
        ServerStruct s = new ServerStruct(servername, serverip, serverport);
        if(!servers.containsKey(s.name) && !servers.contains(s)){
            servers.put(s.name, s);
        }
        try {
            run(s.name, s.ip, s.port);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }





    public static void publish(String topic, int partition,String key,int value){
        int serverIndex =  hashingIndex(topic+partition);
        master.addOne(concat(topic,partition),concate(key,value));
        TopicPartiServer.put(concat(topic,partition),Server);
        for (int i=0;i<partition;i++){
            System.out.println("publish: " + topic   + " into partition " + "[" +i+"] on server "+
                    servers.get(partition).servername);
            writers.get(serverIndex).println("publish   : " +topic + " into partition " + "[" +i+"] on server "+
                    servers.get(partition).servername);
        }
    }

    public static  void publish(String topic,String key,int value){
        publish(topic,1,  key,  value);
    }

 /*   public int getKey(String key){
        int serverIndex = generateIndex(key);
        writers.get(serverIndex).println("getkey " + clientName + " " + key + " " + serverIndex);
        while(wait.get()){

        }
        wait.set(true);
        return gettedValue;
    }

    public  void putKey(String key, Object val){
        int serverIndex = generateIndex(key);
        System.err.println("Put: " + key + " : " + val.toString() + " into " + "[" + servers.get(serverIndex).serverAddress
                + ":" + servers.get(serverIndex).port + "]");
        writers.get(serverIndex).println("putkey " + key.toString() + " " + val.toString() + " " + serverIndex);
    }
   */

    public  int generateIndex(String key){
        int hash = 7;
        for (int i = 0; i < key.length(); i++) {
            hash = hash*31 + key.charAt(i);
        }
        int index = hash % serverSize;
        return index+1;
    }

    /**
     * Connects to the server then enters the processing loop.
     */
    public void run(final String servername, String serverip, int serverport) throws IOException {
        // Make connection and initialize streams
        Socket socket = new Socket(serverip, serverport);

        BufferedReader in;
        PrintWriter out;

        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);

        readers.put(servername, in);
        writers.put(servername, out);

        System.out.println("Please keep entering commands starting with funtion names, following with ENTER key, for example:");
        System.out.println("add|delete <host name>, <ip address>, <port number>");
        System.out.println("create <topicName> [<partitionsNumber>] ### default <partitionsNumber>=1");
        System.out.println("remove  <topicName>");
        System.out.println("subscribe/unsubscribe <topicName>");
        System.out.println("publish <topicName> [<partitionsNumber>]");
        System.out.println("get <topicName> [<partitionsNumber>] ");
        System.out.println("Important: If to connect two servers, has to add twice, same to all the other commands.[] means optional. ");

        t = new Thread(new Runnable() {
            public void run() {
                // Process all messages from server, according to the protocol.
                while (true) {
                    String line = null;
                    try {
                        line = readers.get(servername).readLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (line.startsWith("create") ||line.startsWith("remove")||line.startsWith("subscribe")||line.startsWith("unsubscribe")||line.startsWith("get")) {
                        writers.get(servername).println(line);
                    }
                    else if (line.startsWith("delete")) {

                    }
                    else if (line.startsWith("add")) {

                        else{
                            System.err.println("Undefined command..." + "\n");
                        }
                        //System.err.println(line);
                    }
                }
            }
        }
        );

        t.start();
    }

    public static void main(String[] args) throws Exception {
        String name = "**";
        if(args.length > 0){
            name = args[0];}

        client c = new client(name);
        Scanner scanner = new Scanner(System.in);
  /*      System.out.println("Please keep entering commands starting with funtion names, following with ENTER key, for example:");
        System.out.println("add|delete <host name>, <ip address>, <port number>");
        System.out.println("create <topicName> [<partitionsNumber>] ### default <partitionsNumber>=1");
        System.out.println("remove  <topicName>");
        System.out.println("subscribe/unsubscribe <topicName>");
        System.out.println("publish <topicName> [<partitionsNumber>]");
        System.out.println("get <topicName> [<partitionsNumber>] ");
        System.out.println("Important: If to connect two servers, has to add twice, same to all the other commands.[] means optional. ");
*/
        String line ;
        while(!(line = scanner.nextLine()).isEmpty()){
            String[] command = line.split(" ");

            if (command[0].equals("add")){
                c.add(command[1],command[2],Integer.parseInt(command[3]));

            }
            else if (command[0].equals("delete")){
                c.delete(command[1],command[2],Integer.parseInt(command[3]));
            }
  /          else if (command[0].equals("remove")){
                c.remove(command[1]);
            }
            else if (command[0].equals("create")){
                if (command.length<3) {
                    c.create(command[1]);
                }
                if (command.length>=3){
                    c.create(command[1],Integer.parseInt(command[2]));
                }
            }
            else if (command[0].equals("subscribe")){
                c.subscribe(command[1]);

            }
            else if (command[0].equals("unsubscribe")){
                c.unsubscribe(command[1]);

            }
            else if (command[0].equals("publish")){
                if (command.length<3) {
                    c.publish(command[1]);
                }
                if (command.length>=3){
                    c.publish(command[1],Integer.parseInt(command[2]));
                }
            }
            else if (command[0].equals("get")){
                if (command.length<3) {
                    c.get(command[1]);
                }
                if (command.length>=3){
                    c.get(command[1],Integer.parseInt(command[2]));
                }

            }*/
            else if (command[0].equals("publish")){
                if (command.length<5) {

                    publish(command[1],1,command[3],Integer.parseInt(command[4]));
                }
                if (command.length>=5){
                    publish(command[1],Integer.parseInt(command[2]),command[3],Integer.parseInt(command[4]));
                }
            }


        }
    }

    public static String concat(String topicname,int parti){
        String newtopic=topicname+"/*/*/"+parti;
        return newtopic;
    }
    public static String getfirst(String TopicParti){
        int ind=TopicParti.indexOf("/*/*/");
        return TopicParti.substring(0,ind);
    }
    public static int getlast(String TopicParti){
        int ind=TopicParti.indexOf("/*/*/");
        return Integer.parseInt(TopicParti.substring(ind+5));
    }

}