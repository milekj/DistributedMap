import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.javafx.util.Logging;
import org.jgroups.*;
import org.jgroups.Message.Flag;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import proto.MapOperationProto;
import proto.MapOperationProto.MapOperation;
import proto.MapOperationProto.MapOperation.OperationType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedMap implements SimpleStringMap, AutoCloseable {
    private final static String CLUSTER_NAME = "map";
    private String lastIpOctet;
    private JChannel channel;
    private Map<String, Integer> map;

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    public DistributedMap(String lastIpOctet) throws Exception {
        this.lastIpOctet = lastIpOctet;
        initializeChannelWithReceiver();
        setChannelProtocolStack();
        connectChannel();
        createMapIfAbsent();
    }

    private void initializeChannelWithReceiver() {
        channel = new JChannel(false);
        channel.setReceiver(new ReceiverAdapterImpl());
    }

    private void setChannelProtocolStack() throws Exception {
        ProtocolStack stack = newProtocolStack();
        channel.setProtocolStack(stack);
        stack.init();
    }

    private void connectChannel() throws Exception {
        channel.connect(CLUSTER_NAME, null, 0);
    }

    private void createMapIfAbsent() {
        if (map == null)
            map = new ConcurrentHashMap<>();
    }

    @Override
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    @Override
    public Integer get(String key) {
        return map.get(key);
    }

    @Override
    public void put(String key, Integer value) {
        MapOperationProto.MapOperation operation = MapOperationProto.MapOperation.newBuilder()
                .setType(MapOperationProto.MapOperation.OperationType.PUT)
                .setKey(key)
                .setValue(value)
                .build();
        createAndSendMessage(operation);
    }

    @Override
    public Integer remove(String key) {
        Integer oldValue = map.get(key);
        MapOperationProto.MapOperation operation = MapOperationProto.MapOperation.newBuilder()
                .setType(MapOperationProto.MapOperation.OperationType.REMOVE)
                .setKey(key)
                .setValue(oldValue)
                .build();
        createAndSendMessage(operation);
        return oldValue;
    }

    private void createAndSendMessage(MapOperationProto.MapOperation operation) {
        byte[] buffer = operation.toByteArray();
        Message message = new Message(null, null, buffer);
        //message.setFlag(Flag.RSVP);
        try {
            channel.send(message);
        } catch (Exception e) {
            throw new RuntimeException("Could not send message");
        }
    }

    public Set<Map.Entry<String, Integer>> entrySet() {
        return map.entrySet();
    }

    @Override
    public void close() {
        channel.close();
    }

    private ProtocolStack newProtocolStack() throws Exception {
        ProtocolStack stack = new ProtocolStack();
        stack.addProtocol(new UDP()
            .setValue("mcast_group_addr", InetAddress.getByName("230.100.200." + lastIpOctet)))
            .addProtocol(new PING())
            .addProtocol(new MERGE3())
            .addProtocol(new FD_SOCK())
            .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
            .addProtocol(new VERIFY_SUSPECT())
            .addProtocol(new BARRIER())
            .addProtocol(new NAKACK2())
            .addProtocol(new UNICAST3())
            .addProtocol(new STABLE())
            .addProtocol(new GMS())
            .addProtocol(new UFC())
            .addProtocol(new MFC())
            .addProtocol(new FRAG2())
            .addProtocol(new SEQUENCER())
            .addProtocol(new FLUSH())
            .addProtocol(new STATE_TRANSFER());
        return stack;
    }

    private class ReceiverAdapterImpl extends ReceiverAdapter {
        @Override
        public void getState(OutputStream output) throws Exception {
            Util.objectToStream(map, new DataOutputStream(output));
        }

        @Override
        public void setState(InputStream input) throws Exception {
            map = (Map<String, Integer>) Util.objectFromStream(new DataInputStream(input));
        }

        @Override
        public void receive(Message msg) {
            try {
                byte[] buffer = msg.getBuffer();
                MapOperation operation = MapOperation.parseFrom(buffer);
                System.out.format("\t\t\t\t\t\t%s %s %d\n", operation.getType(), operation.getKey(), operation.getValue());
                handleMapOperation(operation);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Error parsing incoming message", e);
            }
        }

        private void handleMapOperation(MapOperation operation) {
            String key = operation.getKey();
            Integer value = operation.getValue();
            if (operation.getType() == OperationType.PUT)
                map.put(key, value);
            else {
                map.remove(key);

            }

        }

        @Override
        public void viewAccepted(View view) {
            System.out.println("elo!");
            if (view instanceof MergeView) {
                Runnable merger = () -> {
                    List<View> views = ((MergeView) view).getSubgroups();
                    View firstView = views.get(0);
                    Address myAddress = channel.getAddress();
                    if (!firstView.containsMember(myAddress)) {
                        channel.getState();
                    }
                };
                new Thread(merger).start();
            }
        }
    }
}
