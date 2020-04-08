import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.simple.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeoutException;

public class Publisher {
    private static final String exchangeName = "topic_logs";
    //Opens the channel and sends the message
    public void send(String routingKey, JSONObject message) throws IOException,TimeoutException{

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(Connection connection = factory.newConnection()){
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(exchangeName, "topic");

            byte[] data = toStream(message);

            channel.basicPublish(exchangeName, routingKey,null,data);
            System.out.println("[x] sent: "+  routingKey);

       }
    }

    //Converts the msg object into Byte[] and returns it
    private static byte[] toStream(JSONObject message)throws IOException{

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;

        try{

            out = new ObjectOutputStream(bos);
            out.writeObject(message);
            out.flush();

            return bos.toByteArray();

        }finally {
            try{
                bos.close();
            }catch(IOException e){
                e.printStackTrace();

            }

        }

    }
}
