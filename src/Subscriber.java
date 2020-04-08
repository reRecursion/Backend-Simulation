import com.mongodb.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.simple.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.concurrent.TimeoutException;


public class Subscriber {

    //Receives the message from the queue
    private static final String exchangeName = "topic_logs";

    public static void main(String[] args) throws IOException,TimeoutException, ClassNotFoundException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName,"topic");
        String queueName = channel.queueDeclare().getQueue();

        channel.queueBind(queueName, exchangeName, "mtl.*");//receive only msges beginning with mtl
        System.out.println("waiting for message");

        channel.basicConsume(queueName,true, (consumerTag,message)-> {
            JSONObject converted = toMessage(message.getBody());
            //System.out.println(converted.toString());

            String type = sortMsg(converted);
            writeDB(type, converted);

        },consumerTag->{});
    }

    //Converts received bytes back to msg object
    private static JSONObject toMessage(byte[] bytes)throws IOException{

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        JSONObject converted = null;

        try{
            in = new ObjectInputStream(bis);
            converted = (JSONObject) in.readObject();

        }catch(ClassNotFoundException e){
            e.printStackTrace();

        }finally{
            try{
                if(in!= null){
                    in.close();
                }
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        return converted;
    }
// writes the recieved message to a Mongo Database
    public static void writeDB(String type, JSONObject message){

        DBObject obj = new BasicDBObject(type,message);
        MongoClient mongoClient = new MongoClient();
        DB database = mongoClient.getDB("mtl");
        DBCollection collection = database.getCollection("mtl_data");

        collection.insert(obj);
        System.out.println("written: " + type);
    }
//Determine the last word of the mtl file received
    public static String sortMsg(JSONObject obj){
        if(obj.toString().contains("temperature")){
            return "Field: Temperature";
        }if(obj.toString().contains("health")){
            return "Field: Health";
        }if(obj.toString().contains("grades")){
            return "Field: Grade";
        }else{
            return "";
        }
    }

}
