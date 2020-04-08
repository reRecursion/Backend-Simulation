import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class JSonFileReader {

    //Reads JSon file and sends the object through publisher
    public void read(String fileName,String key, Publisher pub) {
        JSONParser parser = new JSONParser();

        try {
            Object obj = parser.parse(new FileReader(fileName));
            JSONArray jsonArray = (JSONArray) obj;
            Iterator iterator = jsonArray.iterator();

            while(iterator.hasNext()){
                //Creates the JSONObject and calls publisher to send it.
                JSONObject mTL = (JSONObject) iterator.next();
                pub.send(key,mTL);
            }

        }
        catch(FileNotFoundException e){ e.printStackTrace();}
        catch(IOException e){ e.printStackTrace();}
        catch(Exception e){ e.printStackTrace();}

    }


}
