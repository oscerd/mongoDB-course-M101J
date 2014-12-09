package com.oscerd.github.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.oscerd.github.mongodb.config.MongoConfig;
import com.oscerd.github.mongodb.config.MongoPropertyReader;

public class Application {
	public static void main(String[] args) throws IOException {
        MongoClient mongoClient;
        MongoPropertyReader reader = new MongoPropertyReader("config.properties");
        Properties properties = reader.getProperties();
        org.junit.Assert.assertNotNull(properties);
        MongoConfig config = new MongoConfig(properties.getProperty("mongodb.host"), Integer.parseInt(properties.getProperty("mongodb.port")));
        mongoClient = new MongoClient(config.getMongoHost(), config.getMongoPort());
        DB database = mongoClient.getDB("school");

        DBCollection collection = database.getCollection("students");

        BasicDBObject query = new BasicDBObject();
        DBCursor cursor = collection.find(query);
        try {
            while( cursor.hasNext() ) {
                DBObject lowest = null;
                DBObject object = cursor.next();
                System.out.println(" Student " + object) ;
                ArrayList<BasicDBObject> scores = (ArrayList) object.get("scores");
                for (BasicDBObject score : scores) {
					if(score.get("type").toString().equalsIgnoreCase("homework")){
						if (lowest == null) lowest = score;
						Float scoreLocal = Float.parseFloat(score.get("score").toString());
						Float lowestLocal = Float.parseFloat(lowest.get("score").toString());
						System.err.println("scoreLocal : " + scoreLocal + " lowestLocal: " + lowestLocal);
						if (scoreLocal < lowestLocal) lowest = score;
					}
				}
				System.err.println("Min: " + lowest);
				scores.remove(lowest);
				object.put("scores", scores);
				collection.update(new BasicDBObject("_id", object.get("_id")), object, true, false);
            }
        }
        finally {
            cursor.close();
        }
    }
}
