package com.oscerd.github.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.types.ObjectId;

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
        DB database = mongoClient.getDB("students");

        DBCollection collection = database.getCollection("grades");

        BasicDBObject query = new BasicDBObject("type", "homework");
        DBCursor cursor = collection.find(query).sort(new BasicDBObject("student_id", 1).append("score", -1));
        List<Object> map = new ArrayList<Object>();
        Integer test = 0;
        ObjectId prev = new ObjectId();
        try {
            while (cursor.hasNext()) {
                DBObject obj = cursor.next();
                Integer student_id = (Integer) obj.get("student_id");
                if (!test.equals(student_id)) {
                    map.add(prev);
                }
                test = student_id;
                prev = (ObjectId) obj.get("_id");
            }
            map.add(prev);
        }
        finally {
            cursor.close();
        }
        for (int i = 0; i < map.size(); i++) {
            collection.remove(new BasicDBObject("_id", map.get(i)));
        }
    }
}
