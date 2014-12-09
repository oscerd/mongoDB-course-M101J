package com.oscerd.github.mongodb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.oscerd.github.mongodb.config.MongoConfig;
import com.oscerd.github.mongodb.config.MongoPropertyReader;

public class MongoDBTest {

	@Test
	public void connection1() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);
		db.dropDatabase();
		mongoClient.close();
	}

	@Test
	public void collections() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);

		Set<String> colls = db.getCollectionNames();

		org.junit.Assert.assertEquals(0, colls.size());

		db.dropDatabase();
		mongoClient.close();
	}

	@Test
	public void collectionInsert() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);

		BasicDBObject document = new BasicDBObject("author", "andrea").append(
				"text", "Pippo").append("date", new Date());

		DBCollection coll = db.getCollection("pippo");

		coll.insert(document);

		Set<String> colls = db.getCollectionNames();

		org.junit.Assert.assertEquals(2, colls.size());

		DBCursor cursor = coll.find();
		org.junit.Assert.assertEquals(1, cursor.size());

		coll.drop();
		db.dropDatabase();
		mongoClient.close();
	}

	@Test
	public void collectionFindOne() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);

		BasicDBObject document = new BasicDBObject("author", "andrea").append(
				"text", "Pippo").append("date", new Date());

		BasicDBObject document1 = new BasicDBObject("author", "peppe").append(
				"text", "Pluto").append("date", new Date());

		BasicDBObject document2 = new BasicDBObject("author", "joseph").append(
				"text", "Paperino").append("date", new Date());

		DBCollection coll = db.getCollection("pippo");

		coll.insert(document);

		coll.insert(document1);

		coll.insert(document2);

		Set<String> colls = db.getCollectionNames();

		org.junit.Assert.assertEquals(2, colls.size());

		DBObject obj = coll.findOne();
		org.junit.Assert.assertEquals(obj.get("author"), "andrea");
		org.junit.Assert.assertEquals(obj.get("text"), "Pippo");

		coll.drop();
		db.dropDatabase();
		mongoClient.close();
	}

	@Test
	public void collectionFind() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);

		BasicDBObject document = new BasicDBObject("author", "andrea").append(
				"text", "Pippo").append("date", new Date());

		BasicDBObject document1 = new BasicDBObject("author", "peppe").append(
				"text", "Pluto").append("date", new Date());

		BasicDBObject document2 = new BasicDBObject("author", "joseph").append(
				"text", "Paperino").append("date", new Date());

		DBCollection coll = db.getCollection("pippo");

		coll.insert(document);

		coll.insert(document1);

		coll.insert(document2);

		Set<String> colls = db.getCollectionNames();

		org.junit.Assert.assertEquals(2, colls.size());

		DBCursor cursor = coll.find();
		org.junit.Assert.assertEquals(3, cursor.size());

		coll.drop();
		db.dropDatabase();
		mongoClient.close();
	}

	@Test
	public void collectionFindQuery() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);

		BasicDBObject document = new BasicDBObject("author", "andrea").append(
				"text", "Pippo").append("date", new Date());

		BasicDBObject document1 = new BasicDBObject("author", "peppe").append(
				"text", "Pluto").append("date", new Date());

		BasicDBObject document2 = new BasicDBObject("author", "joseph").append(
				"text", "Paperino").append("date", new Date());

		DBCollection coll = db.getCollection("pippo");

		coll.insert(document);

		coll.insert(document1);

		coll.insert(document2);

		Set<String> colls = db.getCollectionNames();

		org.junit.Assert.assertEquals(2, colls.size());

		BasicDBObject query = new BasicDBObject("author", "peppe").append(
				"text", "Pluto");

		DBCursor cursor = coll.find(query);
		org.junit.Assert.assertEquals(1, cursor.size());
		DBObject doc = cursor.next();
		org.junit.Assert.assertEquals("peppe", doc.get("author"));
		org.junit.Assert.assertEquals("Pluto", doc.get("text"));

		coll.drop();
		db.dropDatabase();
		mongoClient.close();
	}
	
	@Test
	public void collectionUpdate() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);

		BasicDBObject document = new BasicDBObject("author", "andrea").append(
				"text", "Pippo").append("date", new Date());

		BasicDBObject document1 = new BasicDBObject("author", "peppe").append(
				"text", "Pluto").append("date", new Date());

		BasicDBObject document2 = new BasicDBObject("author", "joseph").append(
				"text", "Paperino").append("date", new Date());

		DBCollection coll = db.getCollection("pippo");

		coll.insert(document);

		coll.insert(document1);

		coll.insert(document2);

		Set<String> colls = db.getCollectionNames();

		org.junit.Assert.assertEquals(2, colls.size());

		BasicDBObject query = new BasicDBObject("author", "peppe").append(
				"text", "Pluto");

		BasicDBObject set = new BasicDBObject("$set",new BasicDBObject("author","joseph").append("text", "Minnie"));
		coll.update(query,set);
		
		DBCursor cursor = coll.find(query);
		org.junit.Assert.assertEquals(0, cursor.size());

		query = new BasicDBObject("author", "joseph").append(
				"text", "Minnie");
		
		cursor = coll.find(query);
		org.junit.Assert.assertEquals(1, cursor.size());
		DBObject doc = cursor.next();
		org.junit.Assert.assertEquals("joseph", doc.get("author"));
		org.junit.Assert.assertEquals("Minnie", doc.get("text"));	

		coll.drop();
		db.dropDatabase();
		mongoClient.close();
	}
	
	@Test
	public void collectionUpsert() throws IOException {
		MongoClient mongoClient;
		MongoPropertyReader reader = new MongoPropertyReader(
				"config.properties");
		Properties properties = reader.getProperties();
		org.junit.Assert.assertNotNull(properties);
		MongoConfig config = new MongoConfig(
				properties.getProperty("mongodb.host"),
				Integer.parseInt(properties.getProperty("mongodb.port")));
		mongoClient = new MongoClient(config.getMongoHost(),
				config.getMongoPort());
		DB db = mongoClient.getDB("mydb");
		org.junit.Assert.assertNotNull(db);

		DBCollection coll = db.getCollection("pippo");

		BasicDBObject query = new BasicDBObject("author", "jimmy").append(
				"text", "Willy");

		DBCursor cursor = coll.find(query);
		//org.junit.Assert.assertEquals(0, cursor.size());
		
		String[] comments = new String[]{"Looks Great!","Boring..","Nice"};
		
		BasicDBObject set = new BasicDBObject("$set",new BasicDBObject("comments", comments));
		coll.update(query,set,true, false);
		
		cursor = coll.find(query);
		org.junit.Assert.assertEquals(1, cursor.size());
		
		DBObject doc = cursor.next();
		org.junit.Assert.assertEquals("jimmy", doc.get("author"));
		org.junit.Assert.assertEquals("Willy", doc.get("text"));
		org.junit.Assert.assertEquals(Arrays.asList(comments), doc.get("comments"));	

		coll.drop();
		db.dropDatabase();
		mongoClient.close();
	}
}
