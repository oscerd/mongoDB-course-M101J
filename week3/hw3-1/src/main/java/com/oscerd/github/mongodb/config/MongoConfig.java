package com.oscerd.github.mongodb.config;

public class MongoConfig {

	private String mongoHost;
	private int mongoPort;

	public String getMongoHost() {
		return mongoHost;
	}

	public void setMongoHost(String mongoHost) {
		this.mongoHost = mongoHost;
	}

	public int getMongoPort() {
		return mongoPort;
	}

	public void setMongoPort(int mongoPort) {
		this.mongoPort = mongoPort;
	}

	public MongoConfig(String mongoHost, int mongoPort) {
		super();
		this.mongoHost = mongoHost;
		this.mongoPort = mongoPort;
	}

	@Override
	public String toString() {
		return "MongoConfig [mongoHost=" + mongoHost + ", mongoPort="
				+ mongoPort + "]";
	}

}
