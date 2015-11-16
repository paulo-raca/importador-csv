package com.geofusion.importation.storm;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MongoId implements Serializable {
	private final String database;
	private final String collection;
	private final Object _id;
	public MongoId(String database, String collection, Object _id) {
		this.database = database;
		this.collection = collection;
		this._id = _id;
	}
	public String getDatabase() {
		return database;
	}
	public String getCollection() {
		return collection;
	}
	public Object get_id() {
		return _id;
	}	

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof MongoId) {
			MongoId other = (MongoId)obj;
			return other.database.equals(this.database) && other.collection.equals(this.collection) && other._id.equals(this._id);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (getDatabase().hashCode() * 997) ^ (getCollection().hashCode() * 503) ^ (_id.hashCode()*79);
	}
	@Override
	public String toString() {
		return getDatabase() + "/" + getCollection() + "/" + get_id();
	}
}
