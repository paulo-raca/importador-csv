package com.geofusion.importation;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.Map;

@SuppressWarnings("serial")
public class ColumnTypeInference implements Serializable { 
	long count = 0;
	Map<ColumnType, Long> matches = new EnumMap<ColumnType, Long>(ColumnType.class);
	
	public ColumnTypeInference() {
		for (ColumnType type : ColumnType.values()) {
			matches.put(type, 0L);
		}
	} 

	public ColumnTypeInference digest(String value) {
		this.count++;
		for (ColumnType type : ColumnType.values()) {
			if (type.match(value)) {
				matches.put(type, matches.get(type) + 1L);
			}
		}
		return this;
	}
	
	public ColumnTypeInference digest(ColumnTypeInference other) {
		this.count += other.count;
		for (ColumnType type : ColumnType.values()) {
			this.matches.put(type, this.matches.get(type) + other.matches.get(type));
		}
		return this;
	}
	
	public ColumnType guessColumnType() {
		ColumnType bestType = null;
		for (ColumnType type : ColumnType.values()) {
			if (bestType == null || matches.get(type) > 1.01*matches.get(bestType)) {
				bestType = type;
			}
		}
		return bestType;
	}
	
	public Map<ColumnType, Long> getMatches() {
		return matches;
	}
	
	@Override
	public String toString() {
		return guessColumnType().name();
	}
	
	public long getCount() {
		return count;
	}
}