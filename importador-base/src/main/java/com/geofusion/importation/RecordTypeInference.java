package com.geofusion.importation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("serial")
public class RecordTypeInference implements Serializable {
	long count = 0;
	List<ColumnTypeInference> columns = new ArrayList<>();
	
	public RecordTypeInference() {}
	
	public RecordTypeInference(Iterator<List<String>> it) {
		while (it.hasNext()) {
			digest(it.next());
		}
	}
	
	public RecordTypeInference digest(Iterable<String> values) {
		this.count++;
		int i=0;
		for (String value : values) {
			if (i >= columns.size()) {
				columns.add(new ColumnTypeInference());
			}
			columns.get(i).digest(value);
			i++;
		}
		return this;
	}
	
	public RecordTypeInference digest (RecordTypeInference other) {
		this.count += other.count;
		for (int i=0; i<other.columns.size(); i++) {
			if (i >= this.columns.size()) {
				this.columns.add(new ColumnTypeInference());
			}
			columns.get(i).digest(other.columns.get(i));
		}
		return this;
	}

	public List<ColumnType> guessColumnTypes() {
		List<ColumnType> ret = new ArrayList<>();
		for (ColumnTypeInference column : columns) {
			ret.add(column.guessColumnType());
		}
		return ret;
	}
	
	public List<ColumnTypeInference> getColumns() {
		return columns;
	}
	
	@Override
	public String toString() {
		return guessColumnTypes().toString();
	}
	
	public long getCount() {
		return count;
	}
}
