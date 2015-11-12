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
	
	public void digest(Iterable<String> values) {
		this.count++;
		int i=0;
		for (String value : values) {
			if (i >= columns.size()) {
				columns.add(new ColumnTypeInference());
			}
			columns.get(i).digest(value);
			i++;
		}
	}
	
	public static RecordTypeInference merge(RecordTypeInference... values) {
		RecordTypeInference ret = new RecordTypeInference();
		List<List<ColumnTypeInference>> columns = new ArrayList<>();
		for (RecordTypeInference value : values) {
			for (int i=0; i<value.columns.size(); i++) {
				if (i >= columns.size()) {
					columns.add(new ArrayList<ColumnTypeInference>());
				}
				columns.get(i).add(value.columns.get(i));
			}
		}
		for (List<ColumnTypeInference> column : columns) {
			ret.columns.add(ColumnTypeInference.merge(column));
		}
		return ret;
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
