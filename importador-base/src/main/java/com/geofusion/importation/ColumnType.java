package com.geofusion.importation;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public enum ColumnType {
	LONG {
		@Override
		public Object parse(String value) throws Exception {
			return Long.parseLong(value);
		}
	},
	DOUBLE {
		@Override
		public Object parse(String value) throws Exception {
			return Double.parseDouble(value);
		}
	},
	TIMESTAMP_TZ {
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa Z");
		@Override
		public Object parse(String value) throws Exception {
			return new Timestamp(dateFormat.parse(value).getTime());
		}
	},
	TIMESTAMP {
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");
		@Override
		public Object parse(String value) throws Exception {
			return new Timestamp(dateFormat.parse(value).getTime());
		}
	},
	STRING {
		@Override
		public Object parse(String value) throws Exception {
			return value;
		}
	};

	

	public boolean match(String value) {
		try {
			parse(value);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	public abstract Object parse(String value) throws Exception;
}
