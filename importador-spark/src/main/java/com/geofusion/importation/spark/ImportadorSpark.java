package com.geofusion.importation.spark;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.geofusion.importation.ColumnType;
import com.geofusion.importation.RecordTypeInference;

public class ImportadorSpark {
	public static List<String> rowToStrings(Row row) {
		List<String> ret = new ArrayList<>();
		for (int i=0; i<row.size(); i++) {
			ret.add(row.getString(i));
		}
		return ret;
	}
	
	public static Row applySchema(Row row, List<ColumnType> columns) {
		Object[] ret = new Object[columns.size()];
		for (int i=0; i<columns.size(); i++) {
			try {
				ret[i] = columns.get(i).parse(row.getString(i));
			} catch (Exception e) {
			}
		}
		return RowFactory.create(ret);
	}
	
	public static DataType toSparkDataType(ColumnType type) {
		switch (type) {
			case DOUBLE: return DataTypes.DoubleType;
			case LONG: return DataTypes.LongType;
			case STRING: return DataTypes.StringType;
			case TIMESTAMP:
			case TIMESTAMP_TZ:return DataTypes.TimestampType;
			default: throw new IllegalArgumentException(type.name());
		}
	}
	
	public static StructType createSparkSchema(StructType oldStructType, List<ColumnType> columnTypes) {
		List<StructField> fields = new ArrayList<>();
		for (int i=0; i<oldStructType.size(); i++) {
			fields.add(DataTypes.createStructField(
					oldStructType.fieldNames()[i], 
					toSparkDataType(columnTypes.get(i)), true));
		}
		return DataTypes.createStructType(fields);
	}

	public static void main(String[] csvFiles) {
		SparkConf sparkConf = new SparkConf().setAppName("Importador CSV").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
 
		SQLContext sqlContext = new SQLContext(sc);
		
		for (String csvFile : csvFiles) {
			System.out.println("Processando: " + csvFile);

			long now = System.currentTimeMillis();
			//1ª parte - Inferencia de tipos
			DataFrame df = sqlContext.read()
					.format("com.databricks.spark.csv")
					.option("header", "true")
					.load(csvFile).cache();
			System.out.println("Arquivo com " + df.count() + " registros lido em " + (System.currentTimeMillis() - now) + "ms");	
	
			now = System.currentTimeMillis();
			RecordTypeInference recordTypeInference = df
					.javaRDD()
					.sample(false, 0.1) //Processa apenas uma amostra pequenina
					.map(ImportadorSpark::rowToStrings)
					.mapPartitions((it) -> Arrays.asList(new RecordTypeInference(it)))
					.reduce((a,b) -> new RecordTypeInference().digest(a).digest(b));
			
			List<ColumnType> columnTypes = recordTypeInference.guessColumnTypes();
			System.out.println("Inferencia de tipos concluida em " + (System.currentTimeMillis() - now) + "ms");
			System.out.println(columnTypes);
			
			//2ª parte - conversão e salvamento no Mongo
			
			now = System.currentTimeMillis();
			StructType sparkSchema = createSparkSchema(df.schema(), columnTypes);
			DataFrame convertedData = sqlContext.createDataFrame(df.javaRDD().map((v) -> ImportadorSpark.applySchema(v, columnTypes)), sparkSchema);
			convertedData.write()
					.format("com.stratio.datasource.mongodb")
					.option("host", "localhost:27017")
					.option("database", "spark")
					.mode(SaveMode.Overwrite)
					.option("collection", csvFile).save();
			System.out.println("Exportação para Mongo concluida em " + (System.currentTimeMillis() - now) + "ms");
			
			df.unpersist();
		}
	}
}
