package com.cs.rfq.decorator;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        //TODO: create an explicit schema for the trade data in the JSON files
        StructType schema = new StructType()
                .add("TraderId", LongType)
                .add("EntityId", LongType)
                //.add("MsgType", StringType)
                //.add("TradeReportId", DataTypes.LongType)
                //.add("PreviouslyReported", DataTypes.StringType)
                .add("SecurityID", DataTypes.StringType)
                //.add("SecurityIdSource", DataTypes.LongType)
                .add("LastQty", DataTypes.LongType)
                .add("LastPx", DataTypes.DoubleType)
                .add("TradeDate", DataTypes.DateType)
                //.add("TransactTime", DataTypes.TimestampType)
                //.add("NoSides", DataTypes.IntegerType)
                //.add("Side", DataTypes.IntegerType)
                //.add("OrderID", DataTypes.LongType)
                .add("Currency", DataTypes.StringType)
        ;

        //TODO: load the trades dataset
        Dataset<Row> trades = session
                .read().
                        schema(schema).
                        json(path);

        //TODO: log a message indicating number of records loaded and the schema used
        log.info(String.format("Number of records loaded:%n%s  ||  schema used:%n%s", Long.toString(trades.count()), schema.treeString()));
        return trades;
    }

}
