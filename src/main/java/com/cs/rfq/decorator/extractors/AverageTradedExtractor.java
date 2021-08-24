package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class AverageTradedExtractor implements RfqMetadataExtractor{
    private String since;

    public AverageTradedExtractor() {
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        this.since = Long.toString(DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis());
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String query = String.format("SELECT sum(LastPx*LastQty)/sum(LastQty)  from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object avg = sqlQueryResults.first().get(0);
        if (avg == null) {
            avg = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.averagePricePastWeek, avg);
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
