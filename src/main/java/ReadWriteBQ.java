import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

public class ReadWriteBQ {
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

    Pipeline p = Pipeline.create(options);
    String table = options.getInputTable();
    String targetTable = options.getOutputTable();
    p.apply("Read from BigQuery", BigQueryIO.readTableRows()
        .fromQuery(String.format("SELECT * FROM [%s] WHERE trip_seconds > 0 limit 5000", table)))
        .apply("Write to BQ", BigQueryIO.writeTableRows().to(targetTable));
    p.run().waitUntilFinish();
  }

  public interface Options extends PipelineOptions {
    @Validation.Required
    String getOutputTable();

    void setOutputTable(String table);

    @Validation.Required
    @Default.String("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    String getInputTable();

    void setInputTable(String table);
  }
}
