import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;


public class CustomDoFn {
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

    Pipeline p = Pipeline.create(options);
    String table = options.getInputTable();
    String targetTable = options.getOutputTable();
    p.apply("Read from BigQuery", BigQueryIO.readTableRows()
        .fromQuery(String.format("SELECT * FROM [%s] WHERE trip_seconds > 0 limit 5000", table)))
        .apply("Extract timestamps", ParDo.of(new ExtractTimestamp()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(3600 * 24))))
        .apply(MapElements.via(new SimpleFunction<TableRow, KV<String, Double>>() {
          @Override
          public KV<String, Double> apply(TableRow input) {
            return KV.of((String) input.get("payment_type"), (Double) input.get("trip_total"));
          }
        }))
        .apply(Sum.doublesPerKey())
        .apply(ParDo.of(new Print()));

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

  public static class ExtractTimestamp extends DoFn<TableRow, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      TableRow tableRow = context.element();
      Instant tripStartTimestamp = Instant.parse((String) tableRow.get("trip_start_timestamp"), DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss z"));//2013-02-09 13:45:00 UTC
      context.outputWithTimestamp(tableRow, tripStartTimestamp);
    }
  }

  public static class Print extends DoFn<KV<String, Double>, Void> {
    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      System.out.printf("Window with upper bound %s: %f%n", window.maxTimestamp().toString(), context.element().getValue());
    }
  }
}
