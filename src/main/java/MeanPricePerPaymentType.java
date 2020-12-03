import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class MeanPricePerPaymentType {

  private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss z");

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

    Pipeline p = Pipeline.create(options);
    String table = options.getInputTable();
    p.apply("Read from BigQuery", BigQueryIO.readTableRows()
        .fromQuery(String.format("SELECT * FROM [%s] WHERE trip_seconds > 0 limit 5000", table)))
        .apply("Extract timestamps", ParDo.of(new ExtractTimestamp()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(3600 * 24 * 30))))
        .apply(MapElements.via(new SimpleFunction<TableRow, KV<String, Double>>() {
          @Override
          public KV<String, Double> apply(TableRow input) {
            Double tripTotalPrice = (Double) input.get("trip_total");
            String paymentMethod = (String) input.get("payment_type");
            return KV.of(paymentMethod, tripTotalPrice);
          }
        }))
        .apply(Mean.perKey())
        .apply(ParDo.of(new MapToResultString()))
        .apply(TextIO.write().to(options.getOutputFile()));

    p.run().waitUntilFinish();
  }

  public interface Options extends PipelineOptions {
    @Validation.Required
    @Default.String("bigquery-public-data.chicago_taxi_trips.taxi_trips")
    String getInputTable();

    void setInputTable(String table);

    String getOutputFile();

    void setOutputFile(String fileName);
  }

  public static class ExtractTimestamp extends DoFn<TableRow, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      TableRow tableRow = context.element();
      Instant tripStartTimestamp = Instant.parse((String) tableRow.get("trip_start_timestamp"),
          DATE_TIME_FORMATTER);//2013-02-09 13:45:00 UTC
      context.outputWithTimestamp(tableRow, tripStartTimestamp);
    }
  }

  public static class MapToResultString extends DoFn<KV<String, Double>, String> {
    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window) {
      String paymentType = context.element().getKey();
      Double meanValue = context.element().getValue();
      String result = String.format("Upper window bound: %s. For %s, mean price: %f",
          window.maxTimestamp().toString(DATE_TIME_FORMATTER),
          paymentType,
          meanValue);
      context.output(result);
    }
  }
}
