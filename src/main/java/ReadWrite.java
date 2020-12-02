import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

public class ReadWrite {
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
    Pipeline p = Pipeline.create(options);
    p.apply(TextIO.read().from(options.getSourceFile()))
    .apply(TextIO.write().to(options.getOutputFile()));
    p.run().waitUntilFinish();
  }

  public interface Options extends PipelineOptions {
    @Validation.Required
    String getOutputFile();

    void setOutputFile(String file);

    @Validation.Required
    @Default.String("gs://apache-beam-samples/shakespeare/romeoandjuliet.txt")
    String getSourceFile();

    void setSourceFile(String file);
  }
}
