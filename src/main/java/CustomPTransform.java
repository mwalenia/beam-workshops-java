import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class CustomPTransform {
  public static void main(String[] args) {
    ReadWrite.Options options = PipelineOptionsFactory.fromArgs(args).as(ReadWrite.Options.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> lines = p.apply(TextIO.read().from(options.getSourceFile()));
    lines
        .apply(new CountAndPrint("lines")); //Print number of lines in a file

    lines.apply(FlatMapElements.via(new SimpleFunction<String, Iterable<String>>() { //FlatMap is like `Map`, but output is flattened into a single PCollection.
      @Override
      public Iterable<String> apply(String input) {
        return Arrays.asList(input.split(" "));
      }
    }))
        .apply(new CountAndPrint("words")); //Print number of words in a file
    p.run().waitUntilFinish();
  }

  public static class CountAndPrint extends PTransform<PCollection<String>, PCollection<Void>> {

    private final String label;

    public CountAndPrint(String label) {
      this.label = label;
    }

    @Override
    public PCollection<Void> expand(PCollection<String> input) {
      // Apply transforms with unique labels
      return input.apply("Count " + this.label, Count.globally())
          .apply("Print " + label, MapElements.via(new SimpleFunction<>() {
            @Override
            public Void apply(Long input) {
              System.out.println(input);
              return null;
            }
          }));
    }
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
