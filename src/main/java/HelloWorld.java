import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class HelloWorld {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of("Hello World!"))
        .apply(MapElements.via(new SimpleFunction<>() {
          @Override
          public Object apply(String input) {
            System.out.println(input);
            return null;
          }
        }));
    p.run().waitUntilFinish();
  }
}
