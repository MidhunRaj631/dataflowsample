import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;

public class WordTransform {

    public static Pipeline createPipeline(String inputPath, String outputPath) {
        Pipeline p = Pipeline.create();

        // Read lines from the input file
        PCollection<String> lines = p.apply(TextIO.read().from(inputPath));

        // Split lines into words
        PCollection<String> words = lines.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                for (String word : c.element().split("\\s+")) {
                    if (!word.isEmpty()) {
                        c.output(word+"-transformed");
                    }
                }
            }
        }));

        // Write word counts to the output file
        words.apply(TextIO.write().to(outputPath).withSuffix(".txt").withoutSharding());

        return p;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: WordCount <inputPath> <outputPath>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Pipeline pipeline = createPipeline(inputPath, outputPath);
        pipeline.run().waitUntilFinish();
    }
}
