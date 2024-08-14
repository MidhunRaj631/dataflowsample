import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;

public class WordCountExample {

    public static Pipeline createPipeline(String inputPath, String outputPath) {
// Create a PipelineOptions object
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options
        Pipeline p = Pipeline.create(options);

        // Read from a file (replace with your input file path)
        PCollection<String> lines = p.apply("ReadLines", TextIO.read().from(inputPath));

        // Split each line into words and count them
        PCollection<Integer> wordCounts = lines.apply("CountWords", ParDo.of(new DoFn<String, Integer>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                int wordCount = line.split("\\s+").length;
                c.output(wordCount);
            }
        }));

        // Sum up all the word counts to get the total count
        PCollection<Integer> totalWordCount = wordCounts.apply("SumCounts", Sum.integersGlobally());

        // Write the result to a file (replace with your output file path)
        totalWordCount.apply("WriteCounts", ParDo.of(new DoFn<Integer, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output("Total word count: " + c.element());
            }
        })).apply(TextIO.write().to(outputPath).withSuffix(".txt").withoutSharding());


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
