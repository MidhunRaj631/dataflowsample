import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

public class SchemaExample {

    public static Pipeline createPipeline(String inputPath, String outputPath) {
        // Define the schema
        Schema schema = Schema.builder()
                .addInt32Field("id")
                .addStringField("name")
                .addInt32Field("age")
                .build();

        // Create a PipelineOptions object
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options
        Pipeline p = Pipeline.create(options);

        // Sample input data as CSV
        PCollection<String> input = p.apply("ReadCSV", TextIO.read().from(inputPath));

        // Convert the input strings to Rows using the defined schema
        PCollection<Row> rows = input.apply("ParseCSV", ParDo.of(new DoFn<String, Row>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] fields = c.element().split(",");
                Row row = Row.withSchema(schema)
                        .addValues(Integer.parseInt(fields[0]), fields[1], Integer.parseInt(fields[2]))
                        .build();
                c.output(row);
            }
        })).setRowSchema(schema);

        // Apply a simple transformation: Select the "name" and "age" fields
        PCollection<Row> selectedRows = rows.apply("SelectFields", Select.fieldNames("name", "age"));

        // Convert Rows back to Strings for output
        PCollection<String> output = selectedRows.apply("FormatOutput", MapElements
                .into(TypeDescriptors.strings())
                .via((SerializableFunction<Row, String>) row ->
                        row.getString("name") + "," + row.getInt32("age")));

        // Write the output to a file
        output.apply("WriteOutput", TextIO.write().to(outputPath).withSuffix(".txt").withoutSharding());

        // Run the pipeline
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
