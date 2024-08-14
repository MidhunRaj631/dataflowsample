import org.apache.beam.sdk.transforms.DoFn;

class AppendSDoFn extends DoFn<String, String> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        String[] words = c.element().split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                c.output(word + "s");  // Append 's' to each word
            }
        }
    }
}