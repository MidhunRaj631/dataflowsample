import org.junit.Test;

public class TestWordCount {

    @Test
    public void testWordCountMain() throws Exception {

        // Run the WordCount main method
        WordCountExample.main(new String[]{"C:\\Users\\midhu\\Documents\\input\\sample.txt", "C:\\Users\\midhu\\Documents\\output\\count"});


    }

    @Test
    public void testWordTransformMain() throws Exception {

        // Run the WordCount main method
        WordTransform.main(new String[]{"C:\\Users\\midhu\\Documents\\input\\sample.txt", "C:\\Users\\midhu\\Documents\\output\\transform"});


    }
    @Test
    public void testSchemaMain() throws Exception {

        // Run the WordCount main method
        SchemaExample.main(new String[]{"C:\\Users\\midhu\\Documents\\input\\sampleSchema.txt", "C:\\Users\\midhu\\Documents\\output\\schemaExample"});


    }
}