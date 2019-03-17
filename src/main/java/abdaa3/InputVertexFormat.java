package abdaa3;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class InputVertexFormat extends TextVertexInputFormat<Text, Text, NullWritable> {

    private static final Pattern SEPERATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new InputVertexReader();
    }

    public class InputVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

        private Text id;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = SEPERATOR.split(line.toString());
            id = new Text(tokens[0]);
            return tokens;
        }

        @Override
        protected Text getId(String[] tokens)throws IOException {
            return id;
        }

        @Override
        protected Text getValue(String[] tokens)throws IOException {
            return new Text("1 1");
        }

        @Override
        protected Iterable<Edge<Text,NullWritable>> getEdges(String[] tokens)throws IOException {
            List<Edge<Text,NullWritable>> edges = new ArrayList<>();
            for(int i = 1; i < tokens.length; i++) {
                edges.add(EdgeFactory.create(new Text(tokens[i])));
            }
            return edges;
        }
    }
}