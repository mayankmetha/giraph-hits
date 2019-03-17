package abdaa3;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class HitsAlgo extends BasicComputation<Text,Text,NullWritable,Text> {

    public static final int MAX_SUPERSTEPS = 30;
    private static final Pattern SEPERATOR = Pattern.compile("[\t ]");
    //Vertex value = "h a"

    @Override
    public void compute(Vertex<Text,Text,NullWritable> vertex, Iterable<Text> messages)throws IOException {

        if(getSuperstep()%2 == 1 && getSuperstep() <= MAX_SUPERSTEPS) {
            List<String> replyID = new ArrayList<>();
            System.out.println(getSuperstep()+":"+vertex.getId());
            //compute authority score
            double sum = 0;
            for(Text msg: messages) {
                System.out.println("Msg:"+msg);
                String m[] = SEPERATOR.split(msg.toString());
                System.out.println(m[0]+" "+m[1]);
                replyID.add(m[0]);
                System.out.println(replyID.get(replyID.indexOf(m[0])));
                sum += Double.parseDouble(m[1]);
            }
            vertex.voteToHalt();
            String nScores[] = SEPERATOR.split(vertex.getValue().toString());
            nScores[1] = String.valueOf(sum);
            vertex.setValue(new Text(nScores[0]+" "+nScores[1]));
            //send authority score
            for (int i = 0; i < replyID.size(); i++ ) {
                sendMessage(new Text(replyID.get(i)),new Text(nScores[1]));
            }
        } else if(getSuperstep()%2 == 0 && getSuperstep() <= MAX_SUPERSTEPS) {
            //compute hub score
            if(getSuperstep() != 0) {
                double sum = 0;
                for(Text msg: messages) {
                    System.out.println("Msg:"+msg);
                    sum += Double.parseDouble(msg.toString());
                }
                System.out.println(getSuperstep()+":"+vertex.getId());
                String nScores[] = SEPERATOR.split(vertex.getValue().toString());
                nScores[0] = String.valueOf(sum);
                vertex.setValue(new Text(nScores[0]+" "+nScores[1]));
            }
            if(getSuperstep() == 0) {
                vertex.setValue(new Text("1 1"));
                System.out.println("superstep 0:"+vertex.getId()+" <> "+vertex.getValue());
            }
            //send hub score
            String scores[] = SEPERATOR.split(vertex.getValue().toString());
            sendMessageToAllEdges(vertex,new Text(vertex.getId()+" "+scores[0]));
        } else {
            vertex.voteToHalt();
        }
    }
}
