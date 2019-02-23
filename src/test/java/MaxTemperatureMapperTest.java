import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Administrator on 2019/2/17.
 */
public class MaxTemperatureMapperTest {
    @Test
    public void processesValidRecord() throws IOException, InterruptedException{
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"+
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
            .with;
    }
}