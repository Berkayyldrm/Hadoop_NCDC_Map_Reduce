import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MeanTemperatureMapper
	extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private NcdcRecordParser parser = new NcdcRecordParser();
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemperature()) {
	context.write(new Text(parser.getYear()),
			new DoubleWritable(parser.getAirTemperature()));
	}
	}
}