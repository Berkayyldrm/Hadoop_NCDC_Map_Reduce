import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MeanTemperatureReducer
	extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		double totalValue = 0;
		double count = 0;
		double scale_factor = 10;
		
		for (DoubleWritable value : values) {
			totalValue = totalValue += value.get();
			count += 1;
		}
		context.write(key, new DoubleWritable(totalValue/(count*scale_factor)));
	}
}