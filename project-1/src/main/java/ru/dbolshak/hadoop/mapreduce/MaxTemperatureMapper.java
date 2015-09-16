package ru.dbolshak.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.dbolshak.hadoop.model.AirTemperature;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            AirTemperature airTemperature = new AirTemperature(value);

            if (airTemperature.isValid()) {
                context.write(
                        new Text(airTemperature.getYear()),
                        new IntWritable(airTemperature.getAirTemperature())
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(value.toString());
        }
    }
}
