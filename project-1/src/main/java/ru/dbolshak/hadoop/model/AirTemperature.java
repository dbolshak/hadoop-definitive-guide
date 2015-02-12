package ru.dbolshak.hadoop.model;

import org.apache.hadoop.io.Text;

/**
 * Just a Pojo, which knows how to handle a line from file to present a temperature information.
 */
public class AirTemperature {
    private final String line;

    public AirTemperature(Text line) {
        this.line = line.toString();
    }

    public String getYear() {
        return line.substring(15, 19);
    }

    public int getAirTemperature() {
        return Integer.parseInt(line.substring(87, 92));
    }

    public String getQuality() {
        return line.substring(92, 93);
    }
}
