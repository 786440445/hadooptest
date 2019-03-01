package com.hadoopstudy.util;

import org.apache.hadoop.io.Text;

/**
 * Created by Administrator on 2019/2/17.
 */
public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;
    private String year;
    private int airTemperature;
    private String quality;
    private String stationId;
    private String stationName;

    public void parse(String record){
        year = record.substring(15, 19);
        String airTemperatureString;

        if(record.charAt(97) == '+'){
            airTemperatureString = record.substring(88, 92);
        }else{
            airTemperatureString = record.substring(87, 92);
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record){
        parse(record.toString());
    }

    public boolean isValidTemperature(){
        return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
    }

    public boolean isMalformedTemperate(){
        return false;
    }

    public boolean isMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public String getQuality() {
        return quality;
    }

    public String getYear(){
        return year;
    }

    public int getAirTemperature(){
        return airTemperature;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }
}
