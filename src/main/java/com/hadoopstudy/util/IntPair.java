package com.hadoopstudy.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair>{
    private int first;
    private int second;

    public IntPair(int first, int second){
        this.first = first;
        this.second = second;
    }

    public IntPair(String first, int second){
        this.first = Integer.parseInt(first);
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    //这个地方对数据进行排序，前递增后递减
    public int compareTo(IntPair pair) {
        int cmpFirst=Integer.valueOf(first).compareTo(pair.first);
        if (cmpFirst != 0) {
            return cmpFirst;
        } else{
            return Integer.valueOf(second).compareTo(pair.second);
        }
    }

    public static int compare(int first1, int first2) {
        return Integer.valueOf(first1).compareTo(first2);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }
}
