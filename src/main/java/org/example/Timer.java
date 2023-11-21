package org.example;

public class Timer {
    long start;

    public Timer() {
        this.start = System.currentTimeMillis();
    }

    public void reset() {
        this.start = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return String.format("%.3f", (System.currentTimeMillis() - this.start) / 1000.0);
    }
}
