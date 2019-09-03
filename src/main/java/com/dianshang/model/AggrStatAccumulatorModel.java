package com.dianshang.model;

import java.io.Serializable;

public class AggrStatAccumulatorModel implements Serializable {
    private Long timePeriod1To3s=0l; //访问时长1秒-3秒
    private Long timePeriod4To6s=0l;
    private Long timePeriod7To9s=0l;
    private Long timePeriod10To30s=0l;
    private Long timePeriod30To60s=0l;
    private Long timePeriod1To3m=0l;
    private Long timePeriod3To10m=0l;
    private Long timePeriod10To30m=0l;
    private Long timePeriod30m=0l;

    private Long  stepPeriod1To3=0l; //访问步长1-3bu
    private Long  stepPeriod4To6=0l;
    private Long  stepPeriod7To9=0l;
    private Long  stepPeriod10To30=0l;
    private Long  stepPeriod30To60=0l;
    private Long  stepPeriod60=0l;


    private Long sessioncount=0l;

    public Long getSessioncount() {
        return sessioncount;
    }

    public void setSessioncount(Long sessioncount) {
        this.sessioncount = sessioncount;
    }

    public Long getTimePeriod1To3s() {
        return timePeriod1To3s;
    }

    public void setTimePeriod1To3s(Long timePeriod1To3s) {
        this.timePeriod1To3s = timePeriod1To3s;
    }

    public Long getTimePeriod4To6s() {
        return timePeriod4To6s;
    }

    public void setTimePeriod4To6s(Long timePeriod4To6s) {
        this.timePeriod4To6s = timePeriod4To6s;
    }

    public Long getTimePeriod7To9s() {
        return timePeriod7To9s;
    }

    public void setTimePeriod7To9s(Long timePeriod7To9s) {
        this.timePeriod7To9s = timePeriod7To9s;
    }

    public Long getTimePeriod10To30s() {
        return timePeriod10To30s;
    }

    public void setTimePeriod10To30s(Long timePeriod10To30s) {
        this.timePeriod10To30s = timePeriod10To30s;
    }

    public Long getTimePeriod30To60s() {
        return timePeriod30To60s;
    }

    public void setTimePeriod30To60s(Long timePeriod30To60s) {
        this.timePeriod30To60s = timePeriod30To60s;
    }

    public Long getTimePeriod1To3m() {
        return timePeriod1To3m;
    }

    public void setTimePeriod1To3m(Long timePeriod1To3m) {
        this.timePeriod1To3m = timePeriod1To3m;
    }

    public Long getTimePeriod3To10m() {
        return timePeriod3To10m;
    }

    public void setTimePeriod3To10m(Long timePeriod3To10m) {
        this.timePeriod3To10m = timePeriod3To10m;
    }

    public Long getTimePeriod10To30m() {
        return timePeriod10To30m;
    }

    public void setTimePeriod10To30m(Long timePeriod10To30m) {
        this.timePeriod10To30m = timePeriod10To30m;
    }

    public Long getTimePeriod30m() {
        return timePeriod30m;
    }

    public void setTimePeriod30m(Long timePeriod30m) {
        this.timePeriod30m = timePeriod30m;
    }

    public Long getStepPeriod1To3() {
        return stepPeriod1To3;
    }

    public void setStepPeriod1To3(Long stepPeriod1To3) {
        this.stepPeriod1To3 = stepPeriod1To3;
    }

    public Long getStepPeriod4To6() {
        return stepPeriod4To6;
    }

    public void setStepPeriod4To6(Long stepPeriod4To6) {
        this.stepPeriod4To6 = stepPeriod4To6;
    }

    public Long getStepPeriod7To9() {
        return stepPeriod7To9;
    }

    public void setStepPeriod7To9(Long stepPeriod7To9) {
        this.stepPeriod7To9 = stepPeriod7To9;
    }

    public Long getStepPeriod10To30() {
        return stepPeriod10To30;
    }

    public void setStepPeriod10To30(Long stepPeriod10To30) {
        this.stepPeriod10To30 = stepPeriod10To30;
    }

    public Long getStepPeriod30To60() {
        return stepPeriod30To60;
    }

    public void setStepPeriod30To60(Long stepPeriod30To60) {
        this.stepPeriod30To60 = stepPeriod30To60;
    }

    public Long getStepPeriod60() {
        return stepPeriod60;
    }

    public void setStepPeriod60(Long stepPeriod60) {
        this.stepPeriod60 = stepPeriod60;
    }

    @Override
    public String toString() {
        return "AggrStatAccumulatorModel{" +
                "timePeriod1To3s=" + timePeriod1To3s +
                ", timePeriod4To6s=" + timePeriod4To6s +
                ", timePeriod7To9s=" + timePeriod7To9s +
                ", timePeriod10To30s=" + timePeriod10To30s +
                ", timePeriod30To60s=" + timePeriod30To60s +
                ", timePeriod1To3m=" + timePeriod1To3m +
                ", timePeriod3To10m=" + timePeriod3To10m +
                ", timePeriod10To30m=" + timePeriod10To30m +
                ", timePeriod30m=" + timePeriod30m +
                ", stepPeriod1To3=" + stepPeriod1To3 +
                ", stepPeriod4To6=" + stepPeriod4To6 +
                ", stepPeriod7To9=" + stepPeriod7To9 +
                ", stepPeriod10To30=" + stepPeriod10To30 +
                ", stepPeriod30To60=" + stepPeriod30To60 +
                ", stepPeriod60=" + stepPeriod60 +
                ", sessioncount=" + sessioncount +
                '}';
    }
}
