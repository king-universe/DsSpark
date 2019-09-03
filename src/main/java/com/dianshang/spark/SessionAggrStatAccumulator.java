package com.dianshang.spark;

import com.dianshang.model.AggrStatAccumulatorModel;
import com.dianshang.utils.Constants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.util.AccumulatorV2;

public class SessionAggrStatAccumulator extends AccumulatorV2<String, AggrStatAccumulatorModel> {

    private AggrStatAccumulatorModel aggrStatAccumulatorModel = new AggrStatAccumulatorModel();

    private static final Logger LOGGER = LogManager.getLogger(SessionAggrStatAccumulator.class);


    public boolean isZero() {
        return true;
    }

    public AccumulatorV2<String, AggrStatAccumulatorModel> copy() {
        SessionAggrStatAccumulator statAccumulator = new SessionAggrStatAccumulator();
        statAccumulator.reset();
        return statAccumulator;
    }

    public void reset() {
        aggrStatAccumulatorModel = new AggrStatAccumulatorModel();
    }

    public void add(String v) {
        System.out.println(v);
        switch (v) {
            case Constants.SESSION_COUNT:
                aggrStatAccumulatorModel.setSessioncount(aggrStatAccumulatorModel.getSessioncount()+1);
                break;
            case Constants.TIME_PERIOD_1s_3s:
                aggrStatAccumulatorModel.setTimePeriod1To3s(aggrStatAccumulatorModel.getTimePeriod1To3s() + 1);
                break;
            case Constants.TIME_PERIOD_4s_6s:
                aggrStatAccumulatorModel.setTimePeriod4To6s(aggrStatAccumulatorModel.getTimePeriod4To6s() + 1);
                break;
            case Constants.TIME_PERIOD_7s_9s:
                aggrStatAccumulatorModel.setTimePeriod7To9s(aggrStatAccumulatorModel.getTimePeriod7To9s() + 1);
                break;
            case Constants.TIME_PERIOD_10s_30s:
                aggrStatAccumulatorModel.setTimePeriod10To30s(aggrStatAccumulatorModel.getTimePeriod10To30s() + 1);
                break;
            case Constants.TIME_PERIOD_30s_60s:
                aggrStatAccumulatorModel.setTimePeriod30To60s(aggrStatAccumulatorModel.getTimePeriod30To60s() + 1);
                break;
            case Constants.TIME_PERIOD_1m_3m:
                aggrStatAccumulatorModel.setTimePeriod1To3m(aggrStatAccumulatorModel.getTimePeriod1To3m() + 1);
                break;
            case Constants.TIME_PERIOD_3m_10m:
                aggrStatAccumulatorModel.setTimePeriod3To10m(aggrStatAccumulatorModel.getTimePeriod3To10m() + 1);
                break;
            case Constants.TIME_PERIOD_10m_30m:
                aggrStatAccumulatorModel.setTimePeriod10To30m(aggrStatAccumulatorModel.getTimePeriod10To30m() + 1);
                break;
            case Constants.TIME_PERIOD_30m:
                aggrStatAccumulatorModel.setTimePeriod30m(aggrStatAccumulatorModel.getTimePeriod30m() + 1);
                break;
            case Constants.STEP_PERIOD_1_3:
                aggrStatAccumulatorModel.setStepPeriod1To3(aggrStatAccumulatorModel.getStepPeriod1To3() + 1);
                break;
            case Constants.STEP_PERIOD_4_6:
                aggrStatAccumulatorModel.setStepPeriod4To6(aggrStatAccumulatorModel.getStepPeriod4To6() + 1);
                break;
            case Constants.STEP_PERIOD_7_9:
                aggrStatAccumulatorModel.setStepPeriod7To9(aggrStatAccumulatorModel.getStepPeriod7To9() + 1);
                break;
            case Constants.STEP_PERIOD_10_30:
                aggrStatAccumulatorModel.setStepPeriod10To30(aggrStatAccumulatorModel.getStepPeriod10To30() + 1);
                break;
            case Constants.STEP_PERIOD_30_60:
                aggrStatAccumulatorModel.setStepPeriod30To60(aggrStatAccumulatorModel.getStepPeriod30To60() + 1);
                break;
            case Constants.STEP_PERIOD_60:
                aggrStatAccumulatorModel.setStepPeriod60(aggrStatAccumulatorModel.getStepPeriod60() + 1);
                break;
            default:
                LOGGER.error("累加器信息错误：" + v);
                break;
        }
    }

    public void merge(AccumulatorV2<String, AggrStatAccumulatorModel> other) {
        if (other instanceof SessionAggrStatAccumulator) {
            aggrStatAccumulatorModel = other.value();
        } else {
            throw new UnsupportedOperationException("Cannot merge " + this.getClass().getName() + " with" + other.getClass().getName());
        }
    }

    public AggrStatAccumulatorModel value() {
        return aggrStatAccumulatorModel;
    }
}
