package com.demo.preprocessor.service;

public class ThermostatServiceImpl implements ThermostatService {

    private final float defaultTemp = 65.0f;
    private float currentTemp = 0.0f;

    @Override
    public float getTemperature() throws Exception {
        if (currentTemp == 0.0) {
            currentTemp = defaultTemp;
        }
        return currentTemp;
    }

    @Override
    public void setTemperature(float value) throws Exception {
        this.currentTemp = value;
    }
}
