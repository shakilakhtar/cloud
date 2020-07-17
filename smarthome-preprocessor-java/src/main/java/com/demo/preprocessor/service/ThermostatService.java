package com.demo.preprocessor.service;

public interface ThermostatService {

    float getTemperature() throws Exception;

    void setTemperature(float value) throws Exception;
}
