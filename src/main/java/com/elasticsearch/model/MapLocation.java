package com.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class MapLocation implements Serializable{

    @JsonProperty(value = "lat")
    private Double lat;
    @JsonProperty(value = "lon")
    private Double lon;

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }
}
