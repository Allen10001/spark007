package com.le.spark.sql;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Description
 * @Author Allen
 * @Date 2020-5-10 16:20
 * id,name,name_en,subtitle,category,cluster_id,external_id,site_id,state,sys_add,album_type,screen_year
 **/
public class AlbumBean implements Serializable {

    private static final long serialVersionUID = 1L;

    //index:  0
    private Long id;
    // 1
    private String name;
    // 11
    private Integer screenYear;

    public AlbumBean() {
    }

    public AlbumBean(Long id, String name, Integer screenYear) {
        this.id = id;
        this.name = name;
        this.screenYear = screenYear;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getScreenYear() {
        return screenYear;
    }

    public void setScreenYear(Integer screenYear) {
        this.screenYear = screenYear;
    }

    @Override
    public String toString() {
        return "AlbumBean{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", screenYear=" + screenYear +
                '}';
    }
}
