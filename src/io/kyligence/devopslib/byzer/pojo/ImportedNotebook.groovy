package io.kyligence.devopslib.byzer.pojo

import groovy.transform.ToString

class ImportedNotebook {
    String  id
    String name
    String type


    @Override
    public String toString() {
        return "ImportedNotebook{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
