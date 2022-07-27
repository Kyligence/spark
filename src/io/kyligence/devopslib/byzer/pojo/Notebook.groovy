package io.kyligence.devopslib.byzer.pojo

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import groovy.transform.ToString

class Notebook {

    @Override
    public String toString() {
        return "Notebook{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", user='" + user + '\'' +
                ", cell_list=" + cell_list +
                '}';
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Cell {
        String id
        String content


        @Override
        public String toString() {
            return "Cell{" +
                    "id='" + id + '\'' +
                    ", content='" + content + '\'' +
                    '}';
        }
    }

    String id
    String name
    String user

    List<Cell> cell_list


}
