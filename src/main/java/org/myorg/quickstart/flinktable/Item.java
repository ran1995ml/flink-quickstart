package org.myorg.quickstart.flinktable;

/**
 * ClassName: Item
 * Description:
 * date: 2021/12/26 22:22
 *
 * @author ran
 */
public class Item {
    private String name;
    private Integer id;
    public Item(){}

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
