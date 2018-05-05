package data;

public class Organization {
    private int id, _lft, _rgt;
    private String name, slug, nameOrganize;

    public Organization() {
    }

    public Organization(int id, int _lft, int _rgt, String name, String slug) {
        this.id = id;
        this._lft = _lft;
        this._rgt = _rgt;
        this.name = name;
        this.slug = slug;
    }

    public Organization(String nameOrganize) {
        this.nameOrganize = nameOrganize;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public int get_lft() {
        return _lft;
    }

    public void set_lft(int _lft) {
        this._lft = _lft;
    }

    public int get_rgt() {
        return _rgt;
    }

    public void set_rgt(int _rgt) {
        this._rgt = _rgt;
    }
}
