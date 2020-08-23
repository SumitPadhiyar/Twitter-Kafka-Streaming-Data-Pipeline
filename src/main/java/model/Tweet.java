package model;

public class Tweet {
    private long id;
    private String text;
    private String lang;

    public Tweet(long id, String text, String lang) {
        this.id = id;
        this.text = text;
        this.lang = lang;
    }

    public long getId() {
        return id;
    }

    public String getText() {
        return text;
    }

    public String getLang() {
        return lang;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "id=" + id +
                ", text='" + text + '\'' +
                ", lang='" + lang + '\'' +
                '}';
    }
}
