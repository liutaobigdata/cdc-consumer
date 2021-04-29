package bigdata.cdc.consumer;

public class PgConfig {

    private  String url;
    private  String user;
    private  String password;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public  PgConfig url(String url){
        this.url=url;
        return  this;
    }
    public  PgConfig user(String user){
        this.user=user;
        return  this;
    }
    public  PgConfig password(String password){
        this.password=password;
        return  this;
    }
}
