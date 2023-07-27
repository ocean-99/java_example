package win.ocean99;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import win.ocean99.util.OracleJDBC;
import win.ocean99.util.OracleQuery;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

@ShellComponent
public class CommandHandler {

    @ShellMethod("Say hello")
    public String hello(String name) {
        return "Hello, " + name + "!";
    }


    @ShellMethod("数据库地址")
    public String db() {
        return OracleJDBC.url+"\n"+OracleJDBC.user+"\n"+OracleJDBC.password;
    }
    @ShellMethod("测试")
    public void test() {
        OracleQuery.testConnection();
    }
    @ShellMethod("修改数据库地址")
    public String changeDB(String url,String user,String password) {
        Optional.of(url).ifPresent(s -> OracleJDBC.url = s);
        Optional.of(user).ifPresent(s -> OracleJDBC.user = s);
        Optional.of(password).ifPresent(s -> OracleJDBC.password = s);
        return "数据库地址已修改为：" + url;
    }

    @ShellMethod("获取当前地址")
    public void getCurPath() {
        Path currentPath = Paths.get("");
        String currentAbsolutePath = currentPath.toAbsolutePath().toString();
        System.out.println("当前位置的绝对路径：" + currentAbsolutePath);
    }

    @ShellMethod(value = "导出序列化表数据(.set)")
    public String exp(String t,String w) throws IOException {
        this.expSer(t,w);
        return "导出成功";
    }

    @ShellMethod(value = "导出表数据(.sql)")
    public String expSql(String t,String w) throws IOException {
        OracleQuery.exportTableDataAsSql(t,w);
        return "导出成功";
    }

    @ShellMethod(value = "导出表数据(.json)")
    public String expJson(String t,String w) throws IOException {
        OracleQuery.exportTableDataAsJson(t,w);
        return "导出成功";
    }

    @ShellMethod(value = "导出序列化表数据(.set)")
    public String expSer(String t,String w) throws IOException {
        OracleQuery.exportTableDataAsSerialize(t,w);
        return "导出成功";
    }

    @ShellMethod(value = "导入表数据(.json)")
    public String impJson(String file){
        Path currentPath = Paths.get("");
        File jsonFile = new File(currentPath.toAbsolutePath()+"/"+file+".json");
        OracleQuery.importJsonData(jsonFile);
        return "导入成功";
    }

    @ShellMethod(value = "导入表数据(.ser)")
    public String imp(String file, @ShellOption(defaultValue = "50")int batchMax) throws Exception {
        OracleQuery.importSerDataSingle(file,batchMax);
        return "导入成功";
    }
}
