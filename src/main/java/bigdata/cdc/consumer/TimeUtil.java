package bigdata.cdc.consumer;


public class TimeUtil {
    /**
     * @param value
     * @return
     * @desc time format such as  2020-08-15 14:23:45.02+08   2020-09-14 10:03:40+08
     */
    public static String timeFormat(String value) {
        String resultV = "";
        try {
            //2020-09-07 13:49:30.671+08
            if (value.contains("+08") && value.contains(".")) {
                resultV = value.split("\\.")[0];
            } else if (value.contains("+08") && !value.contains(".")) {
                resultV = value.split("\\+")[0];
            } else if (value.contains("\\")) {
                resultV = value.replaceAll("\\\\", "");
            } else {
                resultV = value;
            }
        } catch (Exception e) {
            System.out.println("TimeUtil工具解析到的时间" + value);
            resultV = value;
        }
        return resultV;
    }

    public static void main(String[] args) {
        System.out.println(timeFormat("9.5"));
    }

}
