/**
 * Created by pmalasio on 19/2/2016.
 *
 * Parsing 4 html forecast files with 4 distinct metrics and filling in the
 * db tables _forecast_temperature, _forecast_capacitation, _forecast_precipitation and _forecast_wind
 */

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;



public class ForecastParser {

    static Properties props = new Properties();
    //static String propertiesPath = "D:\\\\public\\\\forecast\\\\properties\\\\forecast.properties";
    //static String propertiesPath = "/home/pmalasio/forecastWebdev4.properties";
    static String propertiesPath = "/home/userdecide/forecast.properties";



    public static void main(String[] args) throws SQLException, IOException {

        // 1) Establish FTP connection and get files locally
        if (ftpFetchFiles(propertiesPath)){

            props.load(new FileInputStream(propertiesPath));

            // 2) Parse HTML files with jsoup
            File folder = new File(props.getProperty("localDirectory"));
            File[] listOfFiles = folder.listFiles();

                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        System.out.println("Parsing files----> " + file.getName());
                        ArrayList<String> data = jsoupParse(file);
                        if (!data.isEmpty()) { // if there is data to return

                            // connect to database
                            ForecastParser fp = new ForecastParser();
                            Connection postgresConnection = null;

                            try {
                                postgresConnection = fp.postgresConnection();
                                postgresConnection.setAutoCommit(false);
                                //
                                String matchFileName = file.getName().substring(file.getName().indexOf("IMET"), file.getName().lastIndexOf("_"));
                                props.load(new FileInputStream(propertiesPath));
                                // System.out.println(matchFileName);
                                String db_table = props.getProperty(matchFileName);

                                // Insert data
                                fp.insertToPostgres(postgresConnection, db_table, file.getName().substring(file.getName().lastIndexOf("_") + 1, file.getName().lastIndexOf(".")), data);
                                //
                                postgresConnection.commit();
                                postgresConnection.close();
                            } catch(Exception s){
                                s.printStackTrace();
                                try {
                                    if (postgresConnection!=null){
                                        postgresConnection.rollback();
                                        postgresConnection.close();
                                    }
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }

                        } // End of inserting data
                    }
                }
        }
    }


    public static ArrayList<String> jsoupParse(File input) {
        ArrayList<String> data = new ArrayList<>();

        ArrayList<String> hoursHr = new ArrayList<String>();

        try {
            Document doc = Jsoup.parse(input, "UTF-8");

            // Headers
            Elements head = doc.select("thead");
            for (Element e: head.select("th")){
                if (e.hasText()) {
                    String hr = e.unwrap().toString();
                    hoursHr.add(hr.substring(0, hr.indexOf(" ")));
                }
            }


            // Body
            Elements elements = doc.select("tbody");
            ArrayList<String> stations = new ArrayList<String>();
            System.out.println("Number of stations: " + elements.select("th").size());
            for (Element e: elements.select("th")){
                stations.add(e.unwrap().toString());
            }

            for (Element table : doc.select("body")) {
                int cnt = -1;

                for (Element row : table.select("tr")) {
                    Elements tds = row.select("td");

//                    System.out.println("--- NEW ROW -->" + cnt);
                    int hours = 0;
                    int hrCnt = 0;
                    for (Element col : tds){
                        String value = col.unwrap().toString();
                        data.add(stations.get(cnt) + "#" + value+ "#" + hoursHr.get(hrCnt));
                        hours+=3;
                        hrCnt++;
                    }
                    cnt++;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("DATA to insert: " + data);
        return data;
    }

    public static boolean ftpFetchFiles(String properties){

        boolean updatedFiles = false;

        try {
            props.load(new FileInputStream(properties));

            String serverAddress = props.getProperty("serverAddress").trim();
            String userId = props.getProperty("userId").trim();
            String password = props.getProperty("password").trim();
            String remoteDirectory = props.getProperty("remoteDirectory").trim();
            String localDirectory = props.getProperty("localDirectory").trim();
            System.out.println(remoteDirectory);

            // Clean Local Directory from previous versions of files
            File dir = new File(localDirectory);
            for (File f: dir.listFiles()) {
                if (!f.isDirectory()) f.delete();
            }

            //new ftp client
            FTPClient ftp = new FTPClient();
            //try to connect
            ftp.connect(serverAddress);
            //login to server
            if(!ftp.login(userId, password))
            {
                System.out.println("did not login");
                ftp.logout();
            }
            int reply = ftp.getReplyCode();
            //FTPReply stores a set of constants for FTP reply codes.
            if (!FTPReply.isPositiveCompletion(reply))
            {
                ftp.disconnect();
                System.out.println("disconnected");
            }
            //enter passive mode
            ftp.enterLocalPassiveMode();
            //get system name
            //System.out.println("Remote system is " + ftp.getSystemName());
            //change current directory
            ftp.changeWorkingDirectory(remoteDirectory);
            System.out.println("Current directory is " + ftp.printWorkingDirectory());


            //get list of filenames
            FTPFile[] ftpFiles = ftp.listFiles();

            if (ftpFiles != null && ftpFiles.length > 0) {
                //loop thru files
                for (FTPFile file : ftpFiles) {
                    if (!file.isFile()) {
                        continue;
                    }
                    System.out.println("File is " + file.getName());

                    //get output stream
                    OutputStream output;
                    output = new FileOutputStream(localDirectory + "/" + file.getName());
                    //get the file from the remote system
                    ftp.retrieveFile(file.getName(), output);
                    //close output stream
                    output.close();
                    updatedFiles = true;
                }
            }

            ftp.logout();
            ftp.disconnect();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return updatedFiles;
    }

    public static Connection postgresConnection() throws SQLException, IOException {

        props.load(new FileInputStream(propertiesPath));

        Connection postgresConnection = null;
        Properties postgresConnectionProps = new Properties();
        postgresConnectionProps.put("user", props.getProperty("dbuser").trim());
        postgresConnectionProps.put("password", props.getProperty("dbpassword").trim());

        postgresConnection = DriverManager.getConnection(props.getProperty("driver").trim(), postgresConnectionProps);

        System.out.println("\nConnected to postgres database successfully");
        return postgresConnection;
    }

    public static void insertToPostgres(Connection conn, String db_table, String date, ArrayList<String> data) throws SQLException{

        Map<Integer, String> hoursMap = new HashMap<>();

        // Day 0
        hoursMap.put(0, "12 (0)");
        hoursMap.put(3, "15 (0)");
        hoursMap.put(6, "18 (0)");
        hoursMap.put(9, "21 (0)");
        hoursMap.put(12,"24 (0)");
        hoursMap.put(15,"3 (0)");
        // Day 1
        hoursMap.put(18,"6 (1)");
        hoursMap.put(21,"9 (1)");
        hoursMap.put(24, "12 (1)");
        hoursMap.put(27, "15 (1)");
        hoursMap.put(30, "18 (1)");
        hoursMap.put(33, "21 (1)");
        // Day 2
        hoursMap.put(36, "24 (2)");
        hoursMap.put(39, "3 (2)");
        hoursMap.put(42, "6 (2)");
        hoursMap.put(45, "9 (2)");
        hoursMap.put(48, "12 (2)");
        hoursMap.put(51, "15 (2)");
        hoursMap.put(54, "18 (2)");
        hoursMap.put(57, "21 (2)");
        // Day 3
        hoursMap.put(60, "24 (3)");
        hoursMap.put(63, "3 (3)");
        hoursMap.put(66, "6 (3)");
        hoursMap.put(69, "9 (3)");
        hoursMap.put(72, "12 (3)");

        System.out.println("db_table: " + db_table);
        // First truncate table
        String truncate = "TRUNCATE TABLE " + db_table;
        PreparedStatement t = conn.prepareStatement(truncate);
        t.executeUpdate();

        String query;
        if (db_table.contains("wind")){
           query  = "INSERT INTO " + db_table + " (station, value_degrees, value_speed, date, hours, hours_mapping) values (?, ?, ?, ?, ?, ?) ";
        }
        else {
           query = "INSERT INTO " + db_table + " (station, value, date, hours, hours_mapping) values (?, ?, ?, ?, ?) ";

        }
        PreparedStatement p = conn.prepareStatement(query);

        // Insert Data into clean table
          for (String d: data){
            //System.out.println(d);
            p.setString(1, d.substring(0, d.indexOf("#")));
            // Change value per case
            if (db_table.contains("wind")) {
                Double degrees = Double.parseDouble(d.substring(d.indexOf("#") + 1, d.indexOf("°")));
                Double speed = Double.parseDouble(d.substring(d.indexOf("°") + 1, d.lastIndexOf(" ")));
                p.setDouble(2, degrees);
                p.setDouble(3, speed);
                p.setString(4, date);
                p.setInt(5, Integer.parseInt(d.substring(d.lastIndexOf("#")+1)));
                p.setString(6, hoursMap.get(Integer.parseInt(d.substring(d.lastIndexOf("#")+1))));
            }
            else {
                Double v;
                if (d.contains("N/A"))
                    v = 0.0;
                else {
                    String value = d.substring(d.indexOf("#") + 1, d.lastIndexOf(" "));
                    v=Double.parseDouble(value);
                }
                p.setDouble(2, v);
                p.setString(3, date);
                p.setInt(4, Integer.parseInt(d.substring(d.lastIndexOf("#")+1)));
                p.setString(5, hoursMap.get(Integer.parseInt(d.substring(d.lastIndexOf("#")+1))));
            }

            int success = p.executeUpdate();
            // System.out.println("was successfully inserted: " + success);

            if (db_table.contains("temperature")){ // for thessaloniki
                PreparedStatement update = conn.prepareStatement("UPDATE _forecast_temperature SET station = ? WHERE station = ?");
                update.setString(1, "THESSALONIKI KENTRO-DIMOS");
                update.setString(2, "THESSALONIKI (AIRPORT)");
                update.executeUpdate();
            }
        }

    }

}