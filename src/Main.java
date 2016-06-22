import config.Config;

import java.net.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //connect to db
        String JDBC_DRIVER = "org.postgresql.Driver";
        Class.forName("org.postgresql.Driver");

        Connection con = null;
        Statement stmt = null;
        try {
            con = DriverManager.getConnection(Config.dbURL, Config.uName, Config.uPass);
            con.setAutoCommit(false);
            stmt = con.createStatement();

            //truncate old data
            stmt.executeUpdate("TRUNCATE " + Config.tblTemperature);
            con.commit();
            stmt.executeUpdate("TRUNCATE " + Config.tblPrecipitation);
            con.commit();
            stmt.executeUpdate("TRUNCATE " + Config.tblWind);
            con.commit();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        //data to be retrieved
        List<String> urls = new ArrayList<String>();
        urls.add("http://penteli.meteo.gr/stations/amyntaio/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/thessaloniki/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/serres/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/drama/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/xanthi/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/veroia/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/giannitsa/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/grevena/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/florina/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/ptolemaida/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/kastoria/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/orestiada/NOAAYR.txt");
        urls.add("http://penteli.meteo.gr/stations/rizomata/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/mavropigi/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/ardassa/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/vlasti/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/variko/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/seli/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/veroia/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/vegoritida/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/kerasia/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/kaimaktsalan/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/3-5pigadia/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/dion/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/sindos/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/neamichaniona/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/polygyros/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/kassandreia/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/kerkini/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/lagadas/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/nevrokopi/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/mikrokampos/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/fotolivos/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/paranesti/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/neaperamos/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/thasos/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/alexandroupolis/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/didymoteicho/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/metaxades/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/notiopedio/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/vlasti/NOAAYR.TXT");
        urls.add("http://penteli.meteo.gr/stations/eleftheroupoli/NOAAYR.TXT");

        for(String url : urls) {

            System.out.println(url);

            //get station
            String station = url.split("\\/+")[3];

            //get data
            URL oracle = new URL(url);
            URLConnection yc = oracle.openConnection();
            BufferedReader br = new BufferedReader(new InputStreamReader(yc.getInputStream()));

            //get current month
            Calendar now = Calendar.getInstance();
            int currentMonth = now.get(Calendar.MONTH) + 1;

            //parse data
            boolean flagTemp = true, flagPrecipitation = true, flagWind = true;
            boolean temp = false, precipitation = false, wind = false;

            Map<Integer, String> idFieldTemp = new HashMap<Integer, String>();
            Map<Integer, String> idFieldPrecipitation = new HashMap<Integer, String>();
            Map<Integer, String> idFieldWind = new HashMap<Integer, String>();

            //get temperature data
            for (String next = "", line = br.readLine(); line != null; line = next) {
                next = br.readLine();

                if (next.startsWith("-----------") && flagTemp) {
                    int i = 1;
                    for (String txt : line.trim().split("\\s+")) {
                        idFieldTemp.put(i, txt);
                        i++;
                    }
                    flagTemp = false;
                }

                if (temp) {

                    if (!line.startsWith("-----------")) {
                        int idMonth = getKeyByValue(idFieldTemp, "MO");

                        if (currentMonth >= Integer.parseInt(line.split("\\s+")[idMonth])) {
                            int idYear = getKeyByValue(idFieldTemp, "YR");
                            int idMeanMax = getKeyByValue(idFieldTemp, "MAX");
                            int idMeanMin = getKeyByValue(idFieldTemp, "MIN");
                            int idMean = getKeyByValue(idFieldTemp, "MEAN");

                            //insert to db
                            try {
                                String sql = "INSERT INTO _meteo_current_year_temperature VALUES ('"+ line.split("\\s+")[idYear] +"', '"+ line.split("\\s+")[idMonth] +"', '"+ line.split("\\s+")[idMeanMax] +"', '"+ line.split("\\s+")[idMeanMin] +"', '"+ line.split("\\s+")[idMean] +"', '"+station+"')";
                                stmt.executeUpdate(sql);
                                con.commit();
                            } catch (Exception e) {
                                System.err.println(e.getMessage());
                            }
                        }
                    } else {
                        break;
                    }

                }

                if (line.startsWith("-----------")) {
                    temp = true;
                }
            }

            //get precipitation data
            int cnt = 0;

            yc = oracle.openConnection();
            br = new BufferedReader(new InputStreamReader(yc.getInputStream()));

            for (String next = "", line = br.readLine(); line != null; line = next) {
                next = br.readLine();

                if (next.startsWith("-----------")){ cnt++; }

                if (next.startsWith("-----------") && flagPrecipitation && cnt == 3) {
                    int i = 1;
                    for (String txt : line.trim().split("\\s+")) {
                        idFieldPrecipitation.put(i, txt);
                        i++;
                    }
                    flagPrecipitation = false;
                }

                if (precipitation) {

                    if (!line.startsWith("-----------")) {
                        int idMonth = getKeyByValue(idFieldPrecipitation, "MO");

                        if (currentMonth >= Integer.parseInt(line.split("\\s+")[idMonth])) {
                            int idYear = getKeyByValue(idFieldPrecipitation, "YR");
                            int idTotal = getKeyByValue(idFieldPrecipitation, "TOTAL");
                            int idMaxObsDay = getKeyByValue(idFieldPrecipitation, "DAY");
                            int idDate = getKeyByValue(idFieldPrecipitation, "DATE");
                            int idF2 = getKeyByValue(idFieldPrecipitation, ".2");
                            int idM2 = getKeyByValue(idFieldPrecipitation, "2");
                            int idL2 = getKeyByValue(idFieldPrecipitation, "20");

                            //insert to db
                            try {
                                String sql = "INSERT INTO _meteo_current_year_precipitation VALUES ('"+ line.split("\\s+")[idYear] +"', '"+ line.split("\\s+")[idMonth] +"', '"+ line.split("\\s+")[idTotal] +"', '"+ line.split("\\s+")[idMaxObsDay] +"', '"+ line.split("\\s+")[idDate] +"', '"+ line.split("\\s+")[idF2] +"', '"+ line.split("\\s+")[idM2] +"', '"+ line.split("\\s+")[idL2] +"', '"+station+"')";
                                stmt.executeUpdate(sql);
                                con.commit();
                            } catch (Exception e) {
                                System.err.println(e.getMessage());
                            }
                        }
                    } else {
                        break;
                    }

                }

                if (line.startsWith("-----------") && cnt == 3) {
                    precipitation = true;
                }
            }

            //get wind data
            int cntW = 0;

            yc = oracle.openConnection();
            br = new BufferedReader(new InputStreamReader(yc.getInputStream()));

            for (String next = "", line = br.readLine(); line != null; line = next) {
                next = br.readLine();

                if (next.startsWith("-----------")){ cntW++;}

                if (next.startsWith("-----------") && flagWind && cntW == 5) {
                    int i = 1;
                    for (String txt : line.trim().split("\\s+")) {
                        idFieldWind.put(i, txt);
                        i++;
                    }
                    flagWind = false;
                }

                if (wind) {

                    if (!line.startsWith("-----------")) {
                        int idMonth = getKeyByValue(idFieldWind, "MO");

                        if (currentMonth >= Integer.parseInt(line.split("\\s+")[idMonth])) {
                            int idYear = getKeyByValue(idFieldWind, "YR");
                            int idAvg = getKeyByValue(idFieldWind, "AVG.");
                            int idDir = getKeyByValue(idFieldWind, "DIR");

                            //insert to db
                            try {
                                String sql = "INSERT INTO _meteo_current_year_wind VALUES ('"+ line.split("\\s+")[idYear] +"', '"+ line.split("\\s+")[idMonth] +"', '"+ line.split("\\s+")[idAvg] +"', '"+ line.split("\\s+")[idDir] +"', '"+station+"')";
                                stmt.executeUpdate(sql);
                                con.commit();
                            } catch (Exception e) {
                                System.err.println(e.getMessage());
                            }
                        }
                    } else {
                        break;
                    }

                }

                if (line.startsWith("-----------") && cntW == 5) {
                    wind = true;
                }

            }

            br.close();
        }

        try {
            stmt.close();
            con.close();

            System.out.println("\n--- data updated ---");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
        for (Map.Entry<T, E> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }
}