package config;

public class Config {

    //Postgres connection credentials
    public static String dbURL = "jdbc:postgresql://localhost:5432/gis"; //160.40.63.119
//    public static String dbURL = "jdbc:postgresql://localhost:15432/gis"; //localhost
    public static String uName = "postgres";
    public static String uPass= "password";

    //Postgres tables
    public static String tblTemperature = "_meteo_current_year_temperature";
    public static String tblPrecipitation = "_meteo_current_year_precipitation";
    public static String tblWind = "_meteo_current_year_wind";

}