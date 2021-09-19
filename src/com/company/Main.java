package com.company;

import java.sql.*;

public class Main extends Configs {

    static Connection yDbConnection;
    static Connection nDbConnection;
    static Connection fDbConnection;

    public static void main(String[] args) throws SQLException {
        try {
            yDbConnection = getDbConnection(yDBHost, yDBPort, yDBUser, yDBPass, yDBName);
            System.out.println("Успешно подключились к бд " + yDbConnection.getCatalog());
            nDbConnection = getDbConnection(nDBHost, nDBPort, nDBUser, nDBPass, nDBName);
            System.out.println("Успешно подключились к бд " + nDbConnection.getCatalog());
            fDbConnection = getDbConnection(fDBHost, fDBPort, fDBUser, fDBPass, fDBName);
            System.out.println("Успешно подключились к бд " + fDbConnection.getCatalog());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        int replacedPayments = replacePayments();
        System.out.println("Перенесли " + replacedPayments + " покупок");
        int replacedBuyers = replaceBuyers();
        System.out.println("Перенесли " + replacedBuyers + " покупателей");
        yDbConnection.close();
        nDbConnection.close();
        fDbConnection.close();
    }

    public static Connection getDbConnection(String dbHost, String dbPort, String dbUser, String dbPass, String dbName)
            throws SQLException, ClassNotFoundException {

        String stringConnection = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;

        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection dbConnection = DriverManager.getConnection(stringConnection, dbUser, dbPass);

        return dbConnection;
    }

    public static int replacePayments() throws SQLException {
        int replacedPayments = 0;
        String nSelectAllString = "SELECT * FROM AD_PAYMENTS";
        ResultSet nResultSet = nDbConnection.prepareStatement(nSelectAllString).executeQuery();
        while(nResultSet.next()) {
            PreparedStatement nPrSt = yDbConnection.prepareStatement("SELECT * FROM AD_PAYMENTS WHERE " + Const.USERNAME + " = ? AND " + Const.TIME + " = ?");
            nPrSt.setString(1, nResultSet.getString(Const.USERNAME));
            nPrSt.setInt(2, nResultSet.getInt(Const.TIME));
            if (!nPrSt.executeQuery().next()) {
                String replacingString = "INSERT INTO AD_PAYMENTS (" + Const.USERNAME + ", "
                        + Const.DATA + ", "
                        + Const.TIME + ", "
                        + Const.STATUS + ", "
                        + Const.STIME + ", "
                        + Const.SERVER + ", "
                        + Const.LOG + ") VALUES (?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement prSt = yDbConnection.prepareStatement(replacingString);
                prSt.setString(1, nResultSet.getString(Const.USERNAME));
                prSt.setString(2, nResultSet.getString(Const.DATA));
                prSt.setInt(3, nResultSet.getInt(Const.TIME));
                prSt.setInt(4, nResultSet.getInt(Const.STATUS));
                prSt.setInt(5, nResultSet.getInt(Const.STIME));
                prSt.setInt(6, 2);
                prSt.setString(7, nResultSet.getString(Const.LOG));
                prSt.executeUpdate();
                replacedPayments++;
            }
        }
            String fSelectAllString = "SELECT * FROM AD_PAYMENTS";
            ResultSet fResultSet = fDbConnection.prepareStatement(fSelectAllString).executeQuery();
            while (fResultSet.next()) {
                PreparedStatement fPrSt = yDbConnection.prepareStatement("SELECT * FROM AD_PAYMENTS WHERE " + Const.USERNAME + " = ? AND " + Const.TIME + " = ?");
                fPrSt.setString(1, fResultSet.getString(Const.USERNAME));
                fPrSt.setInt(2, fResultSet.getInt(Const.TIME));
                if (!fPrSt.executeQuery().next()) {
                    String replacingString = "INSERT INTO AD_PAYMENTS (" + Const.USERNAME + ", "
                            + Const.DATA + ", "
                            + Const.TIME + ", "
                            + Const.STATUS + ", "
                            + Const.STIME + ", "
                            + Const.SERVER + ", "
                            + Const.LOG + ") VALUES (?, ?, ?, ?, ?, ?, ?)";
                    PreparedStatement prSt = yDbConnection.prepareStatement(replacingString);
                    prSt.setString(1, fResultSet.getString(Const.USERNAME));
                    prSt.setString(2, fResultSet.getString(Const.DATA));
                    prSt.setInt(3, fResultSet.getInt(Const.TIME));
                    prSt.setInt(4, fResultSet.getInt(Const.STATUS));
                    prSt.setInt(5, fResultSet.getInt(Const.STIME));
                    prSt.setInt(6, 3);
                    prSt.setString(7, fResultSet.getString(Const.LOG));
                    prSt.executeUpdate();
                    replacedPayments++;
                }
            }
            return replacedPayments;
        }

    public static int replaceBuyers() throws SQLException {
        int replacedBuyers = 0;
        String nSelectAllString = "SELECT * FROM AD_BUYERS";
        ResultSet nResultSet = nDbConnection.prepareStatement(nSelectAllString).executeQuery();
        while(nResultSet.next()) {
            PreparedStatement nPrSt = yDbConnection.prepareStatement("SELECT * FROM AD_BUYERS WHERE " + Const.USERNAME + " = ? AND " + Const.TIME + " = ?");
            nPrSt.setString(1, nResultSet.getString(Const.USERNAME));
            nPrSt.setInt(2, nResultSet.getInt(Const.TIME));
            if (!nPrSt.executeQuery().next()) {
                String replacingString = "INSERT INTO AD_BUYERS (" + Const.USERNAME + ", "
                        + Const.GOOD + ", "
                        + Const.TIME + ", "
                        + Const.EXP_TIME + ", "
                        + Const.COST + ", "
                        + Const.SERVER + ") VALUES (?, ?, ?, ?, ?, ?)";
                PreparedStatement prSt = yDbConnection.prepareStatement(replacingString);
                prSt.setString(1, nResultSet.getString(Const.USERNAME));
                prSt.setInt(2, nResultSet.getInt(Const.GOOD));
                prSt.setInt(3, nResultSet.getInt(Const.TIME));
                prSt.setInt(4, nResultSet.getInt(Const.EXP_TIME));
                prSt.setInt(5, nResultSet.getInt(Const.COST));
                prSt.setInt(6, 2);
                prSt.executeUpdate();
                replacedBuyers++;
            }
        }
        String fSelectAllString = "SELECT * FROM AD_BUYERS";
        ResultSet fResultSet = fDbConnection.prepareStatement(fSelectAllString).executeQuery();
        while(fResultSet.next()) {
            PreparedStatement fPrSt = yDbConnection.prepareStatement("SELECT * FROM AD_BUYERS WHERE " + Const.USERNAME + " = ? AND " + Const.TIME + " = ?");
            fPrSt.setString(1, fResultSet.getString(Const.USERNAME));
            fPrSt.setInt(2, fResultSet.getInt(Const.TIME));
            if (!fPrSt.executeQuery().next()) {
                String replacingString = "INSERT INTO AD_BUYERS (" + Const.USERNAME + ", "
                        + Const.GOOD + ", "
                        + Const.TIME + ", "
                        + Const.EXP_TIME + ", "
                        + Const.COST + ", "
                        + Const.SERVER + ") VALUES (?, ?, ?, ?, ?, ?)";
                PreparedStatement prSt = yDbConnection.prepareStatement(replacingString);
                prSt.setString(1, fResultSet.getString(Const.USERNAME));
                prSt.setInt(2, fResultSet.getInt(Const.GOOD));
                prSt.setInt(3, fResultSet.getInt(Const.TIME));
                prSt.setInt(4, fResultSet.getInt(Const.EXP_TIME));
                prSt.setInt(5, fResultSet.getInt(Const.COST));
                prSt.setInt(6, 3);
                prSt.executeUpdate();
                replacedBuyers++;
            }
        }
        return replacedBuyers;
    }
}