/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.agent.tools;

import cn.ac.iie.cls.agent.controller.Controller;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
/**
 *
 * @author apple
 */
public class DatabaseToolsBDAP {
    private static Logger logger = Logger.getLogger(DatabaseTools.class);
    private static String lbUrl = "";
    private static String lbPassword = "";
    
    /** Init the database*/
    public static void init(String url, int port, String password){
        lbUrl = "jdbc.iie.DBroker:@" + url + ":" + port + ":";
        lbPassword = password;
        logger.debug("database init success!");
    }
    /** get connection with the database*/
    public static Connection getLbConnection(String serviceName, String lbUser) {
        Connection lbConn = null;
        try {
            String driverName = "cn.ac.iie.jdbc.DBrokerI.DBrokerDriver";
            Class.forName(driverName);
            DriverManager.setLoginTimeout(100);
            String url = lbUrl + serviceName.trim();
            lbConn = DriverManager.getConnection(url, lbUser, lbPassword);
            logger.debug("get connection success.");
        } catch (ClassNotFoundException ex) {
            logger.error("can't find the class");
            ex.printStackTrace();
            return null;
        } catch (SQLException ex) {
            logger.error("the database connection is fail");
            ex.printStackTrace();
            return null;
        }
        return lbConn;
    }
    /** query the sql and create a local file to store the result*/
    public static boolean query(String data_type, String sql, String localPath, String dstName, Connection conn){
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean connFlag = true;
        try {
            //start query the sql
            ps = conn.prepareStatement(sql.toString());
            rs = ps.executeQuery();
                        
            //create a local path
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mmss");
            String times = sdf.format(new Date());
            String path = localPath + File.separator + data_type + "_" + times + "_" + dstName+  ".txt";
            
            //create a local file to store the result
            File file = new File(localPath);
            if (!file.exists() || !file.isDirectory()) {
                file.mkdir();
            }
            FileOutputStream fos = new FileOutputStream(path);
            OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
            ResultSetMetaData rsmd = rs.getMetaData();
            int numCols = rsmd.getColumnCount();
            if (true) {//if add colum name
                for (int i = 1; i <= numCols; i++) {
                    if (i > 1) {
                            osw.write("|");
                    }
                    osw.write(rsmd.getColumnLabel(i));
                }
                osw.write("\r\n");
            }
            while (rs.next()) {
                for (int i = 1; i <= numCols; i++) {
                    if (i > 1) {
                        osw.write("|");
                    }
                    String str = rs.getString(i);
                    osw.write(str);
                }
                osw.write("\r\n");
            }
            osw.flush();
            osw.close();
        } catch (IOException ex) {
                logger.debug(lbUrl + " The file writen error!" + ex, ex);
                connFlag = false;
        } catch (SQLException ex) {
                logger.debug(lbUrl + " Get connection  error!" + ex, ex);
                System.out.print(ex);
                connFlag = false;
        } finally {
            try {
                DBUtil.close(rs, ps, conn);
            } catch (SQLException ex) {
                    logger.debug(lbUrl + " Close the connection  error!" + ex, ex);
                    connFlag = false;
                }
            }
        return connFlag;
    }
}
