/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.agent.subThread.bdap;

/**
 *
 * @author apple
 */
import cn.ac.iie.cls.agent.po.FileMessage;
import cn.ac.iie.cls.agent.slave.datacollect.CLSAgentBDAPHandler;
//import cn.ac.iie.cls.agent.tools.DatabaseTools;
import org.apache.log4j.Logger;
import cn.ac.iie.cls.agent.tools.DatabaseToolsBDAP;
import java.sql.Connection;

public class SubSQLQueryThreadBDAP implements Runnable{
    private static Logger logger = Logger.getLogger(SubSQLQueryThreadBDAP.class);
    private String sql = "";
    private String localPath = "";
    private String instance = "";
    private String data_type = "";
    //private static String database_type = "oracle";
    private  String url = "";
    private  int port = -1;
    private  String username = "";
    private  String password = "";
    
    public SubSQLQueryThreadBDAP(String data_type, String localPath, String sql, String instance, 
            String url, int port, String username, String password) {
        this.data_type = data_type;
        this.localPath = localPath;
        this.sql = sql;
        this.instance = instance;
        this.url = url;
        this.port = port;
        this.username = username;
        this.password = password;
        
        //this.database_type = database_type;
    }
    
    @Override
    public void run() {
        
        if (url.equals("") || username.equals("")) {
            logger.error(" url or username is empty!");
            FileMessage efm = new FileMessage();
            efm.setMessage("url or username is empty!");
            CLSAgentBDAPHandler.errFileList.add(efm);
        }
        else {
            logger.debug("sqlQuery is starting!");
            boolean connFlag = false;
            //DatabaseTools databaseQuery = new DatabaseTools();
            //connFlag = DatabaseTools.downFile(name, url, port, username, password, localPath, sql, instance, database_type);
            DatabaseToolsBDAP.init(url, port, password);
            Connection conn = DatabaseToolsBDAP.getLbConnection(instance, username);
            if (conn == null) {
                logger.error(" Database Connection is fail!");
                FileMessage efm = new FileMessage();
                efm.setMessage("Database Connection is fail!");
                CLSAgentBDAPHandler.errFileList.add(efm);
            }
            else {
                connFlag = DatabaseToolsBDAP.query(data_type, sql, localPath, conn);
                if (connFlag == false) {
                //System.out.print("connflag=" + connFlag);
                    logger.error(" run SQLQuery is fail!");
                    FileMessage efm = new FileMessage();
                    efm.setMessage("run SQLQuery is fail!");
                    //CLSAgentBDAPHandler.errFileList.add(efm);
                }
                else {
                    logger.info("run SQLQuery is success!");
                    FileMessage efm = new FileMessage();
                    efm.setMessage("run SQLQuery is success!");
                //CLSAgentBDAPHandler.errFileList.add(efm);
                }
            }
        }
    }
        
}
