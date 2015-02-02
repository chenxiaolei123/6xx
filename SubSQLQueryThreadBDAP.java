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
import cn.ac.iie.cls.agent.tools.DatabaseTools;
import org.apache.log4j.Logger;

public class SubSQLQueryThreadBDAP implements Runnable{
    private static Logger logger = Logger.getLogger(SubSQLQueryThreadBDAP.class);
    private String sql = "";
    private String localPath = "";
    private String instance = "";
    private String name = "";
    private static String database_type = "oracle";
    private static String url = "192.168.11.98";
    private static int port = 1521;
    private static String username = "tdrq";
    private static String password = "tdrq";
    
    public SubSQLQueryThreadBDAP(String name,String localPath, String sql, String instance) {
        this.name = name;
        this.localPath = localPath;
        this.sql = sql;
        this.instance = instance;
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
            connFlag = DatabaseTools.downFile(name, url, port, username, password, localPath, sql, instance, database_type);
            if (connFlag == false) {
                System.out.print("connflag=" + connFlag);
                logger.error(" run SQLQuery is fail!");
                FileMessage efm = new FileMessage();
                efm.setMessage("run SQLQuery is fail!");
//                CLSAgentBDAPHandler.errFileList.add(efm);
            }
            else {
                logger.info("run SQLQuery is success!");
                FileMessage efm = new FileMessage();
                efm.setMessage("run SQLQuery is success!");
                CLSAgentBDAPHandler.errFileList.add(efm);
            }
        }
    }
        
}
