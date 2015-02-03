/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.agent.bdap;

/**
 *
 * @author apple
 */
import cn.ac.iie.cls.agent.po.FileMessage;
import cn.ac.iie.cls.agent.slave.datacollect.CLSAgentBDAPHandler;
import cn.ac.iie.cls.agent.subThread.bdap.*;
import cn.ac.iie.cls.agent.tools.DateTools;
import cn.ac.iie.cls.agent.tools.XmlTools;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author chen
 * @date 2014-12-26 9:48:09
 */
public class STK6XXDataCooperation {

    private static Logger logger = Logger.getLogger(STKDataCooperation.class);
    private String dataType;
    private String xml;

    public STK6XXDataCooperation(String dataType, String xml) {
        this.dataType = dataType;
        this.xml = xml;
    }
     /*
    execute the 6xxdata handler ,it will execute the sql and upload the result to the hdfs 
    */
    public boolean execute() {       
        if (dataType.equals("")) {
            logger.error("The dataType is null. and add to errFileList.");
            FileMessage efm = new FileMessage();
            efm.setMessage("The xml's dataType is null. and add to errFileList.");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        int timeout = 0;
        try {
            timeout = Integer.parseInt(XmlTools.getValueFromStrDGText(xml, "timeout"));//get value of "timeout";
        } catch (Exception ex) {
            timeout = 10;
            logger.error("time out err!");
        }
        
        String localPath = "";
        String sql = "";
        String instance = "";//database name;
        String url = "";
        int  port = -1;
        String username = "";
        String password = "";
        //String database_type = "";
        String hdfsPath = "";//集群路径
        String operator_id = "";//操作id
        String processjobinstanceId = "";
        String type = "";
        String dstName = "";
        
        try {
            //localPath = PropsFiles.getValue("resultFilePath") + File.separator + dataType; //the master's props;
            localPath = "/home/apple";
            sql = XmlTools.getValueFromStrDGText(xml, "sql");
            instance = XmlTools.getValueFromStrDGText(xml, "instance");
            url = XmlTools.getValueFromStrDGText(xml, "url");
            port = Integer.parseInt(XmlTools.getValueFromStrDGText(xml, "port"));
            username = XmlTools.getValueFromStrDGText(xml, "username");
            password = XmlTools.getValueFromStrDGText(xml, "password");
            //database_type = XmlTools.getValueFromStrDGText(xml, "database_type");
            hdfsPath = XmlTools.getValueFromStrDGText(xml, "hdfsPath");
            operator_id = XmlTools.getOperatorAttribute(xml, "name");            
            processjobinstanceId = XmlTools.getElValueFromStr(xml, "processJobInstanceId");
            type = XmlTools.getOperatorAttribute(xml, "class");
            dstName = XmlTools.getValueFromStrDGText(xml, "dstName");
            
            
        } catch (Exception ex) {
            logger.error(dataType + "read xml err! and add to errFileList." + ex, ex);
            FileMessage efm = new FileMessage();
            efm.setMessage(dataType + "read xml err! and add to errFileList." + ex.getMessage());
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        
        if (localPath.equals("") || hdfsPath.equals("") || operator_id.equals("") || processjobinstanceId.equals("") || type.equals("") ) {
            logger.error(dataType + " localPath or processjobinstanceId or operator_id or type or hdfsPath is empty!");
            FileMessage efm = new FileMessage();
            efm.setMessage(dataType + " localPath or processjobinstanceId or operator_id or type or hdfsPath is empty!");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        
        if (sql.equals("") || instance.equals("")) {
            logger.error(dataType + " the database param sql or instance is empty!");
            FileMessage efm = new FileMessage();
            efm.setMessage(dataType + " the database param sql or instance is empty!");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dates = new Date();
        String sdate = sdf.format(dates);//date format;
        
        logger.info(processjobinstanceId + " sqlQueryThreadt process start: " + sdate);
        SubSQLQueryThreadBDAP sqlQueryThreadBDAP = new SubSQLQueryThreadBDAP(dataType, dstName, localPath, sql, instance, url, port, username, password);
        Thread sqlQueryThread = new Thread(sqlQueryThreadBDAP);
        try {
            sqlQueryThread.start();
            sqlQueryThread.join(timeout*1000);
        }catch (InterruptedException ex) {
            logger.error(dataType + " the sqlQueryThread Thread start failed!");
            FileMessage efm = new FileMessage();
            efm.setMessage(dataType + " the sqlQueryThread Thread start failed!");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        
        boolean flag = sqlQueryThread.isAlive();
         if (flag) {
                logger.info(processjobinstanceId + "the sqlQueryThread has been killed!");
                sqlQueryThread.stop();
                FileMessage efm = new FileMessage();
                efm.setMessage(processjobinstanceId + " time out.----the sqlQueryThread has been killed!");
                CLSAgentBDAPHandler.errFileList.add(efm);
            } else {
                ;
            }
 
        logger.debug(processjobinstanceId + " " + dataType + " path is " + localPath);
        
        try {
            File fileRoot = new File(localPath);
            String[] allFileList = fileRoot.list();
            for (int i = 0; i < allFileList.length; i++) {
                logger.debug("The filename File [" + (i + 1) + "]:" + allFileList[i]);
            }
            Date datee = new Date();
            String edate = sdf.format(datee);
            logger.info(processjobinstanceId + " sqlQueryThread process finish: " + edate);
        } catch (Exception ex) {
            logger.error(dataType + " the sqlQueryThread get resultFile failed!");
            FileMessage efm = new FileMessage();
            efm.setMessage(dataType + " the sqlQueryThread get resultFile failed!");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        
   
        logger.debug(dataType + " hdfs upload start!");
        logger.debug("dataType :" + dataType);
        logger.debug("localPath :" + localPath);
        logger.debug("type :" + type);
        logger.debug("hdfsPath :" + hdfsPath);
        logger.debug("processjobinstanceId :" + processjobinstanceId);        
        logger.debug("operator_id :" + operator_id);
        //logger.debug("xml :" + xml);
        String timeDir = DateTools.getCurrentDate();
        
        File file = new File(localPath);
        if (!file.isDirectory()) {
            logger.error(localPath + " is not a dir!");
            FileMessage efm = new FileMessage();
            efm.setMessage(localPath + " is not a dir!");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        String[] tmp = file.list();
        if(tmp.length == 0) {
            return false;
        }
        List<String> finalFileList = new ArrayList<String>(); 
        for (int i = 0,j =0; i<tmp.length; ++i) {
            if (tmp[i].endsWith(".txt")) {
                finalFileList.add(tmp[i]);
            }
        }
        boolean upflag = false;
        if (!finalFileList.isEmpty()) {
            upflag = new HdfsUploadBDAP(timeout).uploadPoolTask(finalFileList, dstName, localPath, type, hdfsPath, processjobinstanceId, operator_id, timeDir);
        }
        else {
            logger.debug(localPath + ":the local path don't hava files to be uploaded.");
            FileMessage efm = new FileMessage();
            efm.setMessage(localPath + ":the local path don't hava files to be uploaded.");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        if (!upflag) {
            logger.info(processjobinstanceId + "the upSQLResult fail!");
            FileMessage efm = new FileMessage();
            efm.setMessage(processjobinstanceId + "the upSQLResult fail!");
            CLSAgentBDAPHandler.errFileList.add(efm);
            return false;
        }
        else {
            logger.info(processjobinstanceId + "the upSQLResult success!");
            FileMessage efm = new FileMessage();
            efm.setMessage(processjobinstanceId + "the upSQLResult success!");
            CLSAgentBDAPHandler.succFileList.add(efm);
            return true;
        }
    }
}

