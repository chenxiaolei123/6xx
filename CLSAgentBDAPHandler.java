/*
 * this class have link with BDAPXmlResponse(which collect err/all/succFileList  information then return the repose to this class)
 * and the class also deal two kind of type.One is GM.E.CoAgent3 which link to STKDataCooperation) and another is GM.E.CoAgent7 which is link to Putxml7xx and DownloadData7xx
 */
package cn.ac.iie.cls.agent.slave.datacollect;

import cn.ac.iie.cls.agent.bdap.STKDataCooperation;
import cn.ac.iie.cls.agent.bdap.STK6XXDataCooperation;
import cn.ac.iie.cls.agent.po.FileMessage;
import cn.ac.iie.cls.agent.slave.SlaveHandler;
import cn.ac.iie.cls.agent.subThread.bdap.BDAPXmlResponse;
import cn.ac.iie.cls.agent.subThread.bdap.DownloadData7xx;
import cn.ac.iie.cls.agent.subThread.bdap.Putxml7xx;
import cn.ac.iie.cls.agent.tools.XmlTools;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;

/**
 *
 * @author G
 * @date 2014-12-26 9:31:45
 */
public class CLSAgentBDAPHandler implements SlaveHandler {

    private Logger logger = Logger.getLogger(CLSAgentBDAPHandler.class);//简单地说就是初始化Log4j的一个实例，让这个实例在以后的打印中，题头都带上你的类名！
    public static final String TASK_ERROR_FILE_FLAG_FOR7XX = "e_r_r_o_r";
    public static final String TASK_NUMS_FILE_FLAG_FOR7XX = "partnums";
    public static BlockingQueue<FileMessage> errFileList = null;//BlockingQueue队列
    public static BlockingQueue<FileMessage> allFileList = null;
    public static BlockingQueue<FileMessage> succFileList = null;

    @Override
    public String execute(String pRequestContent) { //执行
        String xml;
        logger.info("bdap...The agent received the request.");
        logger.info("the request content is:\n" + pRequestContent);
//        this.xml = pRequestContent;
        xml = pRequestContent;
        String response = "";

        String responseallStr = "";
        responseallStr = "<all>"
                + "<resultUri>"
                + "###resultFileHdfsPath"
                + "</resultUri>"
                + "</all>";
        String responseSuccessStr = "";
        responseSuccessStr = "<success>"
                + "<resultUri>"
                + "###resultFileHdfsPath"
                + "</resultUri>"
                + "</success>";
        String responseErrStr = "";
        responseErrStr = "<error>"
                + "<message>###errInfo</message>"
                + "<resultUri>"
                + "###errFile"
                + "</resultUri>"
                + "</error>";
        if (xml.equals("")) {   //all choose need add message to FileList ,and then info response to logger;
            logger.info("xml is empty! and add to errFileList.");
            FileMessage efm = new FileMessage();
            efm.setMessage("xml is empty! and add to errFileList.");
            CLSAgentBDAPHandler.errFileList.add(efm);
            BDAPXmlResponse br = new BDAPXmlResponse(responseallStr, responseSuccessStr, responseErrStr);//parameter isn't change;
            response = br.createRsponse();  //the  response  depend on the BlockingQueue
            logger.info("The response is:\n" + response);
            return response;
        }

        String processjobinstanceId = "";
        try {
            String dataType = "";
            dataType = XmlTools.getValueFromStrDGText(xml, "dataType");//value eg datatype = ".txt"

            String type = "";
            type = XmlTools.getOperatorAttribute(xml, "class");//operator shuxing
            System.out.print(type + dataType);
            processjobinstanceId = XmlTools.getElValueFromStr(xml, "processJobInstanceId");

            if (type.equals("") || dataType.equals("") || xml.equals("")) {
                FileMessage efm = new FileMessage();
                efm.setMessage("client thread start err." + "\tdataType=" + dataType + "\ttype=" + type);
                CLSAgentBDAPHandler.errFileList.add(efm);
                BDAPXmlResponse br = new BDAPXmlResponse(responseallStr, responseSuccessStr, responseErrStr);
                response = br.createRsponse();
                logger.info("The response is:\n" + response);
                return response;
            }
            logger.info(type + "\t" + dataType);
//two kind of  type
            if (type.equals("GM.E.CoAgent3")) {//3XX数据接入

                errFileList = new LinkedBlockingQueue<FileMessage>();
                allFileList = new LinkedBlockingQueue<FileMessage>();
                succFileList = new LinkedBlockingQueue<FileMessage>();

                logger.info(processjobinstanceId + "\tGM.E.CoAgent3 is start!");
                try {
                    logger.info(processjobinstanceId + " " + dataType + " starting!");

                    STKDataCooperation sdc = new STKDataCooperation(dataType, xml);//transmit the datatype file
                    boolean reFlag = sdc.execute();
                    if (reFlag) {
                        logger.info(processjobinstanceId + "\texecute the task finished...\tGM.E.CoAgent3");// instance meaning situation
                    } else {
                        logger.info(processjobinstanceId + "\texecute the task error...\tGM.E.CoAgent3");
                    }

                } catch (Exception ex) {
                    logger.info(processjobinstanceId + " " + "start GM.E.CoAgent3 error. " + "\t" + ex.getLocalizedMessage() + "\t" + ex.getMessage() + "\t" + ex.getStackTrace(), ex);
                    FileMessage efm = new FileMessage();
                    efm.setMessage(processjobinstanceId + " " + "start GM.E.CoAgent3 error. " + "\t" + ex.getLocalizedMessage() + "\t" + ex.getMessage());
                    CLSAgentBDAPHandler.errFileList.add(efm);
                }
            } else if (type.equals("GM.E.CoAgent7")) {//7XX数据接入
                logger.info(processjobinstanceId + "GM.E.CoAgent7 is start!");
                errFileList = new LinkedBlockingQueue<FileMessage>();  //here the init should not recur 
                allFileList = new LinkedBlockingQueue<FileMessage>();
                succFileList = new LinkedBlockingQueue<FileMessage>();

                String localSQLFilePath = "/mnt/disk11/iie/data/send";
                String localSQLResultFilePath = "/mnt/disk11/iie/data/recv/";

                try {
                    logger.info(processjobinstanceId + " " + dataType + " starting!");
                    boolean putflag = new Putxml7xx(dataType, xml, localSQLFilePath).run();//create a sql file wait for execute;
                    if (putflag) {
                        logger.info(processjobinstanceId + "\texecute the task start...\tGM.E.CoAgent7 -- create xml file.");
                        DownloadData7xx dd7 = new DownloadData7xx(dataType, xml, localSQLResultFilePath, false);
                        boolean reFlag = dd7.run();  //execute the sql file;
                        if (reFlag) {
                            logger.info(processjobinstanceId + "\texecute the task finished...\tGM.E.CoAgent7");
                        } else {
                            logger.info(processjobinstanceId + "\texecute the task error...\tGM.E.CoAgent7");
                        }
//                        logger.info("execute the task end...\tGetherDataFrom7XXPutXML -- put result file.");
                    }
                } catch (Exception ex) {
                    logger.error(processjobinstanceId + " " + "start GM.E.CoAgent7 error. " + "\t" + ex.getLocalizedMessage() + "\t" + ex.getMessage() + "\t" + ex.getStackTrace(), ex);
                    FileMessage efm = new FileMessage();
                    efm.setMessage(processjobinstanceId + " " + "start GM.E.CoAgent7 error. " + "\t" + ex.getLocalizedMessage() + "\t" + ex.getMessage());
                    CLSAgentBDAPHandler.errFileList.add(efm);
                }
                logger.info("GetherDataFrom7XXPutXML is end!");
            } else if (type.equals("GM.E.CoAgent6")) {//6XX数据接入
                errFileList = new LinkedBlockingQueue<FileMessage>();
                allFileList = new LinkedBlockingQueue<FileMessage>();
                succFileList = new LinkedBlockingQueue<FileMessage>();

                logger.info(processjobinstanceId + "\tGM.E.CoAgent6 is start!");
                try {
                    logger.info(processjobinstanceId + " " + dataType + " starting!");
                    
                    STK6XXDataCooperation sdc = new STK6XXDataCooperation(dataType, xml);
                    boolean reFlag = sdc.execute();
                    if (reFlag) {
                        logger.info(processjobinstanceId + "\texecute the task finished...\tGM.E.CoAgent6");// 
                    } else {
                        logger.info(processjobinstanceId + "\texecute the task error...\tGM.E.CoAgent6");
                    }

                } catch (Exception ex) {
                    logger.info(processjobinstanceId + " " + "start GM.E.CoAgent6 error. " + "\t" + ex.getLocalizedMessage() + "\t" + ex.getMessage() + "\t" + ex.getStackTrace(), ex);
                    FileMessage efm = new FileMessage();
                    efm.setMessage(processjobinstanceId + " " + "start GM.E.CoAgent6 error. " + "\t" + ex.getLocalizedMessage() + "\t" + ex.getMessage());
                    CLSAgentBDAPHandler.errFileList.add(efm);
                }
            }
        } finally {
            //TODO ERROR
            BDAPXmlResponse br = new BDAPXmlResponse(responseallStr, responseSuccessStr, responseErrStr);
            response = br.createRsponse();

            errFileList = null;
            succFileList = null;
            allFileList = null;
        }

        //response
        logger.info(processjobinstanceId + "\tThe response is:\n" + response);
        return response;
    }
}
