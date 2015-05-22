package com.qunar.dba;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Created by loushuai on 5/20/15.
 */
public class QHBaseCompact {

    public QHBaseCompact() {
    }

    public static String KEY_STARTTIME = "starttime";
    public static String KEY_ENDTIME = "endtime";
    public static String KEY_TABLENAME = "tablename";
    public static String KEY_SIZE = "marjorfilesize";
    private static String starttime;
    private static String endtime;
    private static String tablename;
    private static long majorcompactsize;
    private static Logger logger;
    private static Configuration hconf;

    /**
     * wait until time comes
     */
    private static void waitTime() {
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
        String curdate = formatter.format(new java.util.Date());

        while (curdate.compareTo(starttime) < 0 || curdate.compareTo(endtime) > 0) {
            try {
                Thread.sleep(60 * 1000);
                curdate = formatter.format(new java.util.Date());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void quickPoll(Callable<Boolean> c, int waitMs) throws Exception {
        int sleepMs = 10;
        int retries = (int) Math.ceil(((double) waitMs) / sleepMs);
        while (retries-- > 0) {
            if (c.call().booleanValue()) {
                return;
            }
            Thread.sleep(sleepMs);
        }
        logger.error("quick poll timeout");
        return;
    }

    private static void initPropery() {
        /** load property */
        Properties props = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream("config.properties");
            props.load(in);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        }

        /** fulfill property */
        starttime = props.getProperty(KEY_STARTTIME);
        endtime = props.getProperty(KEY_ENDTIME);
        tablename = props.getProperty(KEY_TABLENAME);
        majorcompactsize = Long.parseLong(props.getProperty(KEY_SIZE));
    }


    private static void majorCompact() {
        HTable table = null;
        HBaseAdmin hadmin = null;
        HColumnDescriptor[] columndescs = null;
        HTableDescriptor tdesc;
        try {
            hadmin = new HBaseAdmin(hconf);
            table = new HTable(hconf, tablename);
            tdesc = table.getTableDescriptor();
            columndescs = tdesc.getColumnFamilies();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        }

        int i = 0;
        while (true) {
            getTableDetails();
            waitTime();
            try {
                final HFileSystem hfs = new HFileSystem(hconf, false);
                List<HRegionInfo> regionlist = hadmin.getTableRegions(tablename.getBytes());
                if (i == regionlist.size()) {
                    i = 0;
                }
                for (HColumnDescriptor col : columndescs) {
                    String familyname = col.getNameAsString();
                    HRegionInfo region = regionlist.get(i);
                    final Path regionfamilypath = HStore.getStoreHomedir(FSUtils.getTableDir(FSUtils.getRootDir(hconf),
                            TableName.valueOf(tablename)), region, familyname.getBytes());
                    FileStatus[] statuslist = hfs.listStatus(regionfamilypath);
                    if (statuslist.length > 1) {
                        long totalfilesize = 0;
                        for (FileStatus status : statuslist) {
                            totalfilesize += status.getLen();
                        }
                        if (totalfilesize > majorcompactsize) {
                            logger.error("Table:" + tablename + "\tRegion:" + region.getRegionNameAsString() +
                                    "\tFamily:" + familyname + "\tCan not do major compact caused by filesize too large :" +
                                    totalfilesize);
                        } else {
                            logger.info("Starting major compact region:" + region.getRegionNameAsString() +
                                    "\tFamily:" + familyname + "\tFilenum:" + statuslist.length +
                                    "\tTotalSize:" + totalfilesize/1024/1024/1024+"GB");
                            hadmin.majorCompact(region.getRegionName(), familyname.getBytes());

                            // wait for 4 hours
                            quickPoll(new Callable<Boolean>() {
                                public Boolean call() throws Exception {
                                    return (hfs.listStatus(regionfamilypath).length == 1);
                                }
                            }, 6 * 60 * 60 * 1000);
                            logger.info("Complete major compact region:" + region.getRegionNameAsString());
                        }
                    } else {
                        logger.error("Table:" + tablename + "\tRegion:" + region.getRegionNameAsString() +
                                "\tFamily:" + familyname + "\tCan not do major compact caused by file number" +
                                " is one with size :" + statuslist[0].getLen());
                    }
                }
                i++;
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            } finally {
                if (table != null) {
                    try {
                        table.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error(e.getMessage());
                        System.exit(1);
                    }
                }
                System.exit(1);
            }
        }
    }

    private static void getTableDetails() {
        HTable table = null;
        HBaseAdmin hadmin = null;
        HColumnDescriptor[] columndescs = null;
        HTableDescriptor tdesc;
        try {
            hadmin = new HBaseAdmin(hconf);
            table = new HTable(hconf, tablename);
            tdesc = table.getTableDescriptor();
            columndescs = tdesc.getColumnFamilies();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        }

        try {
            final HFileSystem hfs = new HFileSystem(hconf, false);
            long tableSize = 0;
            List<HRegionInfo> regionlist = hadmin.getTableRegions(tablename.getBytes());
            for (HRegionInfo region : regionlist) {
                for (HColumnDescriptor col : columndescs) {
                    String familyname = col.getNameAsString();
                    final Path regionfamilypath = HStore.getStoreHomedir(FSUtils.getTableDir(FSUtils.getRootDir(hconf),
                            TableName.valueOf(tablename)), region, familyname.getBytes());
                    FileStatus[] statuslist = hfs.listStatus(regionfamilypath);
                    long totalfilesize = 0;
                    for (FileStatus status : statuslist) {
                        totalfilesize += status.getLen();
                        tableSize += status.getLen();
                    }
                    logger.info("Table:" + tablename + "\tRegion:" + region.getRegionNameAsString() +
                            "\tFamily:" + familyname + "\tFilenum:" + statuslist.length + "\tSize:" +
                            totalfilesize/1024/1024/1024 +"GBytes");
                }
            }
            logger.info("Table:" + tablename + "\tSize: "+ tableSize/1024/1024/1024 + "GBytes");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                    System.exit(1);
                }
            }
        }
    }

    public static void main(String[] args) {
        hconf = HBaseConfiguration.create();
        /** init logger */
        logger = Logger.getLogger(QHBaseCompact.class);

        logger.info("Starting Qunar HBase Graceful Major Compact");
        /** Load property */
        initPropery();

        /** start major compact */
        majorCompact();
        logger.info("Stoping Qunar HBase Graceful Major Compact");
    }
}
