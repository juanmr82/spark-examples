package com.pybsoft.pipelines;

import com.pybsoft.pipelines.pipes.impl.ManualDFVersioningLink;
import com.pybsoft.pipelines.pipes.Pipe;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.*;

/**
 * Class to create and run different Spark Tasks
 */
public class PipeHandler {

    private static String APP_NAME = "PipeHandler";
    private static String LOGGER = "org.apache";

    public static void main(String[] args){

        boolean debugFlag = false;

        Options options = new Options();

        options.addOption("d", "debug", false, "Enable Debugging mode");
        options.addOption("l", "log-level", true, "Set Log level: WARN, FATAL, ERROR, ALL, OFF");

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options,args);

            debugFlag =  cmd.hasOption("d");

            if(debugFlag) System.out.println("Debug mode enabled");

            String logLevel = cmd.getOptionValue("l");

            if(cmd.getOptionValue("l") != null){
                switch (logLevel){
                    case "WARNING":
                        Logger.getLogger(LOGGER).setLevel(Level.WARN);
                        break;
                    case "FATAL":
                        Logger.getLogger(LOGGER).setLevel(Level.FATAL);
                    case "ERROR":
                        Logger.getLogger(LOGGER).setLevel(Level.ERROR);
                        break;
                    case "ALL":
                        Logger.getLogger(LOGGER).setLevel(Level.ALL);
                        break;
                    case "OFF":
                        Logger.getLogger(LOGGER).setLevel(Level.OFF);
                        break;
                    default:
                        System.out.println("-->Invalid Log level selected. Using Sparks default log level");
                }
            }

            System.out.println("-->Spark Log level:" + Logger.getLogger(LOGGER).getLevel());

        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("javac PipeHandler.java", "Following options available",options,"",true);
            System.exit(1);

        } catch (IllegalAccessError e){
            Logger.getLogger(LOGGER).setLevel(Level.OFF);
            System.out.println("-->Something went wrong reading Logger config. Using Spark Log level: OFF" );
        }
        /**
        * Creates an Spark Session. Master and other settings are defined during the submit
        */
        SparkSession spark = SparkSession.builder().appName(APP_NAME).getOrCreate();

        List<Pipe> pipeline = new ArrayList<>();
        pipeline.add(new ManualDFVersioningLink(debugFlag));
        pipeline.forEach(p ->{
            p.init();
            p.run(spark);
            p.terminate();
        });
        spark.close();
    }

}

