package com.github.diaosi.BDAM;

import java.util.*;
import java.io.*;
import java.net.*;

import com.github.diaosi.BDAM.InfoboxGetter;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class DiaoSiDriver {
    private static Configuration conf = new Configuration();

    public static void main (String [] args)
    {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("infoboxgetter", InfoboxGetter.class, "Get all the pages contain infobox");
            pgd.driver(args);
            exitCode = 0;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
