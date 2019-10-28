/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lisapark.koctopus.repo;

import com.google.common.reflect.ClassPath;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author alexmy
 */
public class Main {

    static final Logger LOG = Logger.getLogger(AbstractOctopusRepository.class.getName());
    static final String TOP_PACKAGE = "org.lisapark.koctopus.processors";
    private static final String[] REPO_PATH = {"file:///home/alexmy/.m2/repository/k-octopus/k-octopus-processors/0.7.3/k-octopus-processors-0.7.3-jar-with-dependencies.jar"};

    public static void main(String[] args) {
//        String repositoryDir   = "/home/alexmy/.m2/repository";
//        String groupId         = "k-octopus";
//        String artifactId      = "k-octopus-processors";
//        String artifactVersion = "0.7.3";
//        String mainClassName   = "org.lisapark.koctopus.processors.processor.sma.SmaRedis";

        List<String> repoPathList = new ArrayList<>(Arrays.asList(REPO_PATH));

        repoPathList.forEach(new Consumer<String>() {
            @Override
            public void accept(String item) {
                try {
                    URLClassLoader child = new URLClassLoader(new URL[]{new URL(item)}, this.getClass().getClassLoader());
                    ClassPath classpath = ClassPath.from(child);
                    classpath.getAllClasses().forEach((classInfo) -> {
                        if (classInfo.getPackageName().contains(TOP_PACKAGE + ".source")) {
                            System.out.println("SOURCE: " + classInfo);
                        } else if (classInfo.getPackageName().contains(TOP_PACKAGE + ".sink")) {
                            System.out.println("SINK: " + classInfo);
                        } else if (classInfo.getPackageName().contains(TOP_PACKAGE + ".processor") 
                                || classInfo.getPackageName().contains(TOP_PACKAGE + ".pipe")) {
                            System.out.println("PROCESSOR: " + classInfo);
                        }
                    });
                } catch (MalformedURLException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
        });

//        Class<String[]> aClass = String[].class;
//        Method mainMethod = null;
//        try {
//            mainMethod = mainClass.getMethod("main", new Class[]{aClass});
//        } catch (NoSuchMethodException e) {
//            System.out.println("Main class does not contain a main(String[] args) method");
//        }
//        try {
//            mainMethod.invoke(null, (Object) argsToMainMethod);
//        } catch (IllegalAccessException e) {
//            System.out.println("main(String[] args) method not accessible: " + e.getMessage());
//        } catch (InvocationTargetException e) {
//            System.out.println("Calling main(String[] args) failed: " + e.getMessage());
//        }
    }
}
