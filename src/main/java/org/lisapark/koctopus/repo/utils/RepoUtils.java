/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lisapark.koctopus.repo.utils;

import org.lisapark.koctopus.util.Pair;

/**
 *
 * @author alexmy
 */
public class RepoUtils {
    /**
     * 
     * @param splitString
     * @return 
     */
    public static Pair<String, String> getPair(String splitString) {
        Pair<String, String> pair;
        String[] _pair = splitString.split("|");
        switch (_pair.length) {
            case 2:
                pair = new Pair<>(_pair[0], _pair[1]);
                break;
            case 1:
                pair = new Pair<>(null, _pair[1]);
                break;
            default:
                pair = new Pair<>(null, null);
                break;
        }
        return pair;
    }

}
