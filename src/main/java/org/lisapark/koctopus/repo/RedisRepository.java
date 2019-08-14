/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lisapark.koctopus.repo;

import org.lisapark.koctopus.core.OctopusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author alexmy
 */
public class RedisRepository extends AbstractOctopusRepository implements OctopusRepository {

    private static final Logger LOG = LoggerFactory.getLogger(RedisRepository.class);

    public RedisRepository() {
        super();
    }   
}
