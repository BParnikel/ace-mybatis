package com.github.vendigo.acemybatis;

import com.github.vendigo.acemybatis.test.app.User;
import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class InsertTest extends AbstractTest {

    @Before
    public void setUp() throws Exception {
        userTestDao.deleteAll();
    }

    @Test
    public void insertOne() throws Exception {
        userMapper.insertOne(petya);
        List<User> actualResults = userTestDao.selectAll();
        assertThat(actualResults, equalTo(Collections.singletonList(petya)));
    }

    @Test
    public void insertSync() throws Exception {
        userMapper.insertSync(users);
        List<User> actualResults = userTestDao.selectAll();
        assertCollections(actualResults, users);
    }

    @Test
    public void insertListAsOne() throws Exception {
        userMapper.insertListAsOne(users);
        List<User> actualResults = userTestDao.selectAll();
        assertCollections(actualResults, users);
    }

    @Test
    public void insertWithAdditionalParameters() throws Exception {
        userMapper.insertWithAdditionalParameters(users, "Kiev");
        List<User> actualResults = userTestDao.selectAll();

        User kievPetya = new User("Petya", "Pomagay", "illhelpyou@gmail.com", "25315", "Kiev");
        User kievBoris = new User("Boris", "Britva", "boris50@gmail.com", "344", "Kiev");
        User kievEric = new User("Eric", "Cartman", "eric2006@gmail.com", "25315", "Kiev");
        User kievGalya = new User("Galya", "Ivanova", "galya_ivanova@gmail.com", "54915", "Kiev");
        User kievOstin = new User("Ostin", "Lyapunov", "ostin_lyapota@in.ua", "54915", "Kiev");
        List<User> kievUsers = Arrays.asList(kievPetya, kievBoris, kievEric, kievGalya, kievOstin);
        assertCollections(actualResults, kievUsers);
    }

    @Test(expected = PersistenceException.class)
    public void insertWithError() throws Exception {
        userMapper.insertWithError(users);
    }

    @Test
    public void insertCollector() throws Exception {
        List<User> collectedUsers = users.stream().collect(userMapper.insertCollector());
        List<User> actualResults = userTestDao.selectAll();
        assertCollections(actualResults, users);
        assertCollections(collectedUsers, users);
    }
}
