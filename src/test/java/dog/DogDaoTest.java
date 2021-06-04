package dog;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class DogDaoTest {
    DogDao dao;

    @BeforeEach
    void init() throws SQLException {
        MariaDbDataSource dataSource;
        dataSource = new MariaDbDataSource();
        dataSource.setUrl("jdbc:mariadb://localhost:3306/employees?useUnicode=true");
        dataSource.setUser("employees");
        dataSource.setPassword("employees");

        Flyway flyway = Flyway.configure()
                .locations("/db/migration/dogs")
                .dataSource(dataSource).load();

        flyway.clean();
        flyway.migrate();

        dao = new DogDao(dataSource);
    }

    @Test
    void testGetDogsByCountry() {
        List<String> types = dao.getDogsByCountry("Hungary");
        System.out.println(types.toString());
        assertEquals(9, types.size());
        assertTrue(types.contains("komondor"));
        assertTrue(types.contains("kuvasz"));
    }

    @Test
    void testSaveBlob() throws IOException {
        String inputFile = "Qtya.gif";
        InputStream is1 = Files.newInputStream(Path.of("c:", "training", "blob","src","main","resources",inputFile));
        dao.addImage(inputFile, 4L, is1);
//read stream does not exist any more
        InputStream is2 = Files.newInputStream(Path.of("c:", "training", "blob","src","main","resources",inputFile));
        dao.addImage(inputFile, 40, is2);

    }

    @Test
    void testGetImageByDogName() throws IOException {
        //save the 1st image to DB
        String inputFile = "Qtya.gif";
        InputStream is1a = Files.newInputStream(Path.of("c:", "training", "blob","src","main","resources",inputFile));
        dao.addImage(inputFile, 40, is1a);

        //read image of dog1 from DB
        InputStream dogImg1 = dao.getImageByDogName("IRISH SOFT COATED WHEATEN TERRIER");
        byte[] dogArr1 = new byte[20];
        dogImg1.read(dogArr1);

        //save the 2nd image to DB
        InputStream is2a = Files.newInputStream(Path.of("c:", "training", "blob","src","main","resources",inputFile));
        dao.addImage(inputFile, 4, is2a);

        //read image of dog 2 from DB
        InputStream dogImg2 = dao.getImageByDogName("CAIRN TERRIER");
        byte[] dogArr2 = new byte[20];
        dogImg2.read(dogArr2);

        //read the image again for making an Array
        InputStream is1b = Files.newInputStream(Path.of("c:", "training", "blob","src","main","resources",inputFile));
        byte[] isArr = new byte[20];
        is1b.read(isArr);


        assertArrayEquals(dogArr1, isArr);
        assertArrayEquals(dogArr2, isArr);
        assertThrows(IllegalStateException.class, () -> dao.getImageByDogName("non Existing"));
    }

}
