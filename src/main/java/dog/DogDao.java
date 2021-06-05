package dog;

import org.mariadb.jdbc.MariaDbDataSource;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DogDao {
    MariaDbDataSource ds;

    public DogDao() {
        Properties prop = new Properties();
        try(InputStream is = DogDao.class.getResourceAsStream("/dog.properties")) {
            prop.load(is);

            String url = prop.getProperty("Url");
            String user = prop.getProperty("User");
            String password = prop.getProperty("Password");

            ds = new MariaDbDataSource();
            ds.setUrl(url);
            ds.setUser(user);
            ds.setPassword(password);
        } catch (IOException | SQLException e){
            throw new IllegalStateException("Cannot read dog.properties", e);
        }
    }

    public MariaDbDataSource getDs() {
        return ds;
    }

    public List<String> getDogsByCountry(String country) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT name FROM dog_types WHERE country = ? ORDER BY name"
             )) {
            ps.setString(1, country);
            return selectByPrepStatement(ps);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Cannot connect", e);
        }
    }

    private List<String> selectByPrepStatement(PreparedStatement ps) {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(rs.getString("name").toLowerCase());
            }
            return result;
        } catch (SQLException se) {
            throw new IllegalArgumentException("Cannot find name", se);
        }
    }

    public void addImage(String filename, long dogId, InputStream ins) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO images(`dog_id`,`filename`,`extension`,`content`) value(?,?,?,?)"
             )) {
            Blob blobImg = conn.createBlob();
            fillBlobImg(blobImg, ins);     //my reading method

            ps.setLong(1, dogId);
            String nameOfFile = filename.substring(0, filename.lastIndexOf("."));
            String nameOfExtension = filename.substring(filename.lastIndexOf(".") + 1);
            ps.setString(2, nameOfFile);
            ps.setString(3, nameOfExtension);
            ps.setBlob(4, blobImg);
            ps.execute();
        } catch (SQLException se) {
            throw new IllegalStateException("Cannot connect to DB", se);
        }
    }

    private void fillBlobImg(Blob blobImg, InputStream ins) {
        try (OutputStream os = blobImg.setBinaryStream(1);         //indexelés 1-től?
             BufferedInputStream bis = new BufferedInputStream(ins)
        ) {
            bis.transferTo(os);
        } catch (SQLException | IOException e) {
            throw new IllegalArgumentException("Error creating blob", e);
        }
    }

    public InputStream getImageByDogName(String name) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT content FROM images WHERE dog_id = (SELECT id FROM dog_types WHERE NAME = ?);"
             )) {
            ps.setString(1, name);
            return getImageByPrepStatement(ps);
        } catch (SQLException se) {
            throw new IllegalStateException("Cannot connect to DB", se);
        }
    }

    private InputStream getImageByPrepStatement(PreparedStatement ps) {
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                Blob blob = rs.getBlob(1);
                return blob.getBinaryStream();
            }
            throw new IllegalStateException("Cannot find image");
        } catch (SQLException se) {
            throw new IllegalArgumentException("Error by query", se);
        }
    }

    public void updateDogNamesByCountry(String country, String preposition) {
        String dogName = "";
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet rs = st.executeQuery(           //id required!
                     "SELECT id, name, country FROM dog_types"
             )) {
            while (rs.next()) {
                dogName = rs.getString("name");
                String dogCountry = rs.getString("country");
                if (dogCountry.equals(country) && !dogName.contains(preposition)) {
                    rs.updateString("name", preposition.concat("_").concat(dogName));
                    rs.updateRow();
                }
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException("Cannot update" + dogName, e);
        }
    }

    public void updateDogNamesByCountryWithPrepStatement(String country, String preposition) {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT id, name FROM dog_types WHERE country = ?",
                     ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE
             )) {
            ps.setString(1, country);
            modifyByPreparedStatement(ps, preposition);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Cannot access", e);
        }
    }

    private void modifyByPreparedStatement(PreparedStatement ps, String preposition) {
        String dogName = "";
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                dogName = rs.getString("name");
                if (!dogName.contains(preposition)) {
                    rs.updateString("name", preposition.concat("_").concat(dogName));
                    rs.updateRow();
                }
            }
        } catch(SQLException se){
            throw new IllegalArgumentException("Cannot update" + dogName, se);
        }
    }

    public List<String> listOddNames() {
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = st.executeQuery(
                     "SELECT id, name FROM dog_types"
             )) {
            List<String> names = new ArrayList<>();
            if (!rs.next()) {
                return Collections.emptyList();    //or names
            }
            names.add(rs.getLong("id") + " " + rs.getString("name"));
            while (rs.relative(2)) {
                names.add(rs.getLong("id") + " " + rs.getString("name"));
            }
            return names;
        } catch (SQLException e) {
            throw new IllegalArgumentException("Cannot access", e);
        }
    }

    public List<String> readMetadataAboutDB(){
        try(Connection conn = ds.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            return getMetadata(metaData);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect", e);
        }
    }

    private List<String> getMetadata(DatabaseMetaData metaData) throws SQLException {
        try(ResultSet rs = metaData.getTables(null,null,null,null)
        ){
            List<String> allData = new ArrayList<>();
            while(rs.next()){
                allData.add( rs.getString(1));
                allData.add( rs.getString(2));
                allData.add( rs.getString(3));
                allData.add( rs.getString(4));
                allData.add( rs.getString(5));
                allData.add( rs.getString(6));
                allData.add( rs.getString(7));
                allData.add( rs.getString(8));
                allData.add( rs.getString(9));
                allData.add( rs.getString(10));
                allData.add("\n");
            }
            return allData;
        }
    }

}
