package dog;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DogDao {
    DataSource ds;

    public DogDao(DataSource ds) {
        this.ds = ds;
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
             ResultSet rs = st.executeQuery(
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

}
