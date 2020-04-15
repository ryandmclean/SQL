package DataAccessObjects;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import Model.AuthToken;

/**
 * A class to communicate with the authToken table in the Database
 */

public class AuthTokenDAO {
    private static Logger logger;

    static {
        logger = Logger.getLogger("familymap");
    }

    /**
     * Creates the authToken table
     *
     * @throws DataBaseException if applicable
     */
    public void createAuthTokens() throws DataBaseException {
        logger.entering("AuthTokenDao", "createAuthTokens");
        try {
            Statement stmt = null;
            try {
                stmt = db.getInstance().getConn().createStatement();

                stmt.executeUpdate("drop table if exists authToken");   // Drop table if exists to avoid errors
                stmt.executeUpdate("create table authToken ( " +        // Create the table with correct columns
                        "authToken varchar(255) not null primary key,\n" +
                        "userName varchar(255) not null,\n" +
                        "foreign key (userName) references user(userName) )");
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("createAuthTokens failed", e);
        }
        logger.exiting("AuthTokenDao", "createAuthTokens");
    }

    /**
     * Adds a new token to the token table in the database
     *
     * @param token object that is passed in
     * @return the authToken added to the table
     * @throws DataBaseException if failed
     */
    public String addAuthToken(AuthToken token) throws DataBaseException {
        logger.entering("AuthTokenDao", "addAuthToken");
        if (isValidToken(token)) {
            throw new DataBaseException("authToken is already in the Database");
        }
        try {
            PreparedStatement stmt = null;
            try {
                String sql = "insert into authToken (authToken,userName) values( '" + // Insertion table
                        token.getAuthToken() + "', '" + token.getUsername() + "')";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                if (stmt.executeUpdate() != 1) {    // Add token to the table
                    throw new DataBaseException("addToken failed: Could not insert token");
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("addToken failed", e);
        }
        logger.exiting("AuthTokenDao", "addAuthToken");
        return token.getAuthToken();
    }

    /**
     * Checks if a token object is in the table
     *
     * @param token object to check
     * @return true if the token is in the table
     * @throws DataBaseException if failed
     */
    public boolean isValidToken(AuthToken token) throws DataBaseException {
        logger.entering("AuthTokenDao", "isValidToken");
        boolean result = false;
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {

                String sql = "select * from authToken where authToken = '" + token.getAuthToken() + "'";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) { // Get the values returned from query
                    String authToken = rs.getString("authToken");
                    String username = rs.getString("userName");
                    if (authToken.equals(token.getAuthToken()) && username.equals(token.getUsername())) {
                        result = true;
                    }
                }
            } finally { // Make sure everything is closed before moving on
                if (stmt != null) {
                    stmt.close();
                }
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("isValidToken() failed", e);
        }
        logger.exiting("AuthTokenDao", "isValidToken");
        return result;
    }

    /**
     * Verifies that the token string is part of the table
     *
     * @param token string to compare against table
     * @return true if string is in the table
     * @throws DataBaseException if failed
     */
    public boolean isValidToken(String token) throws DataBaseException {
        logger.entering("AuthTokenDao", "isValidToken");
        boolean result = false;
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = "select * from authToken where authToken = '" + token + "'";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    String authToken = rs.getString("authToken");
                    if (authToken.equals(token)) {
                        result = true;
                    }
                }
            } finally { // Close before moving on
                if (stmt != null) {
                    stmt.close();
                }
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("isValidToken() failed", e);
        }
        logger.exiting("AuthTokenDao", "isValidToken");
        return result;
    }

    /**
     * Gets the username associated with the token
     *
     * @param token string that is passed in
     * @return the string username associated with token
     * @throws DataBaseException if failed
     */
    public String getUserName(String token) throws DataBaseException {
        logger.entering("AuthTokenDao", "getUserName");
        String username = "";
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = "select * from authToken where authToken = '" + token + "'";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    username = rs.getString("userName");
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("getUserName() failed", e);
        }
        logger.exiting("AuthTokenDao", "getUserName");
        return username;
    }

    public boolean deleteToken(String username) throws DataBaseException {
        logger.entering("AuthTokenDao", "deleteToken");
        boolean result = false;
        try {
            Statement stmt = null;
            try {
                stmt = db.getInstance().getConn().createStatement();
                String sql = "delete from authToken where userName = '" + username + "'";
                stmt.executeUpdate(sql);
                result = true;
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("DeleteToken Failed", e);
        }
        logger.exiting("AuthTokenDao", "deleteToken");
        return result;
    }

    /**
     * Destroys the token table
     *
     * @return true if the table was deleted
     * @throws DataBaseException when failed
     */
    public boolean clearTokens() throws DataBaseException {
        logger.entering("AuthTokenDao", "clearTokens");
        boolean result = false;
        try {
            Statement stmt = null;
            try {
                stmt = db.getInstance().getConn().createStatement();
                stmt.executeUpdate("drop table if exists authToken");
                result = true;
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("ClearTokens failed", e);
        }
        logger.exiting("AuthTokenDao", "clearTokens");
        return result;
    }
}
