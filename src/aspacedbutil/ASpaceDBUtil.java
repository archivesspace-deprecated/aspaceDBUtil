/*
 * A simple utility class for fixing migration issues directly on the ASpace 
 * backend MYSQL database.
 */

package aspacedbutil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author nathan
 */
public class ASpaceDBUtil {
    public static boolean TEST = false;
    private final String dbmsURL;
    private final String username;
    private final String password;
    
    /**
     * The default constructor
     * 
     * @param dbmsURL
     * @param username
     * @param password 
     */
    public ASpaceDBUtil(String dbmsURL, String username, String password) {
        this.dbmsURL = dbmsURL;
        this.username = username;
        this.password = password;
    }
    
    /**
     * Method to get a connection to the database
     * 
     * @return
     * @throws SQLException 
     */
    public Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(dbmsURL, username, password);
        System.out.println("Connected to database " + dbmsURL);
        return connection;
    }
    
    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws SQLException {
        System.out.println("ASpaceDBUtil v1.0 (10-15-2014)");
        
        String dbmsURL = "";
        String username = "";
        String password = "";
        
        if(args.length >= 3) {
            dbmsURL = args[0];
            username = args[1];
            password = args[2];
            
            // see whether to just run in test mode
            if(args.length == 4 && args[3].equalsIgnoreCase("TEST")) {
                System.out.println("*** TEST MODE ***");
                ASpaceDBUtil.TEST = true;
            }
        } else {
            System.out.println("ERROR: Missing Database Connection Information\n");
            System.out.println("Program Usage:\njava -jar ASpaceDBUtil.jar jdbc:mysql://localhost:3306/aspaceDB username password\n");
            System.out.println("Where the JDBC URL, username and password are replaced with those for the ASpace instance\n\n");
            System.exit(-1);
        }
        
        // we get to this point so run the task now
        ASpaceDBUtil aspaceDBUtil = new ASpaceDBUtil(dbmsURL, username, password);
        Connection connection = null;
        
        try {
            connection = aspaceDBUtil.getConnection();
            System.out.println("\n");
            
            DeDupLocations.runTask(connection);
            CleanClassifcations.runTask(connection);
        } catch (SQLException ex) {
            System.out.println("A database error occured\n" + ex.getMessage());
        } finally {
            if(connection != null) {
                connection.close();
            }
        }
    }  
}
