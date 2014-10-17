/*
 * A util class to remove classfications records not linked to any term 
 * or resources
 */

package aspacedbutil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author nathan
 */
public class CleanClassifcations {
    /**
     * Method to called to do the actual cleaning up of classifications records
     * @param conn
     * @throws SQLException 
     */
    public static void runTask(Connection conn) throws SQLException {
        System.out.println("Cleaning up Classifications ...\n");
        
        // Delete any classification that is not linked to a resource/accession 
        // or has a classification_term that is linked to a resource/accession.
        String deleteClassifcationSQL = "DELETE FROM classification " +
                "WHERE id NOT IN " + 
                "(SELECT DISTINCT classification_id AS id FROM classification_rlshp " + 
                "UNION SELECT DISTINCT root_record_id FROM classification_term " + 
                "WHERE id NOT IN ( SELECT DISTINCT classification_term_id from classification_rlshp ) )";
        
        Statement stmt = conn.createStatement();
        int rows = stmt.executeUpdate(deleteClassifcationSQL);
        System.out.println("Number of classifications deleted: " + rows);
        
        // Clean out any classification_terms that were left orphaned.
        String deleteOrphanedClassifcationTermSQL = "DELETE FROM classification_term WHERE " + 
                "root_record_id NOT IN ( SELECT id FROM  classification )";
        
        rows = stmt.executeUpdate(deleteOrphanedClassifcationTermSQL);
        System.out.println("Number of orphaned classification terms deleted: " + rows);
        
        stmt.closeOnCompletion();
    }
}
