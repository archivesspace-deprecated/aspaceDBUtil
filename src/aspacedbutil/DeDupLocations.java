/*
 * A util class to de-dup locations after then have been migrated using 
 * the Archon Migration tool
 */

package aspacedbutil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author nathan
 */
public class DeDupLocations {    
    /**
     * Method to run the task that finds and de-duplicates the locations
     * in an ASpace database.  It using very basic queries to make it easier to
     * understand what's going on.
     * 
     * @param connection
     * @throws java.sql.SQLException
     */
    public static void runTask(Connection connection) throws SQLException {
        System.out.println("Removing duplicate Locations ...\n");
        
        HashMap<String, ArrayList<String>> duplicatesMap = new HashMap<>();
        
        String duplicateSQL = "SELECT id, title FROM location WHERE title IN " + 
                "(SELECT title FROM location GROUP BY title HAVING count(id) > 1)";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(duplicateSQL);
        
        int total = 0;
        while (rs.next()) {
            total++;
            String id = rs.getString("id");
            String title = rs.getString("title");
            
            System.out.println("Duplicate Location :: " + id + "\t" + title);
            
            // place all duplicates in a hashmap
            if(!duplicatesMap.containsKey(title)) {
                ArrayList<String> idList = new ArrayList<>();
                idList.add(id);
                duplicatesMap.put(title, idList);
            } else {
                //System.out.println("\nWe have duplicate " + title + "\n");
                ArrayList<String> idList = duplicatesMap.get(title);
                idList.add(id);
            }
        }
        
        // now now find all containers which have a duplication locations
        updateContainersAndDeleteLocations(stmt, duplicatesMap);
        
        System.out.println("\nDuplicates/Records with Duplicates " + 
                duplicatesMap.size() + "/" + total + "\n\n");
        
        // close the statement now 
        stmt.closeOnCompletion();
    }
    
    /**
     * Method to find and locate containers which are linked to duplicate locations
     */
    private static void updateContainersAndDeleteLocations(Statement stmt, HashMap<String, ArrayList<String>> duplicatesMap) throws SQLException {
        int totalDeleted = 0;
        
        for(String title: duplicatesMap.keySet()) {
            ArrayList<String> idList = duplicatesMap.get(title);
            if(idList.size() > 1) {
                System.out.println("\nLinking containers to one location: " + title);
                
                String ids = "";
                for(String id: idList) {
                    ids += id + ",";
                }
                
                // need to remove the last , from the string
                ids = "(" + ids.substring(0, ids.length()-1) + ")";
                
                // now update the containers in the housed_at_rlshp table 
                // to point point only to the first location
                String firstId = idList.get(0);
                String updateSQL = "UPDATE housed_at_rlshp SET location_id=" + firstId +
                        " WHERE location_id IN " + ids;
                
                System.out.println(updateSQL);
                
                // initialize row to -1 so we know if we in testing mode
                int rows = -1;
                
                if(!ASpaceDBUtil.TEST) {
                    rows = stmt.executeUpdate(updateSQL);
                }
                
                System.out.println("Number of containers updated: " + rows);
                
                // now we must delete the duplicate rows from the locations table
                String idsToDelete = ids.replace("(" + firstId + ",", "(");
                String deleteSQL = "DELETE FROM location WHERE id IN " + idsToDelete;
                System.out.println(deleteSQL);
                
                if(!ASpaceDBUtil.TEST) {
                    rows = stmt.executeUpdate(deleteSQL);
                }
                
                System.out.println("Number of locations deleted: " + rows);
                totalDeleted += rows;
                
                stmt.closeOnCompletion();
            }
        }
        
        System.out.println("\nTotal # Locations deleted: " + totalDeleted + "\n");
    }
}
