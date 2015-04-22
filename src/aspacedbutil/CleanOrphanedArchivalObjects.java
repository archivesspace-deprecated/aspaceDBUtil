/*
 * A util class to remove classfications records not linked to any term 
 * or resources
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
public class CleanOrphanedArchivalObjects {

    /**
     * Method to called to do the actual cleaning up of classifications records
     *
     * @param conn
     * @throws SQLException
     */
    public static void runTask(Connection conn) throws SQLException {
        System.out.println("Deleting Orphaned Archival Objects ...\n");

        // create a hashmap to store the archival objects for making finding the duplicate archival object 
        // much faster
        HashMap<String, String[]> archivalObjectsMap = new HashMap<>();

        // an array list which stores the archival objects id orphaned archival objects
        ArrayList<String> orphanedList = new ArrayList<>();

        // find any archival objects which do not have parent which is attached to 
        // to the root resource record
        String findArchivalObjectsSQL = "SELECT id, title, root_record_id, parent_id FROM archival_object";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(findArchivalObjectsSQL);

        while (rs.next()) {
            String[] info = new String[3];

            info[0] = rs.getString("id");
            info[1] = rs.getString("title");
            info[2] = rs.getString("parent_id");

            // store this in the hash map
            archivalObjectsMap.put(info[0], info);
        }

        stmt.closeOnCompletion();

        // now find the duplicates using the hashmap
        int total = 0;
        for (String id : archivalObjectsMap.keySet()) {
            String[] info = archivalObjectsMap.get(id);

            if (info[2] != null && !info[2].equals("0")) {
                System.out.println("Child Archival Object :: " + id + "\t" + info[1] + "\t" + info[2]);

                if (hasTopParentId(archivalObjectsMap, info[2], 0)) {
                    System.out.println("Has valid top Archival Object");
                } else {
                    total++;
                    orphanedList.add(id);
                    System.out.println("Orphaned Archival Object");
                }
            }
        }

        System.out.println("Total Number of Orphaned Archival Object: " + total);
        
        // call method to actual delete the records
        if(!orphanedList.isEmpty()) {
            deleteOrphaneRecords(conn, orphanedList);
        }
    }

    /**
     * Method to check to see if this archival object is not circular in nature
     *
     * @param conn
     * @param parentId
     * @param level
     * @return
     * @throws SQLException
     */
    private static boolean hasTopParentId(HashMap<String, String[]> archivalObjectsMap, String parentId, int level) throws SQLException {
        // need to check level is not more than 12 which will indicate we in loop
        level++;

        if (level >= 10) {
            System.out.println("Circular parent child relationship for " + parentId);
            return false;
        }

        if (archivalObjectsMap.containsKey(parentId)) {
            String[] info = archivalObjectsMap.get(parentId);
            String newParentId = info[2];

            if (newParentId == null || newParentId.equals("0")) {
                return true;
            } else {
                return hasTopParentId(archivalObjectsMap, newParentId, level);
            }
        } else {
            return false;
        }
    }

    /**
     * This is where the orphaned archival objects are actual deleted
     *
     * @param conn
     * @param orphanedList
     */
    private static void deleteOrphaneRecords(Connection conn, ArrayList<String> orphanedList) throws SQLException {
        String ids = "";
        for (String id : orphanedList) {
            ids += id + ",";
        }

        // need to remove the last , from the string
        ids = "(" + ids.substring(0, ids.length() - 1) + ")";

        Statement stmt = conn.createStatement();
        String deleteSQL = "DELETE FROM archival_object WHERE id IN " + ids;
        System.out.println(deleteSQL);

        int rows = -1;
        
        // disable foreign key checks so we can delete records without worrying
        // about chld parent links
        stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");
        
        if (!ASpaceDBUtil.TEST) {
            rows = stmt.executeUpdate(deleteSQL);
        }
        
        stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");
        
        System.out.println("Number of archival objects deleted: " + rows);
    }
}
