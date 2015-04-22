aspaceDBUtil
============

A Small Java Utility Program for Doing Post Archon Migration Data Clean Up.

This utility is meant to be used inconjunction with the Archon Migration tool to normalize the transferred records.  It makes changes directly to the ASpace backend MySQL database, so needs to be run after the migration.  The current release:

1. Removes Classifications records which are not linked to any Resource or Accession records.
2. Removes duplicate Location records.
3. Removes orphaned archival objects

#####Usage

1. Stop the ASpace instance.
2. Backup the ASpace MYSQL database.
3. Unzip the aspaceDBUtil.zip into any directory on the server which ASpace is installed on.
4. From the command line change into the aspaveDBUtil directory and issue the following command: 
  - **java -jar aspaceDBUtil.jar jdbc:mysql://localhost:3306/aspaceDB username password**
  - Replace the jdbc URL, username, and password with those for the ASpace instance
5. Change to **[archivesspace directory]/data** and delete the following directories **indexer_state**, **solr_backups**, and **solr_index**.  This will force a re-index of all records when ASpace is resarted.
6. Restart the ASpace instance and wait for the records to re-indexed.

It is also possible to run this program on any computer which has Java 1.6 or later installed, and which has direct access to the ASpace MYSQL database.



