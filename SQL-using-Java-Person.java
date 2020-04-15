package DataAccessObjects;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import Model.Person;


/**
 * A class to communicate with the Person table in the Database
 */
public class PersonDAO {
    private static Logger logger;

    static {
        logger = Logger.getLogger("familymap");
    }

    /**
     * Creates the person table
     *
     * @throws DataBaseException
     */
    public void createPersons() throws DataBaseException {
        logger.entering("PersonDao", "createPersons");
        try {
            Statement stmt = null;
            try {
                stmt = db.getInstance().getConn().createStatement();
                stmt.executeUpdate("drop table if exists person");
                stmt.executeUpdate("create table person ( " +   // create correct columns
                        "id varchar(255) not null primary key,\n" +
                        "descendant varchar(255) not null,\n" +
                        "firstName varchar(255) not null,\n" +
                        "lastName varchar(255) not null,\n" +
                        "gender varchar(255) not null,\n" +
                        "fatherID varchar(255),\n" +
                        "motherID varchar(255),\n" +
                        "spouseID varchar(255),\n" +
                        "foreign key (descendant) references user(userName)," +
                        "foreign key (fatherID) references person(id)," +
                        "foreign key (motherID) references person(id)," +
                        "foreign key (spouseID) references person(id) )");
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("createPersons failed", e);
        }
        logger.exiting("PersonDao", "createPersons");
    }

    /**
     * Adds a specific person to the table
     *
     * @param person to be added to the table
     * @return the PersonID of the person who was added
     * @throws DataBaseException if failed
     */
    public String addPerson(Person person) throws DataBaseException {
        logger.entering("PersonDao", "addPerson");
        try {
            PreparedStatement stmt = null;
            try {
                // Decode person object into Strings
                String userID = person.getPersonID();
                String descendant = person.getDescendant();
                String first = person.getFirstName();
                String last = person.getLastName();
                String gender = person.getGender();
                String father = person.getFather();
                String mother = person.getMother();
                String spouse = person.getSpouse();
                System.out.println("\t\t\tAdding " + person.getPersonID());
                String sql = "insert into person (id,descendant,firstName,lastName,gender,fatherID,motherID,spouseID) values( '" +
                        userID + "', '" + descendant + "', '" + first + "', '" + last + "', '" + gender + "', '" + father + "', '" + mother + "', '" + spouse + "')";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                if (stmt.executeUpdate() != 1) {    // Actually add the person
                    throw new DataBaseException("addPerson failed: Could not insert user");
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("addPerson failed", e);
        }
        logger.exiting("PersonDao", "addPerson");
        return person.getPersonID();
    }

    /**
     * Reads the table to find an existing Person
     * A valid person is one who has the correct ID and associated user
     *
     * @param person object to be checked against database
     * @return true if person is in the table
     * @throws DataBaseException if failed
     */
    public boolean isValidPerson(Person person) throws DataBaseException {
        logger.entering("PersonDao", "isValidPerson");
        boolean result = false;
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = "select * from person"; // Grab everything from the person
                stmt = db.getInstance().getConn().prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) { // Check against parameter
                    String id = rs.getString("id");
                    String username = rs.getString("descendant");
                    if (id.equals(person.getPersonID()) && username.equals(person.getDescendant())) {
                        result = true;
                    }
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
            throw new DataBaseException("isValidPerson() failed", e);
        }
        logger.exiting("PersonDao", "isValidPerson");
        return result;
    }

    /**
     * Returns the personID for the username that was passed in
     *
     * @param descendant the username of the person
     * @return the ID of the requested person
     * @throws DataBaseException when failed
     */
    public String getPersonId(String descendant) throws DataBaseException {
        logger.entering("PersonDao", "getPersonId");
        String id = null;
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = "select userPersonID from user where userName = '" + descendant + "'";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    id = rs.getString("userPersonID");    // Grab the ID
                }
            } finally { // Close everything up before moving on
                if (stmt != null) {
                    stmt.close();
                }
                if (rs != null) {
                    rs.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("isValidPerson() failed", e);
        }
        logger.exiting("PersonDao", "isValidPerson");
        return id;
    }


    /**
     * Reads a person object from the database
     *
     * @param PersonID of the object to be read
     * @return the person object with the personID
     * @throws DataBaseException when failed
     */
    public Person readPerson(String PersonID) throws DataBaseException {
        logger.entering("PersonDao", "readPerson");
        Person person = new Person();
        person.setPersonID(PersonID);
        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = "select * from person where id = '" + PersonID + "'";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) { // Populate return object
                    person.setPersonID(rs.getString("id"));
                    person.setDescendant(rs.getString("descendant"));
                    person.setFirstName(rs.getString("firstName"));
                    person.setLastName(rs.getString("lastName"));
                    person.setGender(rs.getString("gender"));
                    person.setFather(rs.getString("fatherID"));
                    person.setMother(rs.getString("motherID"));
                    person.setSpouse(rs.getString("spouseID"));
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
            throw new DataBaseException("\tERROR: readPerson() failed", e);
        }
        logger.exiting("PersonDao", "readPerson");
        return person;
    }

    /**
     * Returns an array of persons who are descendants of the user
     *
     * @param username who is the descendant of the tree
     * @return an array of ancestors #bringHonorToUsAll
     * @throws DataBaseException if failure occured
     */
    public Person[] readPersons(String username) throws DataBaseException {
        logger.entering("PersonDao", "readPersons");
        // Implement with sets for variable and unknown sizes
        Set<String> IDs = new HashSet<>();
        Set<Person> persons = new HashSet<>();

        try {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                String sql = "select id from person where descendant = '" + username + "'";
                stmt = db.getInstance().getConn().prepareStatement(sql);
                rs = stmt.executeQuery();
                while (rs.next()) {                 //Grab all the matching rows from the table
                    IDs.add(rs.getString("id"));
                }

                for (String id : IDs) {              //Get the corresponding object for each row
                    Person current = readPerson(id);
                    persons.add(current);
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
            throw new DataBaseException("\tERROR: readPersons() failed", e);
        }

        // Transfer to an array from the list
        Person[] list = new Person[persons.size()];
        int i = 0;
        for (Person current : persons) {
            list[i] = current;
            i++;
        }
        logger.exiting("PersonDao", "readPersons");
        return list;
    }

    /**
     * Delete any people associated with the username
     * Called when loading a new family tree
     *
     * @param username who got excommunicated
     * @return true if person was expunged
     * @throws DataBaseException if failure occurred
     */
    public boolean deletePerson(String username) throws DataBaseException {
        logger.entering("PersonDao", "deletePerson");
        boolean result = false;
        try {
            Statement stmt = null;
            try {
                stmt = db.getInstance().getConn().createStatement();
                String sql = "delete from person where descendant = '" + username + "'";
                stmt.executeUpdate(sql);
                result = true;
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("DeletePersons() failed", e);
        }
        logger.exiting("PersonDao", "deletePerson");
        return result;
    }

    /**
     * Destroy the person table
     *
     * @return true if person table was deleted
     * @throws DataBaseException if failure occured
     */
    public boolean clearPersons() throws DataBaseException {
        logger.entering("PersonDao", "clearPersons");
        boolean result = false;
        try {
            Statement stmt = null;
            try {
                stmt = db.getInstance().getConn().createStatement();
                stmt.executeUpdate("drop table if exists person");
                result = true;
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new DataBaseException("ClearPersons failed", e);
        }
        logger.exiting("PersonDao", "clearPersons");
        return result;
    }
}
