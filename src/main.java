public class main {

    public static void main(String[] args) {
        //initializes the publishers and sends the corresponding fiels
        Publisher pubOne = new Publisher();
        Publisher pubTwo = new Publisher();
        Publisher pubThree = new Publisher();

        JSonFileReader reader = new JSonFileReader();

        //Sending the data via the 3 publishers
        reader.read("mtl_temperature.json","mtl.temp",pubOne);
        reader.read("mtl_health.json","mtl.health",pubTwo);
        reader.read("mtl_grade.json","mtl.grade",pubThree);
    }
}
