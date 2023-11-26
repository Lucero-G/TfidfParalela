/*
Programacion Paralela
Seccion: 1
Integrantes:
    Casaperalta Pacheco, Luis
    De Jesús Lezama, Marco Antonio
    Gutierrez Aquino, Lucero Andrea
 */
package pe.edu.unmsm.sistemas.tf_idf;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Casaperalta, De Jesus, Gutierrez
 */
class DocumentActor extends AbstractActor {

    private final String filePath;
    private final TFIDF tfidfCalculation;

    public DocumentActor(String filePath, TFIDF tfidf) {
        this.filePath = filePath;
        this.tfidfCalculation = tfidf;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("process", msg -> processDocument())
                .build();
    }

    private void processDocument() {
        // Aquí procesas el documento y envías las DocumentProperties al SupervisorActor
        HashMap<String, Integer> wordCount = tfidfCalculation.getTermsFromFile(filePath);
        HashMap<String, Double> termFrequency = tfidfCalculation.calculateTermFrequency(wordCount);
        DocumentProperties docProperties = new DocumentProperties();
        docProperties.setWordCountMap(wordCount);
        docProperties.setTermFreqMap(termFrequency);

        // Envía las DocumentProperties al SupervisorActor
        sender().tell(docProperties, self());
    }
}

class SupervisorActor extends AbstractActor {

    private final File[] listOfFiles;
    private final TFIDF tfidfCalculation;
    private final ActorRef[] documentActors;
    private int finishedCount = 0;
    private final HashMap<String, Double> inverseDocFreqMap = new HashMap<>();

    public SupervisorActor(File[] listOfFiles, TFIDF tfidfCalculation) {
        this.listOfFiles = listOfFiles;
        this.tfidfCalculation = tfidfCalculation;
        this.documentActors = new ActorRef[listOfFiles.length];
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("start", msg -> startProcessing())
                .match(DocumentProperties.class, this::processDocumentProperties)
                .build();
    }

    private void startProcessing() {
        for (int i = 0; i < listOfFiles.length; i++) {
            String filePath = listOfFiles[i].getAbsolutePath();
            documentActors[i] = getContext().actorOf(Props.create(DocumentActor.class, filePath, tfidfCalculation));
            documentActors[i].tell("process", self());
        }
    }

    private void processDocumentProperties(DocumentProperties docProperties) {
        // Aquí realizas el cálculo del TF-IDF basado en las DocumentProperties recibidas del DocumentActor
        HashMap<String, Double> inverseDocFreqMap = tfidfCalculation.calculateInverseDocFrequency(new DocumentProperties[]{docProperties});
        HashMap<String, Double> tf = docProperties.getTermFreqMap();
        HashMap<String, Double> tfIDF = new HashMap<>();

        for (Map.Entry<String, Double> entry : tf.entrySet()) {
            String term = entry.getKey();
            Double tfVal = entry.getValue();
            Double idfVal = inverseDocFreqMap.getOrDefault(term, 0.0);
            Double tfIdfValue = tfVal * idfVal;
            tfIDF.put(term, tfIdfValue);
        }

        // Aquí puedes hacer lo que necesites con tfIDF
        System.out.println("TF-IDF for document processed:");
        System.out.println(tfIDF);

        // Count finished documents
        finishedCount++;
        if (finishedCount == listOfFiles.length) {
            // Todos los documentos han sido procesados
            getContext().getSystem().terminate();
        }
    }
}

class DocumentProperties {

    public HashMap<String, Double> getTermFreqMap() {
        return termFreqMap;
    }

    HashMap<String, Integer> getWordCountMap() {
        return DocWordCounts;
    }

    void setTermFreqMap(HashMap<String, Double> inMap) {
        termFreqMap = new HashMap<String, Double>(inMap);
    }

    void setWordCountMap(HashMap<String, Integer> inMap) {
        DocWordCounts = new HashMap<String, Integer>(inMap);
    }
    private HashMap<String, Double> termFreqMap;
    HashMap<String, Integer> DocWordCounts;
}

public class TFIDF {

    SortedSet<String> wordList = new TreeSet(String.CASE_INSENSITIVE_ORDER);

    //Calculates inverse Doc frequency.
    public HashMap<String, Double> calculateInverseDocFrequency(DocumentProperties[] docProperties) {
        HashMap<String, Double> inverseDocFreqMap = new HashMap<>();

        int totalDocuments = docProperties.length;

        for (String word : wordList) {
            int documentsWithWord = 0;

            for (DocumentProperties docProperty : docProperties) {
                if (docProperty.getWordCountMap().containsKey(word)) {
                    documentsWithWord++;
                }
            }

            if (documentsWithWord > 0) {
                double idf = Math.log((double) totalDocuments / documentsWithWord);
                inverseDocFreqMap.put(word, idf);
            } else {
                // Si documentsWithWord es 0, puedes asignar un valor predeterminado o manejarlo de alguna manera que tenga sentido para tu aplicación.
                inverseDocFreqMap.put(word, 0.0); // Por ejemplo, asignar un IDF de 0.0
            }

        }

        return inverseDocFreqMap;
    }

    //calculates Term frequency for all terms
    public HashMap<String, Double> calculateTermFrequency(HashMap<String, Integer> inputMap) {

        HashMap<String, Double> termFreqMap = new HashMap<>();
        double sum = 0.0;
        //Get the sum of all elements in hashmap
        for (float val : inputMap.values()) {
            sum += val;
        }

        //create a new hashMap with Tf values in it.
        Iterator it = inputMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            double tf = (Integer) pair.getValue() / sum;
            termFreqMap.put((pair.getKey().toString()), tf);
        }
        return termFreqMap;
    }

    //Returns if input contains numbers or not
    public boolean isDigit(String input) {
        String regex = "(.)*(\\d)(.)*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        boolean isMatched = matcher.matches();
        if (isMatched) {
            return true;
        }
        return false;
    }

    //Writes the contents of hashmap to CSV file
    public void outputAsCSV(HashMap<String, Double> treeMap, String OutputPath) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Double> keymap : treeMap.entrySet()) {
            builder.append(keymap.getKey());
            builder.append(",");
            builder.append(keymap.getValue());
            builder.append("\r\n");
        }
        String content = builder.toString().trim();
        BufferedWriter writer = new BufferedWriter(new FileWriter(OutputPath));
        writer.write(content);
        writer.close();
    }

    //cleaning up the input by removing .,:"
    public String cleanseInput(String input) {
        String newStr = input.replaceAll("[, . : ;\"]", "");
        newStr = newStr.replaceAll("\\p{P}", "");
        newStr = newStr.replaceAll("\t", "");
        return newStr;
    }

    // Converts the input text file to hashmap and even dumps the final output as CSV files
    public HashMap<String, Integer> getTermsFromFile(String Filename) {
        HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
        HashMap<String, Integer> finalMap = new HashMap<>();
        try ( BufferedReader reader = new BufferedReader(new FileReader(Filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.toLowerCase().split(" ");
                for (String term : words) {
                    term = cleanseInput(term);
                    if (!isDigit(term) && term.length() > 0) {
                        wordCount.put(term, wordCount.getOrDefault(term, 0) + 1);
                    }
                }
            }
            // sorting the hashmap
            Map<String, Integer> treeMap = new TreeMap<>(wordCount);
            finalMap = new HashMap<String, Integer>(treeMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return finalMap;
    }

    public static void main(String[] args) {
        System.out.print("Enter path for input files: ");
        Scanner scan = new Scanner(System.in);
        String folderPath = scan.nextLine();

        // Create the Akka system
        final ActorSystem system = ActorSystem.create("TFIDFSystem");

        // Create supervisor actor
        final ActorRef supervisorActor = system.actorOf(Props.create(SupervisorActor.class,
                new File(folderPath).listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File file) {
                        return !file.isHidden();
                    }
                }), new TFIDF()));

        // Start document processing
        supervisorActor.tell("start", ActorRef.noSender());

    }
}
