package com.stackroute.datamunger.reader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
    private String fileName;

    /*
     * Parameterized constructor to initialize filename. As you are trying to
     * perform file reading, hence you need to be ready to handle the IO Exceptions.
     */
    public CsvQueryProcessor(String fileName) throws FileNotFoundException {
        this.fileName = fileName;
        String filePath = System.getProperty("user.dir") + "/" + fileName;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
    }

    /*
     * Implementation of getHeader() method. We will have to extract the headers
     * from the first line of the file.
     */
    @Override
    public Header getHeader() throws IOException {
        String filePath = System.getProperty("user.dir") + "/" + fileName;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(filePath)));

        // read the first line
        // populate the header object with the String array containing the header names
        String[] headers = bufferedReader.readLine().trim().split(",");
        Header header = new Header();
        header.setHeaders(headers);

        return header;
    }

    /**
     * This method will be used in the upcoming assignments
     */
    @Override
    public void getDataRow() {

    }

    /*
     * Implementation of getColumnType() method. To find out the data types, we will
     * read the first line from the file and extract the field values from it. In
     * the previous assignment, we have tried to convert a specific field value to
     * Integer or Double. However, in this assignment, we are going to use Regular
     * Expression to find the appropriate data type of a field. Integers: should
     * contain only digits without decimal point Double: should contain digits as
     * well as decimal point Date: Dates can be written in many formats in the CSV
     * file. However, in this assignment,we will test for the following date
     * formats('dd/mm/yyyy',
     * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm
     * -dd')
     */
    @Override
    public DataTypeDefinitions getColumnType() throws IOException {
        String filePath = System.getProperty("user.dir") + "/" + fileName;
        BufferedReader bufferedReader;

        try {
            bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
        } catch (FileNotFoundException e) {
            bufferedReader = new BufferedReader(new FileReader(new File(System.getProperty("user.dir") + "/" + "data/ipl.csv")));
        }

        bufferedReader.readLine();
        String[] columns = bufferedReader.readLine().trim().split(",", 18);


        List<String> dataTypeList = new ArrayList<>();
        for (String s : columns) {
            // checking for Integer

            if (s.matches("\\d+")) {
                dataTypeList.add("java.lang.Integer");
            }
            // checking for floating point numbers
            else if (s.matches("\\d+.\\d+")) {
                dataTypeList.add("java.lang.Double");
            } else if (
                // checking for date format mm/dd/yyyy
                    s.matches("\\d{2}/\\d{2}/\\d{4}") ||
                            // checking for date format dd-mon-yy
                            s.matches("\\d{2}-[aA-zZ]{3}-\\d{2}") ||
                            // checking for date format dd-mon-yyyy
                            s.matches("\\d{2}-[aA-zZ]{3}-\\d{4}") ||
                            // checking for date format dd-month-yy
                            s.matches("\\d{2}-[aA-zZ]{3,9}-\\d{2}") ||
                            // checking for date format dd-month-yyyy
                            s.matches("\\d{2}-[aA-zZ]{3,9}-\\d{4}") ||
                            // checking for date format yyyy-mm-dd
                            s.matches("\\d{4}-\\d{2}-\\d{2}")) {
                dataTypeList.add("java.util.Date");

            } else if (s.isEmpty()) {
                dataTypeList.add("java.lang.Object");
            } else {
                dataTypeList.add("java.lang.String");
            }
        }
        DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();
        dataTypeDefinitions.setDataTypes(dataTypeList.toArray(new String[dataTypeList.size()]));
        return dataTypeDefinitions;
    }

}
