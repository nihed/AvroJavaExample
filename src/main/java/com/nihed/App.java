package com.nihed;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
        List<Schema.Field> fields = new ArrayList<Field>();
        fields.add(new Field("data", Schema.create(Type.STRING), null, null));
        fields.add(new Field("id", Schema.create(Type.INT), null, null));
        Schema schema = Schema.createRecord("Entry", null, "avro.test", false);
        schema.setFields(fields);

        File file = new File("test-file.avro");

        GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schema);
        DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(datum);

        writer.create(schema, file);

        writer.append(makeObject(schema, "{\n" +
                "\t\"id\": 2,\n" +
                "\t\"firstname\": \"nihed\",\n" +
                "\t\"lastname\": \"mbarek\",\n" +
                "\t\"adresse\": \"tunis\",\n" +
                "\t\"niv1\": {\n" +
                "\t\t\"niv11\": \"val11\",\n" +
                "\t\t\"niv12\": \"val12\"\n" +
                "\t},\n" +
                "\t\"niv2\": [{\n" +
                "\t\t\t\"a\": \"a\"\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"a\": \"b\"\n" +
                "\t\t}\n" +
                "\n" +
                "\t]\n" +
                "}", 31));

        writer.close();

        GenericDatumReader<GenericData.Record> data = new GenericDatumReader<GenericData.Record>();
        DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, data);

        GenericData.Record record = new GenericData.Record(reader.getSchema());
        while (reader.hasNext()) {
            reader.next(record);
            System.out.println("data " + record.get("data") + " id " + record.get("id"));
        }

        reader.close();


    }

    private static GenericData.Record makeObject(Schema schema, String data, int id) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("data", data);
        record.put("id", id);
        return(record);

    }
}
