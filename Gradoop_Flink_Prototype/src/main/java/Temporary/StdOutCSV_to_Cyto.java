package Temporary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StdOutCSV_to_Cyto {
	public static void main(String[] args) throws IOException {
		String fromPath = "/home/aljoscha/out.txt";
		String toPath = "/home/aljoscha/converted.txt";
		BufferedReader csvReader = new BufferedReader(new FileReader(fromPath));
		String row;
		List<String[]> list = new ArrayList<String[]>();
		while ((row = csvReader.readLine()) != null) {
		    String[] data = row.split(" ");
		    list.add(data);
		}
		csvReader.close();
		List<String[]> filtered_list = new ArrayList<String[]>();
		for (String[] line: list) {
			Boolean bool = false;
			if (line[0].equals("true")) {
				bool = true;
				for (String[] line2: list) {
					if (line[1].equals(line2[1]) && line2[0].equals("false")) {
						bool = false;
					}
				}
			}
			if (bool) filtered_list.add(new String[]{line[1], line[2], line[3], line[4], line[5]});
		}
		FileWriter csvWriter = new FileWriter(toPath);
		for (String[] rowData : filtered_list) {
		    csvWriter.append(String.join(" ", rowData));
		    csvWriter.append("\n");
		}
		csvWriter.flush();
		csvWriter.close();
	}
}
