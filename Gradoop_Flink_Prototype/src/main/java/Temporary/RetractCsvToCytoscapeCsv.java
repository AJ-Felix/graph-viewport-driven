package Temporary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RetractCsvToCytoscapeCsv {
	public static void convert(String fromPath, String toPath) throws IOException {
		BufferedReader csvReader = new BufferedReader(new FileReader(fromPath));
		String row;
		List<String[]> list = new ArrayList<String[]>();
		while ((row = csvReader.readLine()) != null) {
		    String[] data = row.split(",");
		    list.add(data);
		}
		csvReader.close();
		List<String[]> filtered_List = new ArrayList<String[]>();
		for (String[] l1 : list) {
			if (l1[0].equals("true")) {
					Boolean add = true;
				for (String[] l2: list) {
					if (l2[4].equals(l1[4]) && l2[0].equals("false")) add = false;
				}
				if (add) filtered_List.add(l1);
			}
		}
		List<String[]> rearranged_List = new ArrayList<String[]>();
		for (String[] l: filtered_List) {
			String[] rearranged = new String[] {l[4], l[5], l[4], l[2], l[3], l[6]};
			rearranged_List.add(rearranged);
		}
		System.out.println("Vertex count is :" + rearranged_List.size());
		FileWriter csvWriter = new FileWriter(toPath);
		for (String[] rowData : rearranged_List) {
		    csvWriter.append(String.join(" ", rowData));
		    csvWriter.append("\n");
		}
		csvWriter.flush();
		csvWriter.close();
	}
}
