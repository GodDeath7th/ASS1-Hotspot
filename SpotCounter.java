import java.io.*;
import java.io.File;
import java.util.*;
import mpi.*;
import net.sf.json.JSONObject;
import net.sf.json.JSONArray;
public class SpotCounter {
	public static int categorise(String record, ArrayList<String> s, ArrayList<Double> d) {
		JSONObject json = JSONObject.fromObject(record);
		json = (JSONObject)json.get("doc");
		if(json.get("coordinates") != null) {
			JSONArray jArray = json.getJSONArray("coordinates");
			if(!(jArray.get(0) instanceof JSONObject) &&
			   !(jArray.get(1) instanceof JSONObject)){
				double x = Double.parseDouble(String.valueOf(jArray.get(1)));
				double y = Double.parseDouble(String.valueOf(jArray.get(0)));
				int i = 0;
				while(i < s.size()) {
					if(d.get(i*4+2)<=y && y<d.get(i*4+3) &&
					   d.get(i+4)<=x && x<d.get(i*4+1)) {
						return i;
					}
					else {
						i++;
					}
				}
			}
		}
		return -1;	
	}
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		// MPI set up and send-recv parameter configuration
		MPI.Init(args);
		int rank = MPI.COMM_WORLD.getRank();
		int size = MPI.COMM_WORLD.getSize();
		int master = 0;
		int tag = 1;
		
		// Map Hotspot Record;
		ArrayList<String> district = new ArrayList<>();
		ArrayList<Double> range = new ArrayList<>();
		ArrayList<Integer> count = new ArrayList<>();
		
		// build map information
		File map = new File("melbGrid.json");
		InputStreamReader in = new InputStreamReader(new FileInputStream(map));
		BufferedReader br = new BufferedReader(in);
		String line = "";
		String mapInfo = "";
		while((line = br.readLine()) != null) {
			mapInfo = mapInfo+line;
		}
		JSONObject json = JSONObject.fromObject(mapInfo);
		JSONArray jArray = (JSONArray)json.get("features");
		for(Object eachDistrict: jArray) {
			JSONObject districtDetail = (JSONObject)((JSONObject)eachDistrict).get("properties");
			district.add((String)districtDetail.get("id"));
			range.add((Double)districtDetail.get("xmin"));
			range.add((Double)districtDetail.get("xmax"));
			range.add((Double)districtDetail.get("ymin"));
			range.add((Double)districtDetail.get("ymax"));
			count.add(0);
			
		}
		
		File instagram = new File("tinyInstagram.json");
		in = new InputStreamReader(new FileInputStream(instagram));
		br = new BufferedReader(in);
		String insInfo = "";
		// loop for balance of process
		int loop = 1;
		while((line = br.readLine()) != null) {
			// split Instagram.json into pieces and count 
			insInfo = insInfo+line;
			if(insInfo.substring(insInfo.length()-3,insInfo.length()).equals("}},") && 
			   !insInfo.substring(insInfo.length()-7,insInfo.length()-4).equals("jpg")) {
				insInfo = insInfo.substring(0,insInfo.length()-1);
			}else if(insInfo.substring(insInfo.length()-2,insInfo.length()).equals("]}")) {
				insInfo = insInfo.substring(0, insInfo.length()-2);
			}else {
				continue;
			}
			
			if(size == 1) {
				int point = categorise(insInfo,district,range);
				if(point != -1) {
					count.set(point, count.get(point)+1);
				}
				
			}
			else{
				if(rank == master) {
					char msg[] = insInfo.toCharArray();
					int point[] = new int[1]; // the value of point defines location of new founded hotspot
					MPI.COMM_WORLD.send(msg, msg.length, MPI.CHAR, loop%size, tag);
					MPI.COMM_WORLD.recv(point, point.length, MPI.INT, loop%size, tag);
					// -1 means this record is invalid in terms of identify new hotspot
					if(point[0] != -1) {
						count.set(point[0], count.get(point[0])+1);
					}
					loop++;
					// one-round loop end
					if(loop == size) {
						loop = 1;
					}
				
				}else {
					char msg[] = new char[1000];
					String piece;
					int rev[] = new int[1];
					MPI.COMM_WORLD.recv(msg, msg.length, MPI.CHAR, master, tag);
					piece = String.valueOf(msg);
					rev[0] = categorise(piece, district, range);
					// JSON analysis
					MPI.COMM_WORLD.send(rev, rev.length, MPI.INT, master, tag);
				}
			}
			insInfo = "";
		}
		br.close();
		in.close();
		System.out.println(count);
		MPI.Finalize();
	}

}
