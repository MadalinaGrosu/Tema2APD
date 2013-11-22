import java.awt.image.ReplicateScaleFilter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.Vector;

/**
 * Clasa Main
 */

/**
 * @author Madalina - Andreea Grosu 334CB
 *
 */
public class Main {

	/* numarul de thread-uri workeri */
	@SuppressWarnings("unused")
	private int numThreads;
	/* numele fisierului de intrare */
	private String fileIn;
	/* numele fisierului de iesire */
	@SuppressWarnings("unused")
	private String fileOut;

	public Main(int numThreads, String fileIn, String fileOut) {
		this.numThreads = numThreads;
		this.fileIn = fileIn;
		this.fileOut = fileOut;
	}

	/**
	 * @param args numarul de thread-uri workeri, numele fisierului 
	 * de intrare, numele fisierului de iesire
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		Main master = new Main(Integer.parseInt(args[0]),args[1],args[2]);
		/* numele documentului pentru care se doreste gradul de plagiere */
		String doc;
		/* dimensiunea in octeti a fragmentelor in care se impart fisierele*/
		int d = 0;
		/* pragul de similaritate */
		double x;
		/* numarul de documente */
		int docNum = 0;
		/* numele documentelor */
		String []docs = null;
		/* workpool-ul pentru operatia map */
		WorkPool mapWP = new WorkPool(master.numThreads);
		/* lista de rezultate partiale */
		HashMap<String, Vector<HashMap<String,Integer>>> list;
		list = new HashMap<String, Vector<HashMap<String,Integer>>>();
		/* lista cu workeri de tip map */
		Worker[] workers = new Worker[master.numThreads];

		try {
			Scanner in = new Scanner(new File(master.fileIn));
			doc = in.nextLine();
			d = in.nextInt();
			x = in.nextDouble();
			docNum = in.nextInt();
			docs = new String[docNum];
			in.nextLine();
			for (int i = 0; i < docNum; i++) {
				docs[i] = in.nextLine();
			}

			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < docNum; i++) {
			list.put(docs[i], new Vector<HashMap<String,Integer>>());
			FileReader inputStream = null;
			try {
				inputStream = new FileReader(docs[i]);
				char[] buf = new char[3*d/2];
				int ch;
				int len = inputStream.read(buf, 0, d);
				while (len != -1) {
					if ((buf[len-1] >= 'a' && buf[len-1] <= 'z') 
							|| (buf[len-1] >= 'A' && buf[len-1] <= 'Z')) {
						while ((ch = inputStream.read()) != -1 && (ch != ' ' 
								&& ch != '\n'))  {
							buf[len++] = (char) ch;
						}
					}
					mapWP.putWork(new PartialSolution(docs[i], buf, len));
					buf = new char[3*d/2];
					len = inputStream.read(buf, 0, d);
				} 
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		for (int i = 0; i < master.numThreads; i++) {
			workers[i] = new Worker(mapWP, list);
		}

		for (int i = 0; i < master.numThreads; i++) {
			workers[i].start();
		}


		try {
			for (int i = 0; i < master.numThreads; i++) {
				workers[i].join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* frecventele cuvintelor din documente */
		HashMap<String, HashMap<String,Float>> frequency = new HashMap<String, HashMap<String,Float>>();
		/* reduce workpool */
		WorkPool reduceWP = new WorkPool(master.numThreads);
		
		for (String keys : list.keySet()) {
			frequency.put(keys, new HashMap<String,Float>());
			reduceWP.putWork(new PartialSolution(keys, list.get(keys)));
		}
		
		Worker[] reduceWorkers = new Worker[master.numThreads];
		
		for (int i = 0; i < master.numThreads; i++) {
			reduceWorkers[i] = new Worker(reduceWP);
			reduceWorkers[i].setFrequency(frequency);
		}
		
		for (int i = 0; i < master.numThreads; i++) {
			reduceWorkers[i].start();
		}
		
		try {
			for (int i = 0; i < master.numThreads; i++) {
				reduceWorkers[i].join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("It's over!");
	}

}
