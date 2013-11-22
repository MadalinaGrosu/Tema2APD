import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * Clasa ce reprezinta o solutie partiala pentru problema de rezolvat. Aceste
 * solutii partiale constituie task-uri care sunt introduse in workpool.
 */
class PartialSolution {
	/* numele fisierului */
	String filename;
	/* fragmentul din fisier de dimensiune D */
	char[] buf;
	/* setul de cuvinte cu numarul aparitiilor/frecventa */
	HashMap<String, Integer> map;
	/* numarul de octeti cititi */
	int len;
	/* vector cu asocieri cuvant - numar de aparitii */
	Vector<HashMap<String, Integer>> vector;

	public PartialSolution(String filename, char[] buf, int len) {
		this.filename = filename;
		this.buf = buf;
		this.len = len;
		map = new HashMap<String,Integer>();
	}

	public PartialSolution(String filename, HashMap<String,Integer> map) {
		this.filename = filename;
		this.map = map;
	}


	public PartialSolution(String keys, Vector<HashMap<String, Integer>> vector) {
		// TODO Auto-generated constructor stub
		this.filename = keys;
		this.vector = vector;
	}

	public String toString() {
		if (buf != null)
			return ("Numele fisierului " + filename + " Numarul de octeti " +
					len + '\n' + String.copyValueOf(buf) + '\n');
		return ("Numele fisierului " + filename + map);
	}
}

/**
 * Clasa ce reprezinta un thread worker.
 */
public class Worker extends Thread {
	WorkPool wp;
	HashMap<String, Vector<HashMap<String, Integer>>> list;
	HashMap<String, HashMap<String,Float>> frequency;

	public Worker(WorkPool workpool, HashMap<String,Vector<HashMap<String,Integer>>> list) {
		this.wp = workpool;
		this.list = list;
	}

	public Worker(WorkPool reduceWP) {
		// TODO Auto-generated constructor stub
		this.wp = reduceWP;
	}
	
	public void setFrequency(HashMap<String, HashMap<String,Float>> frequency) {
		this.frequency = frequency;
	}

	/**
	 * Procesarea unei solutii partiale. Aceasta poate implica generarea unor
	 * noi solutii partiale care se adauga in workpool folosind putWork().
	 * Daca s-a ajuns la o solutie finala, aceasta va fi afisata.
	 */
	void processPartialSolution(PartialSolution ps) {
		if (ps.buf != null) {// map
			StringTokenizer tokens = new StringTokenizer(String.copyValueOf(ps.buf),
					" ,.\n-(\t\r\f)\"");
			while (tokens.hasMoreTokens()) {
				String word = tokens.nextToken().toLowerCase().trim();
				if (!word.equals(""))
					if (ps.map.containsKey(word)) {
						int oldValue = ps.map.get(word);
						ps.map.put(word, oldValue+1);
					} else 
						ps.map.put(word,1);
			}
			Vector<HashMap<String, Integer>> v = list.get(ps.filename);
			v.add(ps.map);
			list.put(ps.filename, v);
		} else {//reduce
			int wordCount = 0;
			for (HashMap<String, Integer> map : ps.vector) {
				for (Map.Entry<String, Integer> entry : map.entrySet()) {
					float freq = entry.getValue();
					String word = entry.getKey();
					wordCount += freq;
					HashMap<String, Float> hash = frequency.get(ps.filename);
					if (hash.containsKey(word)) {
						freq += hash.get(word);
					}
					hash.put(word, freq);
					frequency.put(ps.filename, hash);
				}
			}
			
			HashMap<String,Float> freqMap = new HashMap<String,Float>();
			for (Map.Entry<String, Float> entry : 
				frequency.get(ps.filename).entrySet()) {
				if (entry.getKey().equals("apache") && ps.filename.equals("doc4.txt"))
					System.out.println("Apache = " + entry.getValue() + " wc = " + wordCount);
				float freq = (float)entry.getValue() / wordCount * 100;
				freqMap.put(entry.getKey(), freq);
			}
			frequency.put(ps.filename, freqMap);
		}
	}

	public void run() {
		System.out.println("Thread-ul worker " + this.getName() +
				" a pornit...");
		while (true) {
			PartialSolution ps = wp.getWork();
			if (ps == null)
				break;

			processPartialSolution(ps);
		}
		System.out.println("Thread-ul worker " + this.getName() 
				+ " s-a terminat...");
	}
}



