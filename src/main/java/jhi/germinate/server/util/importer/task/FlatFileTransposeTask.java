package jhi.germinate.server.util.importer.task;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class FlatFileTransposeTask implements Runnable
{
	private static final int CACHELINES = 5000;

	private List<String[]> data    = new ArrayList<>(CACHELINES);
	private int            maxCols = 0;
	private List<File>     cache   = new ArrayList<>();

	private static File folder;

	private final File input;
	private final File output;

	private List<String> prefix;

	public FlatFileTransposeTask(File input, File output, List<String> prefix)
	{
		this.input = input;
		this.output = output;
		this.prefix = prefix;
	}

	@Override
	public void run()
	{
		try
		{
			folder = Files.createTempDirectory("transpose").toFile();
			folder.deleteOnExit();

			readData();
			writeData();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private void readData()
			throws IOException
	{
		try (BufferedReader in = new BufferedReader(new FileReader(input)))
		{
			String str;
			int line = 0;
			long s = System.currentTimeMillis();
			while (((str = in.readLine()) != null) && (str.length() > 0))
			{
				this.data.add(str.split("\t", -1));
				line++;
				if (this.data.size() == CACHELINES)
				{
					System.out.println(line + " - read:  " + (System.currentTimeMillis() - s) + "ms");

					s = System.currentTimeMillis();
					writeCache();
					System.out.println(line + " - cache: " + (System.currentTimeMillis() - s) + "ms");

					s = System.currentTimeMillis();
				}
			}
			writeCache();
		}
	}

	private void writeCache()
			throws IOException
	{
		int cols = this.data.get(0).length;
		this.maxCols = cols;
		if (this.cache.size() == 0)
		{
			for (int i = 0; i < cols; i++)
			{
				File file = new File(folder, "_transpose_temp_" + i);
				file.delete();

				this.cache.add(file);
			}
		}
		for (int i = 0; i < cols; i++)
		{
			try (BufferedWriter out = new BufferedWriter(new FileWriter(cache.get(i), true)))
			{
				for (String[] aData : data)
				{
					try
					{
						out.write(aData[i] + "\t");
					}
					catch (ArrayIndexOutOfBoundsException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		data.clear();
	}

	private void writeData()
			throws IOException
	{
		try (BufferedWriter out = new BufferedWriter(new FileWriter(output)))
		{
			if (prefix != null)
			{
				for (String p : prefix)
				{
					out.write(p);
					out.newLine();
				}
			}

			for (int i = 0; i < maxCols; i++)
			{
				try (BufferedReader in = new BufferedReader(new FileReader(cache.get(i))))
				{
					String line = in.readLine();
					out.write(line.substring(0, line.length() - 1));
					if (i < maxCols - 1)
						out.newLine();
				}

				cache.get(i).delete();
			}
		}
	}
}
