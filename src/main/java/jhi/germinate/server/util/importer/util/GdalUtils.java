package jhi.germinate.server.util.importer.util;

import com.google.gson.Gson;
import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.util.*;

public class GdalUtils
{
	public static GdalInfo gdalinfo(File file)
			throws IOException, InterruptedException
	{
		List<String> params = new ArrayList<>();
		params.add("gdalinfo");
		params.add("-json");
		params.add("\"" + file.getAbsolutePath() + "\"");

		if (SystemUtils.IS_OS_WINDOWS)
		{
			params.add(0, "/c");
			params.add(0, "cmd.exe");
		}

		ProcessBuilder builder = new ProcessBuilder().command(params);
		Process proc = builder.start();
		String json = captureJsonOutput(proc);
		int result = proc.waitFor();
		if (result == 0)
			return new Gson().fromJson(json, GdalInfo.class);
		else
			return null;
	}

	public static boolean gdalwarp(File input, File output)
			throws IOException, InterruptedException
	{
		List<String> params = new ArrayList<>();
		params.add("gdalwarp");
		params.add("-t_srs");
		params.add("EPSG:3857");
		params.add("-overwrite");
		params.add("-co");
		params.add("COMPRESS=DEFLATE");
		params.add("\"" + input.getAbsolutePath() + "\"");
		params.add("\"" + output.getAbsolutePath() + "\"");

		if (SystemUtils.IS_OS_WINDOWS)
		{
			params.add(0, "/c");
			params.add(0, "cmd.exe");
		}

		ProcessBuilder builder = new ProcessBuilder().command(params);

		Process proc = builder.start();
		return proc.waitFor() == 0;
	}

	public static boolean gdal_translate(File input, File output)
			throws IOException, InterruptedException
	{
		List<String> params = new ArrayList<>();

		params.add("gdal_translate");
		params.add("-a_nodata");
		params.add("0");
		params.add("-of");
		params.add("PNG");
		params.add("-co");
		params.add("WORLDFILE=YES");

		GdalInfo info = gdalinfo(input);

		// If there's no information about the size, just reduce it to 50% per dimension
		if (info == null || info.size == null || info.size.length != 2)
		{
			params.addAll(Arrays.asList("-outsize", "50%", "50%"));
		}
		else if (info.size[0] > 3000 || info.size[1] > 3000)
		{
			// Restrict the larger dimension to roughly 3000 pixels
			int max = Math.max(info.size[0], info.size[1]);
			int percent = Math.round(3000f / max * 100);
			params.addAll(Arrays.asList("-outsize", percent + "%", percent + "%"));
		}

		params.add("\"" + input.getAbsolutePath() + "\"");
		params.add("\"" + output.getAbsolutePath() + "\"");

		if (SystemUtils.IS_OS_WINDOWS)
		{
			params.add(0, "/c");
			params.add(0, "cmd.exe");
		}

		ProcessBuilder builder = new ProcessBuilder().command(params);

		Process proc = builder.start();
		return proc.waitFor() == 0;
	}

	private static String captureJsonOutput(Process process)
			throws IOException
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream())))
		{
			StringBuilder buffer = new StringBuilder();
			String line = "";

			while (line != null && !line.startsWith("{"))
				line = reader.readLine();

			do
			{
				buffer.append(line);
				line = reader.readLine();
			}
			while (line != null);

			return buffer.toString();
		}
	}

	public static class GdalInfo
	{
		private int[]  size;
		private Extent wgs84Extent;

		public int[] getSize()
		{
			return size;
		}

		public Extent getWgs84Extent()
		{
			return wgs84Extent;
		}
	}

	public static class Extent
	{
		private double[][][] coordinates;

		public double[][][] getCoordinates()
		{
			return coordinates;
		}
	}
}
