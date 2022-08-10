package jhi.germinate.server.util.importer.task;

import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.hdf5.FJTabbedToHdf5Converter;

import java.io.*;
import java.nio.file.*;
import java.util.logging.Logger;

public abstract class FJTabbedToHdf5Task implements Runnable
{
	private final File          input;
	private final File          hdf5;
	private final boolean       transpose;
	private final ErrorCallback callback;
	private       int           skipLines;

	public FJTabbedToHdf5Task(File input, File hdf5, boolean transpose, int skipLines, ErrorCallback callback)
	{
		this.input = input;
		this.hdf5 = hdf5;
		this.transpose = transpose;
		this.skipLines = skipLines;
		this.callback = callback;
	}

	@Override
	public void run()
	{
		try
		{
			File temp = Files.createTempFile(input.getName(), ".temp").toFile();

			FJTabbedToHdf5Converter converter = new FJTabbedToHdf5Converter(input, temp);
			// Tell it to skip the map definition. It skips the other headers automatically anyway.
			converter.setSkipLines(this.skipLines);
			converter.setTranspose(transpose);
			converter.convertToHdf5();

			Files.move(temp.toPath(), hdf5.toPath(), StandardCopyOption.REPLACE_EXISTING);

			Logger.getLogger("").info("HDF5 file written to: " + hdf5.getAbsolutePath() + " " + hdf5.exists() + " " + hdf5.length());
		}
		catch (IOException e)
		{
			e.printStackTrace();
			Logger.getLogger("").severe(e.getMessage());
			callback.onError(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
		finally
		{
			this.onFinished();
		}
	}

	protected abstract void onFinished();
}
