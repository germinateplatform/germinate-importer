package jhi.germinate.server.util.importer.task;

import jhi.germinate.server.database.pojo.ImportStatus;
import jhi.germinate.server.util.hdf5.*;

import java.io.*;
import java.nio.file.*;
import java.util.logging.Logger;

public abstract class HapmapToHdf5Task implements Runnable
{
	private final File          hapmap;
	private final File          hdf5;
	private final boolean transpose;
	private final ErrorCallback callback;

	public HapmapToHdf5Task(File hapmap, File hdf5, boolean transpose, ErrorCallback callback)
	{
		this.hapmap = hapmap;
		this.hdf5 = hdf5;
		this.transpose = transpose;
		this.callback = callback;
	}

	@Override
	public void run()
	{
		try
		{
			File temp = Files.createTempFile(hapmap.getName(), ".temp").toFile();

			HapmapToHdf5Converter converter = new HapmapToHdf5Converter(hapmap, temp);
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
