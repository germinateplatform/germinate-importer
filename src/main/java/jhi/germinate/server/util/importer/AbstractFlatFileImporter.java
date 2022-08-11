package jhi.germinate.server.util.importer;

import jhi.germinate.server.database.pojo.ImportStatus;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public abstract class AbstractFlatFileImporter extends AbstractImporter
{
	public AbstractFlatFileImporter(Integer importJobId)
	{
		super(importJobId);
	}

	@Override
	protected final void checkFile()
	{
		try (BufferedReader br = Files.newBufferedReader(this.getInputFile().toPath(), StandardCharsets.UTF_8))
		{
			checkFile(br);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected final void importFile()
	{
		try (BufferedReader br = Files.newBufferedReader(this.getInputFile().toPath(), StandardCharsets.UTF_8))
		{
			importFile(br);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected final void updateFile()
	{
		try (BufferedReader br = Files.newBufferedReader(this.getInputFile().toPath(), StandardCharsets.UTF_8))
		{
			updateFile(br);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	protected abstract void checkFile(BufferedReader br);

	protected abstract void importFile(BufferedReader br);

	protected abstract void updateFile(BufferedReader br);
}
