package jhi.germinate.server.util.importer;

import com.google.gson.Gson;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.pojo.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import java.util.logging.*;

public abstract class AbstractImporter
{
	protected SimpleDateFormat SDF_FULL_DASH  = new SimpleDateFormat("yyyy-MM-dd");
	protected SimpleDateFormat SDF_FULL       = new SimpleDateFormat("yyyyMMdd");
	protected SimpleDateFormat SDF_YEAR_MONTH = new SimpleDateFormat("yyyyMM");
	protected SimpleDateFormat SDF_YEAR_DAY   = new SimpleDateFormat("yyyydd");
	protected SimpleDateFormat SDF_YEAR       = new SimpleDateFormat("yyyy");

	protected       boolean                         isUpdate;
	private         boolean                         deleteOnFail;
	private         File                            input;
	protected final int                             userId;
	private         Map<ImportStatus, ImportResult> errorMap = new HashMap<>();

	private Instant start;

	public AbstractImporter(File input, boolean isUpdate, boolean deleteOnFail, int userId)
	{
		this.input = input;
		this.isUpdate = isUpdate;
		this.deleteOnFail = deleteOnFail;
		this.userId = userId;
	}

	protected void init(String[] args)
	{
		Database.init(args[0], args[1], args[2], args[3], args[4], false);

		start = Instant.now();
	}

	public void run(RunType runtype)
	{
		prepare();

		if (runtype.includesCheck())
			checkFile();

		Logger.getLogger("").log(Level.INFO, errorMap.toString());

		if (errorMap.size() < 1)
		{
			if (runtype.includesImport())
			{
				if (isUpdate)
					updateFile();
				else
					importFile();
			}
		}
		else if (deleteOnFail)
		{
			input.delete();
		}

		List<ImportResult> result = getImportResult();

		String jsonFilename = this.input.getName().substring(0, this.input.getName().lastIndexOf("."));
		File json = new File(input.getParent(), jsonFilename + ".json");
		try
		{
			Files.write(json.toPath(), Collections.singletonList(new Gson().toJson(result)), StandardCharsets.UTF_8);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		Duration duration = Duration.between(start, Instant.now());

		Logger.getLogger("").info("DURATION: " + duration);
		System.out.println("DURATION: " + duration);
	}

	protected void addImportResult(ImportStatus status, int rowIndex, String message)
	{
		if (!errorMap.containsKey(status))
			errorMap.put(status, new ImportResult(status, rowIndex, message));
	}

	private List<ImportResult> getImportResult()
	{
		return new ArrayList<>(errorMap.values());
	}

	protected File getInputFile()
	{
		return this.input;
	}

	protected Map<ImportStatus, ImportResult> getErrorMap() {
		return Collections.unmodifiableMap(this.errorMap);
	}

	protected abstract void prepare();

	protected abstract void checkFile();

	protected abstract void importFile();

	protected abstract void updateFile();

	public static class ImportConfig
	{
		private String uuid;
		private Thread thread;

		public ImportConfig(String uuid, Thread thread)
		{
			this.uuid = uuid;
			this.thread = thread;
		}

		public String getUuid()
		{
			return uuid;
		}

		public ImportConfig setUuid(String uuid)
		{
			this.uuid = uuid;
			return this;
		}

		public Thread getThread()
		{
			return thread;
		}

		public ImportConfig setThread(Thread thread)
		{
			this.thread = thread;
			return this;
		}
	}

	public enum RunType
	{
		CHECK,
		IMPORT,
		CHECK_AND_IMPORT;

		public boolean includesCheck()
		{
			return this == CHECK || this == CHECK_AND_IMPORT;
		}

		public boolean includesImport()
		{
			return this == IMPORT || this == CHECK_AND_IMPORT;
		}

		public static RunType getType(String input)
		{
			try
			{
				return RunType.valueOf(input);
			}
			catch (Exception e)
			{
				return CHECK;
			}
		}
	}
}
