package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.DataImportJobsStatus;
import jhi.germinate.server.database.codegen.tables.pojos.DataImportJobs;
import jhi.germinate.server.database.codegen.tables.records.DataImportJobsRecord;
import jhi.germinate.server.database.pojo.*;
import org.jooq.DSLContext;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import java.util.logging.*;

import static jhi.germinate.server.database.codegen.tables.DataImportJobs.*;

public abstract class AbstractImporter
{
	protected SimpleDateFormat SDF_FULL_DASH  = new SimpleDateFormat("yyyy-MM-dd");
	protected SimpleDateFormat SDF_FULL       = new SimpleDateFormat("yyyyMMdd");
	protected SimpleDateFormat SDF_YEAR_MONTH = new SimpleDateFormat("yyyyMM");
	protected SimpleDateFormat SDF_YEAR_DAY   = new SimpleDateFormat("yyyydd");
	protected SimpleDateFormat SDF_YEAR       = new SimpleDateFormat("yyyy");

	protected final Integer                         importJobId;
	protected       DataImportJobs                  jobDetails;
	private         File                            inputFile;
	private         Map<ImportStatus, ImportResult> errorMap = new HashMap<>();

	private Instant start;

	public AbstractImporter(Integer importJobId)
	{
		this.importJobId = importJobId;
	}

	protected void init(String[] args)
	{
		Database.init(args[0], args[1], args[2], args[3], args[4], false);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			DataImportJobsRecord job = context.selectFrom(DATA_IMPORT_JOBS).where(DATA_IMPORT_JOBS.ID.eq(this.importJobId)).fetchAny();
			job.setStatus(DataImportJobsStatus.running);
			job.store(DATA_IMPORT_JOBS.STATUS);

			this.jobDetails = job.into(DataImportJobs.class);

			this.inputFile = new File(new File(new File(this.jobDetails.getJobConfig().getBaseFolder(), "async"), this.jobDetails.getUuid()), this.jobDetails.getJobConfig().getDataFilename());
		}
		catch (SQLException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Unable to establish database connection: " + e.getMessage());
		}
		catch (Exception e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Invalid job config or file system setup: " + e.getMessage());
		}

		start = Instant.now();
	}

	public void run()
	{
		try
		{
			RunType runtype = jobDetails.getJobConfig().getRunType();
			prepare();

			if (runtype.includesCheck()) checkFile();

			if (errorMap.size() < 1)
			{
				if (runtype.includesImport())
				{
					if (jobDetails.getIsUpdate()) updateFile();
					else importFile();

					postImport();
				}
			}
			else if (jobDetails.getJobConfig().getDeleteOnFail())
			{
				inputFile.delete();
			}

			Logger.getLogger("").log(Level.INFO, errorMap.toString());

			List<ImportResult> result = getImportResult();

			// Update the database record to indicate the job has finished running
			try (Connection conn = Database.getConnection())
			{
				DSLContext context = Database.getContext(conn);

				DataImportJobsRecord job = context.selectFrom(DATA_IMPORT_JOBS).where(DATA_IMPORT_JOBS.ID.eq(this.importJobId)).fetchAny();
				job.setFeedback(result.toArray(new ImportResult[0]));
				job.setStatus(DataImportJobsStatus.completed);
				job.store(DATA_IMPORT_JOBS.FEEDBACK, DATA_IMPORT_JOBS.STATUS);
			}
			catch (SQLException e)
			{
				addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Unable to establish database connection: " + e.getMessage());
			}

			Duration duration = Duration.between(start, Instant.now());

			Logger.getLogger("").info("DURATION: " + duration);
			System.out.println("DURATION: " + duration);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			reportError(e);
		}
	}

	protected void addImportResult(ImportStatus status, int rowIndex, String message)
	{
		if (!errorMap.containsKey(status)) errorMap.put(status, new ImportResult(status, rowIndex, message));
	}

	private List<ImportResult> getImportResult()
	{
		return new ArrayList<>(errorMap.values());
	}

	protected File getInputFile()
	{
		return this.inputFile;
	}

	protected Map<ImportStatus, ImportResult> getErrorMap()
	{
		return Collections.unmodifiableMap(this.errorMap);
	}

	protected abstract void prepare();

	protected abstract void checkFile();

	protected abstract void importFile();

	protected abstract void updateFile();

	protected abstract void postImport();

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

	protected void reportError(Exception ex)
	{
		// Update the database record to indicate the job has finished running
		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, ex.getMessage());
			List<ImportResult> result = getImportResult();

			result.add(new ImportResult(ImportStatus.GENERIC_IO_ERROR, -1, ex.getMessage()));
			DataImportJobsRecord job = context.selectFrom(DATA_IMPORT_JOBS).where(DATA_IMPORT_JOBS.ID.eq(this.importJobId)).fetchAny();
			job.setFeedback(result.toArray(new ImportResult[0]));
			job.setStatus(DataImportJobsStatus.failed);
			job.store(DATA_IMPORT_JOBS.FEEDBACK, DATA_IMPORT_JOBS.STATUS);
		}
		catch (SQLException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, "Unable to establish database connection: " + e.getMessage());
		}
	}
}
