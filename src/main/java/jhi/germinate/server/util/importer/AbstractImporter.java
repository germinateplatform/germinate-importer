package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.*;
import jhi.germinate.server.database.codegen.tables.pojos.DataImportJobs;
import jhi.germinate.server.database.codegen.tables.records.DataImportJobsRecord;
import jhi.germinate.server.database.pojo.*;
import org.jooq.DSLContext;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;
import java.util.logging.*;

import static jhi.germinate.server.database.codegen.tables.DataImportJobs.DATA_IMPORT_JOBS;

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
	private         List<ImportResult>              errorList      = new ArrayList<>();
	private         Set<ImportStatus>               errorSet       = new HashSet<>();
	private         String[]                        args;
	protected       ImportJobStats                  importJobStats = new ImportJobStats();

	private Instant start;

	public AbstractImporter(Integer importJobId)
	{
		this.importJobId = importJobId;
	}

	protected String[] getArgs()
	{
		return Arrays.copyOf(args, args.length);
	}

	public static Integer createImportJobFromCommandline(String[] args, DataImportJobsDatatype type)
			throws IOException, SQLException
	{
		Database.init(args[0], args[1], args[2], args[3], args[4], false);

		File germinateFolder = new File(args[5]);
		if (!germinateFolder.exists())
			throw new IOException("Specified Germinate folder does not exist.");

		File configFile = new File(germinateFolder, "config.properties");
		if (!configFile.exists())
			throw new IOException("config.properties file not found in the specified Germinate folder. Make sure you specified the correct location of the Germinate folder");

		File inputFile = new File(args[6]);
		if (!inputFile.exists())
			throw new IOException("Specified input file does not exist.");

		RunType runType;

		try
		{
			runType = RunType.getType(args[7]);
		}
		catch (Exception e)
		{
			throw new IOException("Invalid run type specified: '" + args[7] + "'.");
		}

		int userId;

		try
		{
			userId = Integer.parseInt(args[8]);
		}
		catch (Exception e)
		{
			throw new IOException("Invalid user id specified: " + args[8] + ".");
		}

		String[] parts = inputFile.getName().split("\\.");
		String extension = parts[parts.length - 1];

		String uuid = UUID.randomUUID().toString();

		File jobFolder = new File(new File(germinateFolder, "async"), uuid);
		jobFolder.mkdirs();

		File targetFile = new File(jobFolder, uuid + "." + extension);

		// Copy input file to its location
		Files.copy(inputFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

		try (Connection conn = Database.getConnection())
		{
			DSLContext context = Database.getContext(conn);

			ImportJobDetails details = new ImportJobDetails();
			details.setDeleteOnFail(true);
			details.setBaseFolder(germinateFolder.getAbsolutePath());
			details.setDataFilename(targetFile.getName());
			details.setRunType(runType);

			DataImportJobsRecord job = context.newRecord(DATA_IMPORT_JOBS);
			job.setDatatype(type);
			job.setDatasetstateId(1);
			job.setUuid(uuid);
			job.setStatus(DataImportJobsStatus.waiting);
			job.setImported(false);
			job.setIsUpdate(false);
			job.setJobId("1");
			job.setUserId(userId);
			job.setOriginalFilename(inputFile.getName());
			job.setJobConfig(details);
			job.setCreatedOn(new Timestamp(System.currentTimeMillis()));
			job.setUpdatedOn(new Timestamp(System.currentTimeMillis()));
			job.store();

			return job.getId();
		}
	}

	public void init(String[] args)
	{
		this.args = args;
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

			if (!hasImportError())
			{
				if (runtype.includesImport())
				{
					if (jobDetails.getIsUpdate())
						updateFile();
					else
						importFile();

					postImport();
				}
			}
			else if (jobDetails.getJobConfig().getDeleteOnFail())
			{
				inputFile.delete();
			}

			Logger.getLogger("").log(Level.INFO, errorList.toString());

			List<ImportResult> result = getImportResult();

			// Update the database record to indicate the job has finished running
			try (Connection conn = Database.getConnection())
			{
				DSLContext context = Database.getContext(conn);

				DataImportJobsRecord job = context.selectFrom(DATA_IMPORT_JOBS).where(DATA_IMPORT_JOBS.ID.eq(this.importJobId)).fetchAny();
				job.setFeedback(result.toArray(new ImportResult[0]));
				job.setStatus(DataImportJobsStatus.completed);
				job.setStats(importJobStats);
				job.store(DATA_IMPORT_JOBS.FEEDBACK, DATA_IMPORT_JOBS.STATUS, DATA_IMPORT_JOBS.STATS);
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

	protected boolean hasImportError()
	{
		return errorList.stream().anyMatch(r -> r.getType() == ImportResult.StatusType.ERROR);
	}

	protected void addImportResult(ImportStatus status, int rowIndex, String message)
	{
		if (!status.isAllowsMultiple() && errorSet.contains(status))
			return;

		errorSet.add(status);
		errorList.add(new ImportResult(status, rowIndex, message));
	}

	protected void addImportResult(ImportStatus status, int rowIndex, String message, ImportResult.StatusType type)
	{
		if (!status.isAllowsMultiple() && errorSet.contains(status))
			return;

		errorSet.add(status);
		errorList.add(new ImportResult(status, rowIndex, message, type));
	}

	protected List<ImportResult> getImportResult()
	{
		return new ArrayList<>(errorList);
	}

	protected File getInputFile()
	{
		return this.inputFile;
	}

	protected void setInputFile(File file)
	{
		this.inputFile = file;
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
