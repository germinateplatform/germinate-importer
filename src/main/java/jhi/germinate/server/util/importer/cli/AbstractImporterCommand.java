package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.database.pojo.RunType;
import jhi.germinate.server.util.importer.AbstractImporter;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.*;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public abstract class AbstractImporterCommand implements Callable<Integer>
{
	@CommandLine.Option(
			names = {"-dbserver", "--database-server"},
			paramLabel = "databaseServer",
			required = true,
			description = "Name/IP of the database server"
	)
	protected String databaseServer;

	@CommandLine.Option(
			names = {"-dbname", "--database-name"},
			paramLabel = "databaseName",
			required = true,
			description = "Name of the database on the database server"
	)
	protected String databaseName;

	@CommandLine.Option(
			names = {"-dbport", "--database-port"},
			paramLabel = "databasePort",
			defaultValue = "3306",
			description = "Port the database server is listening on (default: 3306)"
	)
	protected String databasePort;

	@CommandLine.Option(
			names = {"-dbuser", "--database-username"},
			paramLabel = "databaseUsername",
			required = true,
			description = "Username of a user with permissions to access this database"
	)
	protected String databaseUsername;

	@CommandLine.Option(
			names = {"-dbpass", "--database-password"},
			paramLabel = "databasePassword",
			description = "Password of the user specified with -dbuser or --database-username"
	)
	protected String databasePassword;

	@CommandLine.ArgGroup(multiplicity = "1")
	Args args;

	static class Args
	{
		@CommandLine.ArgGroup(exclusive = false, multiplicity = "1", heading = "Existing import job args%n")
		ExistingImportJob existingImportJobArgs;

		@CommandLine.ArgGroup(exclusive = false, multiplicity = "1", heading = "New import job args%n")
		NewImportJob newImportJobArgs;
	}

	static class ExistingImportJob
	{
		@CommandLine.Option(
				names = {"-e", "--existing-import-job"},
				required = true
		)
		protected boolean existingImportJobMode;

		@CommandLine.Option(
				names = {"-jid", "--job-id"},
				paramLabel = "jobId",
				required = true,
				description = "The id of the Germinate data import job."
		)
		protected Integer jobId;
	}

	static class NewImportJob
	{
		@CommandLine.Option(
				names = {"-n", "--new-import-job"},
				required = true
		)
		protected boolean newImportJobMode;

		@CommandLine.Option(
				names = {"-germinate", "--germinate-folder"},
				paramLabel = "germinateFolder",
				required = true,
				description = "Absolute path to the folder the Germinate configuration is located in. This is the folder containing your config.properties file."
		)
		protected String germinateFolder;

		@CommandLine.Option(
				names = {"-rt", "--run-type"},
				paramLabel = "runType",
				defaultValue = "CHECK",
				description = "The run type of this import job. Either one of: ${COMPLETION-CANDIDATES}"
		)
		protected RunType runType;

		@CommandLine.Option(
				names = {"-uid", "--user-id"},
				paramLabel = "userId",
				required = true,
				description = "The id of the Germinate user this job should be imported as."
		)
		protected Integer userId;

		@CommandLine.Option(
				names = {"-i", "--input"},
				paramLabel = "inputFile",
				required = true,
				description = "Absolute path to the Germinate data template or flat input file"
		)
		protected String inputFile;
	}

	@Override
	public final Integer call()
	{
		try
		{
			// Get the class implementation
			Class<?> clazz = getImporterClass();
			AbstractImporter importer;

			// Get the job id
			Integer jobId;
			if (args.existingImportJobArgs != null && args.existingImportJobArgs.jobId != null)
				jobId = args.existingImportJobArgs.jobId;
			else
				jobId = AbstractImporter.createImportJobFromCommandline(new String[]{databaseServer, databaseName, databasePort, databaseUsername, databasePassword, args.newImportJobArgs.germinateFolder, args.newImportJobArgs.inputFile, args.newImportJobArgs.runType.name()}, getDataImportJobsDatatype());

			// Create a new instance of the importer
			Constructor<?> constructor = clazz.getConstructor(Integer.class);
			importer = (AbstractImporter) constructor.newInstance(jobId);
			importer.init(new String[]{databaseServer, databaseName, databasePort, databaseUsername, databasePassword});
			importer.run();
		}
		catch (SQLException | IOException | NoSuchMethodException | InstantiationException | IllegalAccessException |
			   InvocationTargetException e)
		{
			e.printStackTrace();
			Logger.getLogger("").severe(e.getMessage());
			return 1;
		}

		return 0;
	}

	protected abstract DataImportJobsDatatype getDataImportJobsDatatype();

	protected abstract Class<?> getImporterClass();
}
