package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.GeotiffImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "geotiff",
		description = "Import geotiff templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class GeotiffImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "geotiff"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new GeotiffImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.geotiff;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return GeotiffImporter.class;
	}
}