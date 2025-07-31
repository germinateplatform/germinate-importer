package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.ShapefileImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "shapefile",
		description = "Import shapefile templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class ShapefileImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "shapefile"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new ShapefileImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.shapefile;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return ShapefileImporter.class;
	}
}