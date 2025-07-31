package jhi.germinate.server.util.importer.cli;

import jhi.germinate.server.database.codegen.enums.DataImportJobsDatatype;
import jhi.germinate.server.util.importer.ImageImporter;
import picocli.CommandLine;

@CommandLine.Command(
		name = "image",
		description = "Import image Excel data templates",
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class ImageImporterCommand extends AbstractImporterCommand
{
	public static final String[] CMD_ARGS = {"import", "image"};

	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new ImageImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	protected DataImportJobsDatatype getDataImportJobsDatatype()
	{
		return DataImportJobsDatatype.images;
	}

	@Override
	protected Class<?> getImporterClass()
	{
		return ImageImporter.class;
	}
}