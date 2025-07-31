package jhi.germinate.server.util.importer.cli;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
		name = "import",
		subcommands = {
				McpdImporterCommand.class,
				GenotypeImporterCommand.class,
				PedigreeImporterCommand.class,
				GroupImporterCommand.class,
				ImageImporterCommand.class,
				TrialImporterCommand.class,
				ClimateImporterCommand.class,
				GeotiffImporterCommand.class,
				ShapefileImporterCommand.class,
		},
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class ImporterCommand implements Callable<Integer>
{
	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new ImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	public Integer call()
	{
		System.out.println("Germinate importer CLI");
		return 0;
	}
}
