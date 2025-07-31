package jhi.germinate.server.util.importer.cli;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
		name = "genotype",
		subcommands = {
				ExcelGenotypeImporterCommand.class,
				HapmapGenotypeImporterCommand.class,
				FlatFileGenotypeImporterCommand.class,
		},
		mixinStandardHelpOptions = true,
		versionProvider = jhi.germinate.GerminateCommandVersion.class
)
public class GenotypeImporterCommand implements Callable<Integer>
{
	public static void main(String[] args)
	{
		int exitCode = new CommandLine(new GenotypeImporterCommand()).execute(args);
		System.exit(exitCode);
	}

	@Override
	public Integer call()
	{
		System.out.println("Germinate genotype importer CLI");
		return 0;
	}
}
