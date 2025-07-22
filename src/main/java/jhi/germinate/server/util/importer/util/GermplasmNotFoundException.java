package jhi.germinate.server.util.importer.util;

import jhi.germinate.server.database.pojo.ImportStatus;

public class GermplasmNotFoundException extends RuntimeException
{
	private ImportStatus reason;

	public GermplasmNotFoundException(ImportStatus reason)
	{
		this.reason = reason;
	}

	public ImportStatus getReason()
	{
		return reason;
	}
}