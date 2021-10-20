package jhi.germinate.server.util.importer.task;

import jhi.germinate.server.database.pojo.ImportStatus;

public interface ErrorCallback
{
	void onError(ImportStatus status, int rowIndex, String message);
}
