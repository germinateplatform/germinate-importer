package jhi.germinate.server.util.importer;

import org.dhatim.fastexcel.reader.*;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import jhi.germinate.resource.ImportResult;
import jhi.germinate.resource.enums.ImportStatus;

/**
 * @author Sebastian Raubach
 */
public abstract class AbstractImporter
{
	private static Map<String, List<ImportResult>> CONCURRENT_STATUS = new ConcurrentHashMap<>();

	protected boolean                         isUpdate;
	private   File                            input;
	private   String                          uuid     = UUID.randomUUID().toString();
	private   Map<ImportStatus, ImportResult> errorMap = new HashMap<>();

	public AbstractImporter(File input, boolean isUpdate)
	{
		this.input = input;
		this.isUpdate = isUpdate;
	}

	public static synchronized List<ImportResult> getStatus(String uuid)
	{
		if (CONCURRENT_STATUS.containsKey(uuid))
		{
			List<ImportResult> result = CONCURRENT_STATUS.get(uuid);
			// If the key is there return the value (null or finished result)
			return result.size() == 0 ? null : result;
		}
		else
		{
			// Else throw a not found
			throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND);
		}
	}

	public String run()
	{
		// Put an empty list to indicate that the UUID is valid, but not finished
		CONCURRENT_STATUS.put(uuid, new ArrayList<>());
		new Thread(() -> {
			try (ReadableWorkbook wb = new ReadableWorkbook(input))
			{
				prepare();

				checkFile(wb);

				if (errorMap.size() < 1)
				{
					if(isUpdate)
						updateFile(wb);
					else
						importFile(wb);
				}
				else
				{
					input.delete();
				}

				// Put the result
				CONCURRENT_STATUS.put(uuid, getImportResult());
			}
			catch (Exception e)
			{
				e.printStackTrace();
				CONCURRENT_STATUS.put(uuid, Collections.singletonList(new ImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage())));
			}
		}).start();

		return uuid;
	}

	protected String getCellValue(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			return r.getCellText(columnNameToIndex.get(column));
		}
		catch (Exception e)
		{
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, r.getRowNum(), "Column missing: '" + column + "'");
			return null;
		}
	}

	protected void addImportResult(ImportStatus status, int rowIndex, String message)
	{
		if (!errorMap.containsKey(status))
			errorMap.put(status, new ImportResult(status, rowIndex, message));
	}

	private List<ImportResult> getImportResult()
	{
		return new ArrayList<>(errorMap.values());
	}

	protected abstract void prepare();

	protected abstract void checkFile(ReadableWorkbook wb);

	protected abstract void importFile(ReadableWorkbook wb);

	protected abstract void updateFile(ReadableWorkbook wb);
}
