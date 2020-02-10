package jhi.germinate.server.util.importer;

import org.dhatim.fastexcel.reader.*;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;

import jhi.germinate.resource.ImportResult;
import jhi.germinate.resource.enums.ImportStatus;
import jhi.germinate.server.util.StringUtils;

/**
 * @author Sebastian Raubach
 */
public abstract class AbstractImporter
{
	private static Map<String, List<ImportResult>> CONCURRENT_STATUS = new ConcurrentHashMap<>();
	private        SimpleDateFormat                SDF_FULL_DASH     = new SimpleDateFormat("yyyy-MM-dd");
	private        SimpleDateFormat                SDF_FULL          = new SimpleDateFormat("yyyyMMdd");
	private        SimpleDateFormat                SDF_YEAR_MONTH    = new SimpleDateFormat("yyyyMM");
	private        SimpleDateFormat                SDF_YEAR_DAY      = new SimpleDateFormat("yyyydd");
	private        SimpleDateFormat                SDF_YEAR          = new SimpleDateFormat("yyyy");

	protected boolean                         isUpdate;
	private   boolean                         deleteOnFail;
	private   File                            input;
	private   String                          uuid     = UUID.randomUUID().toString();
	private   Map<ImportStatus, ImportResult> errorMap = new HashMap<>();

	public AbstractImporter(File input, boolean isUpdate, boolean deleteOnFail)
	{
		this.input = input;
		this.isUpdate = isUpdate;
		this.deleteOnFail = deleteOnFail;
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

				Logger.getLogger("").log(Level.INFO, getImportResult().toString());
				if (errorMap.size() < 1)
				{
					if (isUpdate)
						updateFile(wb);
					else
						importFile(wb);
				}
				else if (deleteOnFail)
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

	protected BigDecimal getCellValueBigDecimal(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			return new BigDecimal(Double.parseDouble(getCellValue(r, columnNameToIndex, column)));
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Double getCellValueDouble(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			return Double.parseDouble(getCellValue(r, columnNameToIndex, column));
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Integer getCellValueInteger(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			return Integer.parseInt(getCellValue(r, columnNameToIndex, column));
		}
		catch (Exception e)
		{
			return null;
		}
	}

	protected Date getCellValueDate(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		String value = getCellValue(r, columnNameToIndex, column);

		java.util.Date date = null;
		if (!StringUtils.isEmpty(value))
		{
			if (!StringUtils.isEmpty(value))
			{
				if (value.length() == 10)
				{
					try
					{
						date = SDF_FULL_DASH.parse(value);
					}
					catch (Exception e)
					{
					}
				}
				else
				{
					// Replace all hyphens with zeros so that we only have one case to handle.
					value = value.replace("-", "0");

					try
					{
						boolean noMonth = value.substring(4, 6).equals("00");
						boolean noDay = value.substring(6, 8).equals("00");

						if (noDay && noMonth)
							date = SDF_YEAR.parse(value.substring(0, 4));
						else if (noDay)
							date = SDF_YEAR_MONTH.parse(value.substring(0, 6));
						else if (noMonth)
							date = SDF_YEAR_DAY.parse(value.substring(0, 4) + value.substring(6, 8));
						else
							date = SDF_FULL.parse(value);
					}
					catch (Exception e)
					{
					}
				}
			}
		}

		if (date != null)
			return new Date(date.getTime());
		else
			return null;
	}

	protected String getCellValue(Row r, Map<String, Integer> columnNameToIndex, String column)
	{
		try
		{
			String value = r.getCellText(columnNameToIndex.get(column));

			if (Objects.equals(value, ""))
				value = null;

			return value;
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
