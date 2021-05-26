package jhi.germinate.server.util.importer;

import jhi.germinate.server.Database;
import jhi.germinate.server.database.codegen.enums.AttributesDatatype;
import jhi.germinate.server.database.codegen.tables.records.*;
import jhi.germinate.server.database.pojo.*;
import jhi.germinate.server.util.StringUtils;
import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

import static jhi.germinate.server.database.codegen.tables.Attributedata.*;
import static jhi.germinate.server.database.codegen.tables.Attributes.*;
import static jhi.germinate.server.database.codegen.tables.Collaborators.*;
import static jhi.germinate.server.database.codegen.tables.Countries.*;
import static jhi.germinate.server.database.codegen.tables.Datasetcollaborators.*;
import static jhi.germinate.server.database.codegen.tables.Datasetlocations.*;
import static jhi.germinate.server.database.codegen.tables.Datasets.*;
import static jhi.germinate.server.database.codegen.tables.Experiments.*;
import static jhi.germinate.server.database.codegen.tables.Institutions.*;
import static jhi.germinate.server.database.codegen.tables.Locations.*;

/**
 * @author Sebastian Raubach
 */
public abstract class DatasheetImporter extends AbstractImporter
{
	private static final String[] METADATA_LABELS = {"Title", "Description", "Rights", "Date of creation", "Publisher", "Format", "Language", "Source", "Type", "Subject", "Contact"};

	protected DatasetsRecord       dataset;
	protected Map<String, Integer> locationNameToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	private   Map<String, Integer> countryCode2ToId;
	protected Map<String, Integer> metadataLabelToRowIndex;
	private   Map<String, Integer> attributeToId = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

	public DatasheetImporter(File input, boolean isUpdate, boolean deleteOnFail, int userId)
	{
		super(input, isUpdate, deleteOnFail, userId);
	}

	@Override
	protected void prepare()
	{
		try (DSLContext context = Database.getContext())
		{
			countryCode2ToId = context.selectFrom(COUNTRIES)
									  .fetchMap(COUNTRIES.COUNTRY_CODE2, COUNTRIES.ID);

			context.selectFrom(ATTRIBUTES)
				   .where(ATTRIBUTES.TARGET_TABLE.eq("datasets"))
				   .forEach(a -> attributeToId.put(a.getName(), a.getId()));
		}
	}

	@Override
	protected void checkFile(ReadableWorkbook wb)
	{
		wb.findSheet("METADATA")
		  .ifPresent(this::checkMetadataSheet);

		wb.findSheet("LOCATION")
		  .ifPresent(this::checkLocationSheet);

		wb.findSheet("ATTRIBUTES")
		  .ifPresent(this::checkAttributeSheet);

		wb.findSheet("COLLABORATORS")
		  .ifPresent(this::checkCollaboratorsSheet);
	}

	private void checkAttributeSheet(Sheet s)
	{
		try
		{
			s.openStream()
			 .findFirst()
			 .ifPresent(r -> {
				 if (allCellsEmpty(r))
					 return;

				 if (!Objects.equals(getCellValue(r, 0), "Attribute"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Attribute");
				 if (!Objects.equals(getCellValue(r, 1), "Type"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 1, "Type");
				 if (!Objects.equals(getCellValue(r, 2), "Value"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 2, "Value");
			 });

			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String attribute = getCellValue(r, 0);
				 String dataType = getCellValue(r, 1);
				 AttributesDatatype dt = null;
				 try
				 {
					 dt = AttributesDatatype.valueOf(dataType);
				 }
				 catch (Exception e)
				 {
				 }

				 String value = getCellValue(r, 2);

				 if (dt == null && StringUtils.isEmpty(attribute) && StringUtils.isEmpty(value))
				 	return;

				 if (dt == null)
					 addImportResult(ImportStatus.GENERIC_INVALID_DATATYPE, r.getRowNum(), "Data Type: " + dataType);
				 if (StringUtils.isEmpty(attribute))
					 addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Attribute");
				 if (StringUtils.isEmpty(value))
					 addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Value");
			 });
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void checkCollaboratorsSheet(Sheet s)
	{
		try
		{
			s.openStream()
			 .findFirst()
			 .ifPresent(r -> {
				 if (allCellsEmpty(r))
					 return;

				 if (!Objects.equals(getCellValue(r, 0), "Last Name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Last Name");
				 if (!Objects.equals(getCellValue(r, 1), "First Name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "First Name");
				 if (!Objects.equals(getCellValue(r, 2), "Email"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Email");
				 if (!Objects.equals(getCellValue(r, 3), "Phone"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Phone");
				 if (!Objects.equals(getCellValue(r, 4), "Contributor"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Contributor");
				 if (!Objects.equals(getCellValue(r, 5), "Address"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Address");
				 if (!Objects.equals(getCellValue(r, 6), "Country"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Country");
			 });

			s.openStream()
			 .skip(2)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String firstName = getCellValue(r, 0);
				 String lastName = getCellValue(r, 1);
				 String country = getCellValue(r, 6);

				 if (StringUtils.isEmpty(firstName))
					 addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "First Name");
				 if (StringUtils.isEmpty(lastName))
					 addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Last Name");
				 if (!StringUtils.isEmpty(country) && !countryCode2ToId.containsKey(country))
					 addImportResult(ImportStatus.GENERIC_INVALID_COUNTRY_CODE, r.getRowNum(), country);
			 });
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void checkMetadataSheet(Sheet s)
	{
		try
		{
			s.openStream()
			 .findFirst()
			 .ifPresent(this::checkMetadataHeaders);

			checkMetadataLabels(s);
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	protected List<String> checkLocationSheet(Sheet s)
	{
		try
		{
			List<String> result = new ArrayList<>();
			// Check the headers
			s.openStream()
			 .findFirst()
			 .ifPresent(r -> {
				 if (!Objects.equals(getCellValue(r, 0), "Name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Name");
				 if (!Objects.equals(getCellValue(r, 1), "Short name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Short name");
				 if (!Objects.equals(getCellValue(r, 2), "Country"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Country");
				 if (!Objects.equals(getCellValue(r, 3), "Elevation"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Elevation");
				 if (!Objects.equals(getCellValue(r, 4), "Latitude"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Latitude");
				 if (!Objects.equals(getCellValue(r, 5), "Longitude"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Longitude");
			 });
			// Check the data
			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String name = getCellValue(r, 0);
				 String shortName = getCellValue(r, 1);
				 String country = getCellValue(r, 2);
				 String elevation = getCellValue(r, 3);
				 String latitude = getCellValue(r, 4);
				 String longitude = getCellValue(r, 5);

				 result.add(name);

				 if (StringUtils.isEmpty(name))
					 addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, r.getRowNum(), "Location Name");
				 if (!StringUtils.isEmpty(shortName) && shortName.length() > 22)
					 addImportResult(ImportStatus.GENERIC_VALUE_TOO_LONG, r.getRowNum(), "Location Short Name: " + shortName + " is longer than 22 characters.");
				 if (!StringUtils.isEmpty(country) && !countryCode2ToId.containsKey(country))
					 addImportResult(ImportStatus.GENERIC_INVALID_COUNTRY_CODE, r.getRowNum(), country);
				 if (!StringUtils.isEmpty(elevation))
				 {
					 try
					 {
						 Double.parseDouble(elevation);
					 }
					 catch (Exception e)
					 {
						 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), elevation);
					 }
				 }
				 if (!StringUtils.isEmpty(latitude))
				 {
					 try
					 {
						 Double.parseDouble(latitude);
					 }
					 catch (Exception e)
					 {
						 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), latitude);
					 }
				 }
				 if (!StringUtils.isEmpty(longitude))
				 {
					 try
					 {
						 Double.parseDouble(longitude);
					 }
					 catch (Exception e)
					 {
						 addImportResult(ImportStatus.GENERIC_INVALID_NUMBER, r.getRowNum(), longitude);
					 }
				 }
			 });

			return result;
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}

		return null;
	}

	private void checkMetadataHeaders(Row r)
	{
		if (!Objects.equals(getCellValue(r, 0), "LABEL"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "LABEL");
		if (!Objects.equals(getCellValue(r, 1), "DEFINITION"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "DEFINITION");
		if (!Objects.equals(getCellValue(r, 2), "VALUE"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "VALUE");
	}

	protected void checkMetadataLabels(Sheet s)
	{
		try
		{
			List<Row> rows = s.read();
			readMetadataLabels(rows);

			Arrays.stream(METADATA_LABELS)
				  .forEachOrdered(c -> {
					  if (!metadataLabelToRowIndex.containsKey(c))
						  addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, -1, c);
				  });

			Integer index = metadataLabelToRowIndex.get("Title");
			if (index != null)
			{
				String name = getCellValue(rows.get(index), 0);
				if (StringUtils.isEmpty(name))
					addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, index, "Title");
			}

			index = metadataLabelToRowIndex.get("Description");
			if (index != null)
			{
				String description = getCellValue(rows.get(index), 0);
				if (StringUtils.isEmpty(description))
					addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, index, "Description");
			}
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void readMetadataLabels(List<Row> rows)
	{
		metadataLabelToRowIndex = new HashMap<>();

		for (int i = 1; i < rows.size(); i++)
		{
			Row r = rows.get(i);

			if (allCellsEmpty(r))
				break;

			String label = getCellValue(r, 0);
			metadataLabelToRowIndex.put(label, i);
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		try (DSLContext context = Database.getContext())
		{
			wb.findSheet("METADATA")
			  .ifPresent(s -> {
				  try
				  {
					  List<Row> rows = s.read();
					  readMetadataLabels(rows);
				  }
				  catch (IOException e)
				  {
					  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
				  }
			  });

			getOrCreateLocations(context, wb);
			getOrCreateDataset(context, wb);
			getOrCreateAttributes(context, wb);
			getOrCreateCollaborators(context, wb);
		}
	}

	private void getOrCreateAttributes(DSLContext context, ReadableWorkbook wb)
	{
		wb.findSheet("ATTRIBUTES")
		  .ifPresent(s -> {
			  try
			  {
				  s.openStream()
				   .skip(1)
				   .forEachOrdered(r -> {
					   if (allCellsEmpty(r))
						   return;

					   String attribute = getCellValue(r, 0);
					   String dataType = getCellValue(r, 1);
					   AttributesDatatype dt = null;
					   try
					   {
						   dt = AttributesDatatype.valueOf(dataType);
					   }
					   catch (Exception e)
					   {
					   }
					   String value = getCellValue(r, 2);

					   if (dt == null && StringUtils.isEmpty(attribute) && StringUtils.isEmpty(value))
						   return;

					   Integer attributeId = attributeToId.get(attribute);

					   if (attributeId == null)
					   {
						   AttributesRecord a = context.newRecord(ATTRIBUTES);
						   a.setName(attribute);
						   a.setTargetTable("datasets");
						   a.setDatatype(dt);
						   a.setCreatedOn(new Timestamp(System.currentTimeMillis()));
						   a.store();

						   attributeId = a.getId();
						   attributeToId.put(a.getName(), a.getId());
					   }

					   AttributedataRecord ad = context.newRecord(ATTRIBUTEDATA);
					   ad.setAttributeId(attributeId);
					   ad.setValue(value);
					   ad.setForeignId(dataset.getId());
					   ad.setCreatedOn(new Timestamp(System.currentTimeMillis()));
					   ad.store();
				   });
			  }
			  catch (IOException e)
			  {
				  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			  }
		  });
	}

	private void getOrCreateCollaborators(DSLContext context, ReadableWorkbook wb)
	{
		wb.findSheet("COLLABORATORS")
		  .ifPresent(s -> {
			  try
			  {
				  s.openStream()
				   .skip(2)
				   .forEachOrdered(r -> {
					   if (allCellsEmpty(r))
						   return;

					   String lastName = getCellValue(r, 0);
					   String firstName = getCellValue(r, 1);
					   String email = getCellValue(r, 2);
					   String phone = getCellValue(r, 3);
					   String institutionName = getCellValue(r, 4);
					   String address = getCellValue(r, 5);
					   String countryCode = getCellValue(r, 6);

					   Integer countryId = countryCode2ToId.get(countryCode);

					   InstitutionsRecord institution = context.selectFrom(INSTITUTIONS)
															   .where(INSTITUTIONS.NAME.isNotDistinctFrom(institutionName))
															   .and(INSTITUTIONS.ADDRESS.isNotDistinctFrom(address))
															   .and(INSTITUTIONS.COUNTRY_ID.isNotDistinctFrom(countryId))
															   .fetchAny();

					   if (!StringUtils.isEmpty(institutionName) && institution == null)
					   {
						   institution = context.newRecord(INSTITUTIONS);
						   institution.setName(institutionName);
						   institution.setAddress(address);
						   institution.setCountryId(countryId);
						   institution.setCreatedOn(new Timestamp(System.currentTimeMillis()));
						   institution.store();
					   }

					   CollaboratorsRecord collaborator = context.selectFrom(COLLABORATORS)
																 .where(COLLABORATORS.FIRST_NAME.isNotDistinctFrom(firstName))
																 .and(COLLABORATORS.LAST_NAME.isNotDistinctFrom(lastName))
																 .and(COLLABORATORS.EMAIL.isNotDistinctFrom(email))
																 .and(COLLABORATORS.PHONE.isNotDistinctFrom(phone))
																 .and(COLLABORATORS.INSTITUTION_ID.isNotDistinctFrom(institution == null ? null : institution.getId()))
																 .fetchAny();

					   if (collaborator == null)
					   {
						   collaborator = context.newRecord(COLLABORATORS);
						   collaborator.setFirstName(firstName);
						   collaborator.setLastName(lastName);
						   collaborator.setEmail(email);
						   collaborator.setPhone(phone);
						   collaborator.setInstitutionId(institution == null ? null : institution.getId());
						   collaborator.setCreatedOn(new Timestamp(System.currentTimeMillis()));
						   collaborator.store();
					   }

					   DatasetcollaboratorsRecord dsCollab = context.selectFrom(DATASETCOLLABORATORS)
																	.where(DATASETCOLLABORATORS.DATASET_ID.isNotDistinctFrom(dataset.getId()))
																	.and(DATASETCOLLABORATORS.COLLABORATOR_ID.isNotDistinctFrom(collaborator.getId()))
																	.fetchAny();

					   if (dsCollab == null)
					   {
						   dsCollab = context.newRecord(DATASETCOLLABORATORS);
						   dsCollab.setDatasetId(dataset.getId());
						   dsCollab.setCollaboratorId(collaborator.getId());
						   dsCollab.setCreatedOn(new Timestamp(System.currentTimeMillis()));
						   dsCollab.store();
					   }
				   });
			  }
			  catch (IOException e)
			  {
				  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			  }
		  });
	}

	private void getOrCreateLocations(DSLContext context, ReadableWorkbook wb)
	{
		wb.findSheet("LOCATION")
		  .ifPresent(s -> {
			  try
			  {
				  s.openStream()
				   .skip(1)
				   .forEachOrdered(r -> {
					   if (allCellsEmpty(r))
						   return;

					   String name = getCellValue(r, 0);
					   String shortName = getCellValue(r, 1);
					   String country = getCellValue(r, 2);
					   Integer countryId = countryCode2ToId.get(country);
					   BigDecimal ele = getCellValueBigDecimal(r, 3);
					   BigDecimal lat = getCellValueBigDecimal(r, 4);
					   BigDecimal lng = getCellValueBigDecimal(r, 5);

					   LocationsRecord location = context.selectFrom(LOCATIONS)
														 .where(LOCATIONS.SITE_NAME.isNotDistinctFrom(name))
														 .and(LOCATIONS.SITE_NAME_SHORT.isNotDistinctFrom(shortName))
														 .and(LOCATIONS.COUNTRY_ID.isNotDistinctFrom(countryId))
														 .and(LOCATIONS.ELEVATION.isNotDistinctFrom(ele))
														 .and(LOCATIONS.LATITUDE.isNotDistinctFrom(lat))
														 .and(LOCATIONS.LONGITUDE.isNotDistinctFrom(lng))
														 .fetchAny();

					   if (location == null)
					   {
						   location = context.newRecord(LOCATIONS);
						   location.setSiteName(name);
						   location.setSiteNameShort(shortName);
						   location.setCountryId(countryId);
						   location.setLocationtypeId(2);
						   location.setElevation(ele);
						   location.setLatitude(lat);
						   location.setLongitude(lng);
						   location.store();
					   }

					   locationNameToId.put(location.getSiteName(), location.getId());
				   });
			  }
			  catch (IOException e)
			  {
				  addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
			  }
		  });
	}

	private void getOrCreateDataset(DSLContext context, ReadableWorkbook wb)
	{
		wb.findSheet("METADATA")
		  .ifPresent(s -> {
			  try
			  {
				  DublinCore dublinCore = new DublinCore();
				  String name = null;
				  String description = null;
				  Integer datasetType = getDatasetTypeId();

				  List<Row> rows = s.read();
				  Integer index = metadataLabelToRowIndex.get("Title");
				  if (index != null)
				  {
					  name = getCellValue(rows.get(index), 2);
					  dublinCore.setTitle(new String[]{name});
				  }
				  index = metadataLabelToRowIndex.get("Description");
				  if (index != null)
				  {
					  description = getCellValue(rows.get(index), 2);
					  dublinCore.setTitle(new String[]{description});
				  }
				  index = metadataLabelToRowIndex.get("Rights");
				  if (index != null)
					  dublinCore.setRights(new String[]{getCellValue(rows.get(index), 2)});
				  index = metadataLabelToRowIndex.get("Date of creation");
				  if (index != null)
					  dublinCore.setDate(new String[]{getCellValue(rows.get(index), 2)});
				  index = metadataLabelToRowIndex.get("Publisher");
				  if (index != null)
					  dublinCore.setPublisher(new String[]{getCellValue(rows.get(index), 2)});
				  index = metadataLabelToRowIndex.get("Format");
				  if (index != null)
					  dublinCore.setFormat(new String[]{getCellValue(rows.get(index), 2)});
				  index = metadataLabelToRowIndex.get("Language");
				  if (index != null)
					  dublinCore.setLanguage(new String[]{getCellValue(rows.get(index), 2)});
				  index = metadataLabelToRowIndex.get("Source");
				  if (index != null)
					  dublinCore.setSource(new String[]{getCellValue(rows.get(index), 2)});
				  index = metadataLabelToRowIndex.get("Type");
				  if (index != null)
					  dublinCore.setType(new String[]{getCellValue(rows.get(index), 2)});
				  index = metadataLabelToRowIndex.get("Subject");
				  if (index != null)
					  dublinCore.setSubject(new String[]{getCellValue(rows.get(index), 2)});

				  // Check if the dataset exists (assuming name+description are unique
				  dataset = context.selectFrom(DATASETS)
								   .where(DATASETS.NAME.isNotDistinctFrom(name))
								   .and(DATASETS.DESCRIPTION.isNotDistinctFrom(description))
								   .and(DATASETS.DATASETTYPE_ID.eq(datasetType))
								   .fetchAny();

				  if (dataset == null)
				  {
					  // If it doesn't, check if the experiment exists
					  ExperimentsRecord experiment = context.selectFrom(EXPERIMENTS)
															.where(EXPERIMENTS.EXPERIMENT_NAME.isNotDistinctFrom(name))
															.and(EXPERIMENTS.DESCRIPTION.isNotDistinctFrom(description))
															.fetchAny();

					  if (experiment == null)
					  {
						  // If it doesn't, create it
						  experiment = context.newRecord(EXPERIMENTS);
						  experiment.setExperimentName(name);
						  experiment.setDescription(description);
						  experiment.store();
					  }

					  // Then create the dataset
					  dataset = context.newRecord(DATASETS);
					  dataset.setExperimentId(experiment.getId());
					  dataset.setDatasettypeId(getDatasetTypeId());
					  dataset.setName(name);
					  dataset.setDescription(description);

					  index = metadataLabelToRowIndex.get("Contact");
					  if (index != null)
						  dataset.setContact(getCellValue(rows.get(index), 2));

					  dataset.setDatasetStateId(1);
					  dataset.setDublinCore(dublinCore);
					  dataset.store();


					  for (Integer locationId : locationNameToId.values())
					  {
						  DatasetlocationsRecord dslr = context.selectFrom(DATASETLOCATIONS)
															   .where(DATASETLOCATIONS.LOCATION_ID.eq(locationId))
															   .and(DATASETLOCATIONS.DATASET_ID.eq(dataset.getId()))
															   .fetchAny();

						  if (dslr == null)
						  {
							  dslr = context.newRecord(DATASETLOCATIONS);
							  dslr.setDatasetId(dataset.getId());
							  dslr.setLocationId(locationId);
							  dslr.store();
						  }
					  }
				  }
			  }
			  catch (IOException e)
			  {
				  throw new RuntimeException(e);
			  }
		  });
	}

	protected boolean areEqual(Row one, Row two)
	{
		if (one.getCellCount() != two.getCellCount())
			return false;

		for (int i = 0; i < one.getCellCount(); i++)
		{
			if (!Objects.equals(getCellValue(one, i), getCellValue(two, i)))
				return false;
		}

		return true;
	}

	@Override
	protected void updateFile(ReadableWorkbook wb)
	{
		// We don't support updates, simply import
		importFile(wb);
	}

	protected abstract int getDatasetTypeId();
}
