package jhi.germinate.server.util.importer;

import org.dhatim.fastexcel.reader.*;
import org.jooq.DSLContext;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

import jhi.germinate.resource.DublinCore;
import jhi.germinate.resource.enums.ImportStatus;
import jhi.germinate.server.Database;
import jhi.germinate.server.database.tables.records.*;
import jhi.germinate.server.util.StringUtils;

import static jhi.germinate.server.database.tables.Collaborators.*;
import static jhi.germinate.server.database.tables.Countries.*;
import static jhi.germinate.server.database.tables.Datasetcollaborators.*;
import static jhi.germinate.server.database.tables.Datasetlocations.*;
import static jhi.germinate.server.database.tables.Datasets.*;
import static jhi.germinate.server.database.tables.Experiments.*;
import static jhi.germinate.server.database.tables.Institutions.*;
import static jhi.germinate.server.database.tables.Locations.*;

/**
 * @author Sebastian Raubach
 */
public abstract class DatasheetImporter extends AbstractImporter
{
	private static final String[] METADATA_LABELS = {"Title", "Description", "Rights", "Date of creation", "Publisher", "Format", "Language", "Source", "Type", "Subject", "Contact"};

	protected DatasetsRecord       dataset;
	private   Set<LocationsRecord> locations = new LinkedHashSet<>();
	private   Map<String, Integer> countryCode2ToId;
	private   Map<String, Integer> metadataLabelToRowIndex;

	public DatasheetImporter(File input, boolean isUpdate, boolean deleteOnFail)
	{
		super(input, isUpdate, deleteOnFail);
	}

	@Override
	protected void prepare()
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			countryCode2ToId = context.selectFrom(COUNTRIES)
									  .fetchMap(COUNTRIES.COUNTRY_CODE2, COUNTRIES.ID);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	protected void checkFile(ReadableWorkbook wb)
	{
		wb.findSheet("METADATA")
		  .ifPresent(this::checkMetadataSheet);

		wb.findSheet("LOCATION")
		  .ifPresent(this::checkLocationSheet);

		wb.findSheet("COLLABORATORS")
		  .ifPresent(this::checkCollaboratorsSheet);
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

				 if (!Objects.equals(getCellValue(r.getCell(0)), "Last Name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Last Name");
				 if (!Objects.equals(getCellValue(r.getCell(1)), "First Name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "First Name");
				 if (!Objects.equals(getCellValue(r.getCell(2)), "Email"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Email");
				 if (!Objects.equals(getCellValue(r.getCell(3)), "Phone"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Phone");
				 if (!Objects.equals(getCellValue(r.getCell(4)), "Contributor"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Contributor");
				 if (!Objects.equals(getCellValue(r.getCell(5)), "Address"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Address");
				 if (!Objects.equals(getCellValue(r.getCell(6)), "Country"))
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

	private void checkLocationSheet(Sheet s)
	{
		try
		{
			s.openStream()
			 .findFirst()
			 .ifPresent(r -> {
				 if (!Objects.equals(getCellValue(r.getCell(0)), "Name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Name");
				 if (!Objects.equals(getCellValue(r.getCell(1)), "Short name"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Short name");
				 if (!Objects.equals(getCellValue(r.getCell(2)), "Country"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Country");
				 if (!Objects.equals(getCellValue(r.getCell(3)), "Elevation"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Elevation");
				 if (!Objects.equals(getCellValue(r.getCell(4)), "Latitude"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Latitude");
				 if (!Objects.equals(getCellValue(r.getCell(5)), "Longitude"))
					 addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "Longitude");
			 });
			s.openStream()
			 .skip(1)
			 .forEachOrdered(r -> {
				 if (allCellsEmpty(r))
					 return;

				 String name = getCellValue(r.getCell(0));
				 String shortName = getCellValue(r.getCell(1));
				 String country = getCellValue(r.getCell(2));
				 String elevation = getCellValue(r.getCell(3));
				 String latitude = getCellValue(r.getCell(3));
				 String longitude = getCellValue(r.getCell(3));

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
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	private void checkMetadataHeaders(Row r)
	{
		if (!Objects.equals(getCellValue(r.getCell(0)), "LABEL"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "LABEL");
		if (!Objects.equals(getCellValue(r.getCell(1)), "DEFINITION"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "DEFINITION");
		if (!Objects.equals(getCellValue(r.getCell(2)), "VALUE"))
			addImportResult(ImportStatus.GENERIC_MISSING_COLUMN, 0, "VALUE");
	}

	private void checkMetadataLabels(Sheet s)
	{
		try
		{
			List<Row> rows = s.read();
			metadataLabelToRowIndex = new HashMap<>();

			for (int i = 1; i < rows.size(); i++)
			{
				Row r = rows.get(i);

				if (allCellsEmpty(r))
					break;

				String label = getCellValue(r.getCell(0));
				metadataLabelToRowIndex.put(label, i);
			}

			Arrays.stream(METADATA_LABELS)
				  .forEachOrdered(c -> {
					  if (!metadataLabelToRowIndex.containsKey(c))
						  addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, -1, c);
				  });

			Integer index = metadataLabelToRowIndex.get("Title");
			if (index != null)
			{
				String name = getCellValue(rows.get(index).getCell(0));
				if (StringUtils.isEmpty(name))
					addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, index, "Title");
			}

			index = metadataLabelToRowIndex.get("Description");
			if (index != null)
			{
				String description = getCellValue(rows.get(index).getCell(0));
				if (StringUtils.isEmpty(description))
					addImportResult(ImportStatus.GENERIC_MISSING_REQUIRED_VALUE, index, "Description");
			}
		}
		catch (IOException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
	}

	@Override
	protected void importFile(ReadableWorkbook wb)
	{
		try (Connection conn = Database.getConnection();
			 DSLContext context = Database.getContext(conn))
		{
			getOrCreateLocations(context, wb);
			getOrCreateDataset(context, wb);
			getOrCreateCollaborators(context, wb);
		}
		catch (SQLException e)
		{
			addImportResult(ImportStatus.GENERIC_IO_ERROR, -1, e.getMessage());
		}
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

					   locations.add(location);
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

				  List<Row> rows = s.read();
				  Integer index = metadataLabelToRowIndex.get("Title");
				  if (index != null)
				  {
					  name = getCellValue(rows.get(index).getCell(2));
					  dublinCore.setTitle(new String[]{name});
				  }
				  index = metadataLabelToRowIndex.get("Description");
				  if (index != null)
				  {
					  description = getCellValue(rows.get(index).getCell(2));
					  dublinCore.setTitle(new String[]{description});
				  }
				  index = metadataLabelToRowIndex.get("Rights");
				  if (index != null)
					  dublinCore.setRights(new String[]{getCellValue(rows.get(index).getCell(2))});
				  index = metadataLabelToRowIndex.get("Date of creation");
				  if (index != null)
					  dublinCore.setDate(new String[]{getCellValue(rows.get(index).getCell(2))});
				  index = metadataLabelToRowIndex.get("Publisher");
				  if (index != null)
					  dublinCore.setPublisher(new String[]{getCellValue(rows.get(index).getCell(2))});
				  index = metadataLabelToRowIndex.get("Format");
				  if (index != null)
					  dublinCore.setFormat(new String[]{getCellValue(rows.get(index).getCell(2))});
				  index = metadataLabelToRowIndex.get("Language");
				  if (index != null)
					  dublinCore.setLanguage(new String[]{getCellValue(rows.get(index).getCell(2))});
				  index = metadataLabelToRowIndex.get("Source");
				  if (index != null)
					  dublinCore.setSource(new String[]{getCellValue(rows.get(index).getCell(2))});
				  index = metadataLabelToRowIndex.get("Type");
				  if (index != null)
					  dublinCore.setType(new String[]{getCellValue(rows.get(index).getCell(2))});
				  index = metadataLabelToRowIndex.get("Subject");
				  if (index != null)
					  dublinCore.setSubject(new String[]{getCellValue(rows.get(index).getCell(2))});

				  // Check if the dataset exists (assuming name+description are unique
				  dataset = context.selectFrom(DATASETS)
								   .where(DATASETS.NAME.isNotDistinctFrom(name))
								   .and(DATASETS.DESCRIPTION.isNotDistinctFrom(description))
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
						  dataset.setContact(getCellValue(rows.get(index).getCell(2)));

					  dataset.setDatasetStateId(1);
					  dataset.setDublinCore(dublinCore);
					  dataset.store();


					  for (LocationsRecord location : locations)
					  {
						  DatasetlocationsRecord dslr = context.selectFrom(DATASETLOCATIONS)
															   .where(DATASETLOCATIONS.LOCATION_ID.eq(location.getId()))
															   .and(DATASETLOCATIONS.DATASET_ID.eq(dataset.getId()))
															   .fetchAny();

						  if (dslr == null)
						  {
							  dslr = context.newRecord(DATASETLOCATIONS);
							  dslr.setDatasetId(dataset.getId());
							  dslr.setLocationId(location.getId());
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
		if (one.getPhysicalCellCount() != two.getPhysicalCellCount())
			return false;

		for (int i = 0; i < one.getPhysicalCellCount(); i++)
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
