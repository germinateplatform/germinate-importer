package jhi.germinate.server.util.importer;

import jhi.germinate.server.database.tables.Storage;

import static jhi.germinate.server.database.tables.Attributedata.*;
import static jhi.germinate.server.database.tables.Countries.*;
import static jhi.germinate.server.database.tables.Germinatebase.*;
import static jhi.germinate.server.database.tables.Institutions.*;
import static jhi.germinate.server.database.tables.Locations.*;
import static jhi.germinate.server.database.tables.Pedigreedefinitions.*;
import static jhi.germinate.server.database.tables.Taxonomies.*;

/**
 * @author Sebastian Raubach
 */
public enum McpdField
{
	PUID(GERMINATEBASE.PUID.getName()),
	INSTCODE(INSTITUTIONS.CODE.getName()),
	ACCENUMB(GERMINATEBASE.NAME.getName(), GERMINATEBASE.GENERAL_IDENTIFIER.getName()),
	COLLNUMB(GERMINATEBASE.COLLNUMB.getName()),
	COLLCODE(GERMINATEBASE.COLLCODE.getName()),
	COLLNAME(GERMINATEBASE.COLLNAME.getName()),
	COLLINSTADDRESS(INSTITUTIONS.ADDRESS.getName()),
	COLLMISSID(GERMINATEBASE.COLLMISSID.getName()),
	GENUS(TAXONOMIES.GENUS.getName()),
	SPECIES(TAXONOMIES.SPECIES.getName()),
	SPAUTHOR(TAXONOMIES.SPECIES_AUTHOR.getName()),
	SUBTAXA(TAXONOMIES.SUBTAXA.getName()),
	SUBTAUTHOR(TAXONOMIES.SUBTAXA_AUTHOR.getName()),
	CROPNAME(TAXONOMIES.CROPNAME.getName()),
	ACCENAME(GERMINATEBASE.NUMBER.getName()),
	ACQDATE(GERMINATEBASE.ACQDATE.getName()),
	ORIGCTY(COUNTRIES.COUNTRY_CODE3.getName()),
	COLLSITE(LOCATIONS.SITE_NAME.getName()),
	DECLATITUDE(LOCATIONS.LATITUDE.getName()),
	LATITUDE(LOCATIONS.LATITUDE.getName()),
	DECLONGITUDE(LOCATIONS.LONGITUDE.getName()),
	LONGITUDE(LOCATIONS.LONGITUDE.getName()),
	COORDUNCERT(LOCATIONS.COORDINATE_UNCERTAINTY.getName()),
	COORDDATUM(LOCATIONS.COORDINATE_DATUM.getName()),
	GEOREFMETH(LOCATIONS.GEOREFERENCING_METHOD.getName()),
	ELEVATION(LOCATIONS.ELEVATION.getName()),
	COLLDATE(GERMINATEBASE.COLLDATE.getName()),
	BREDCODE(GERMINATEBASE.BREEDERS_CODE.getName()),
	BREDNAME(GERMINATEBASE.BREEDERS_NAME.getName()),
	SAMPSTAT(GERMINATEBASE.BIOLOGICALSTATUS_ID.getName()),
	ANCEST(PEDIGREEDEFINITIONS.DEFINITION.getName()),
	COLLSRC(GERMINATEBASE.COLLSRC_ID.getName()),
	DONORCODE(GERMINATEBASE.DONOR_CODE.getName()),
	DONORNAME(GERMINATEBASE.DONOR_NAME.getName()),
	DONORNUMB(GERMINATEBASE.DONOR_NUMBER.getName()),
	OTHERNUMB(GERMINATEBASE.OTHERNUMB.getName()),
	DUPLSITE(GERMINATEBASE.DUPLSITE.getName()),
	DUPLINSTNAME(GERMINATEBASE.DUPLINSTNAME.getName()),
	STORAGE(Storage.STORAGE.ID.getName()),
	MLSSTAT(GERMINATEBASE.MLSSTATUS_ID.getName()),
	REMARKS(ATTRIBUTEDATA.VALUE.getName());

	private String[] fields;

	McpdField(String... fields)
	{
		this.fields = fields;
	}

	public String[] getFields()
	{
		return fields;
	}
}
