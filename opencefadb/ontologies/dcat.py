from typing import Union

from ontolutils import namespaces, urirefs
from pydantic import HttpUrl, Field, FileUrl
from ssnolib.dcat.resource import Distribution as SSNOlibDistribution
from ssnolib.dcat.resource import Resource, Dataset


@namespaces(dcat="http://www.w3.org/ns/dcat#")
@urirefs(DataService='dcat:DataService',
         endpointURL='dcat:endpointURL',
         servesDataset='dcat:servesDataset')
class DataService(Resource):
    endpointURL: Union[HttpUrl, FileUrl] = Field(alias='endpoint_url', default=None)  # dcat:endpointURL
    servesDataset: Dataset = Field(alias='serves_dataset', default=None)  # dcat:servesDataset


@namespaces(dcat="http://www.w3.org/ns/dcat#")
@urirefs(Distribution='dcat:Distribution',
         accessService='dcat:distribution')
class Distribution(SSNOlibDistribution):
    accessService: DataService = Field(default=None, alias='access_service')  # dcat:accessService
