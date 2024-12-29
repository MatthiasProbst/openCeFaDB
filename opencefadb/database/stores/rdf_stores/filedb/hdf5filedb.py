import pathlib
from abc import ABC, abstractmethod
from typing import List

from gldb import RawDataStore
from gldb.query import Query
from h5rdmtoolbox.database.hdfdb import FilesDB


class HDF5FileQuery(Query, ABC):

    def __init__(self, filter, objfilter=None, recursive=True, **kwargs):
        self._filter = filter
        self._objfilter = objfilter
        self._recursive = recursive
        self._kwargs = kwargs

    @abstractmethod
    def execute(self, filenames: List[pathlib.Path], *args, **kwargs):
        pass


class FindOneLazyHDFObject(HDF5FileQuery):
    def execute(self, filenames: List[pathlib.Path], *args, **kwargs):
        return FilesDB(filenames).find_one(flt=self._filter,
                                           objfilter=self._objfilter,
                                           recursive=self._recursive,
                                           **kwargs)


class FindManyLazyHDFObject(HDF5FileQuery):
    def execute(self, filenames: List[pathlib.Path], *args, **kwargs):
        return FilesDB(filenames).find(flt=self._filter,
                                       objfilter=self._objfilter,
                                       recursive=self._recursive, **kwargs)


class HDF5FileDB(RawDataStore):

    def __init__(self):
        self._filenames = []
        self._expected_file_extensions = {".hdf5", ".h5"}

    @property
    def filenames(self):
        return self._filenames

    def upload_file(self, filename) -> bool:
        filename = pathlib.Path(filename)
        assert filename.exists(), f"File {filename} does not exist."
        assert filename.suffix in self._expected_file_extensions, f"File type {filename.suffix} not supported"
        if filename.resolve().absolute() in self._filenames:
            return True
        self._filenames.append(filename.resolve().absolute())
        return True

    def execute_query(self, query: HDF5FileQuery):
        return query.execute(self.filenames)
