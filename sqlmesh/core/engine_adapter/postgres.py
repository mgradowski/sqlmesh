from __future__ import annotations

import logging
import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
)

logger = logging.getLogger(__name__)


class PostgresEngineAdapter(
    BasePostgresEngineAdapter,
    LogicalReplaceQueryMixin,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    DIALECT = "postgres"
    SUPPORTS_INDEXES = True

    @t.overload
    def _fetch_native_df(
        self,
        query: t.Union[exp.Expression, str],
        quote_identifiers: bool = ...,
        chunksize: None = ...,
    ) -> pd.DataFrame:
        ...

    @t.overload
    def _fetch_native_df(
        self,
        query: t.Union[exp.Expression, str],
        quote_identifiers: bool = ...,
        *,
        chunksize: int,
    ) -> t.Iterator[pd.DataFrame]:
        ...

    def _fetch_native_df(
        self,
        query: t.Union[exp.Expression, str],
        quote_identifiers: bool = False,
        chunksize: t.Optional[int] = None,
    ) -> t.Union[pd.DataFrame, t.Iterator[pd.DataFrame]]:
        """
        `read_sql_query` when using psycopg will result on a hanging transaction that must be committed

        https://github.com/pandas-dev/pandas/pull/42277
        """
        df = super()._fetch_native_df(query, quote_identifiers, chunksize=chunksize)
        if not self._connection_pool.is_transaction_active:
            self._connection_pool.commit()
        return df
