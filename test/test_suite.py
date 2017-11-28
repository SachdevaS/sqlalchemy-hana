# Copyright 2015 SAP SE.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

# test/test_suite.py

from sqlalchemy import testing
from sqlalchemy.testing import engines
from sqlalchemy.testing.suite import *
from sqlalchemy.testing.exclusions import skip_if
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy import event
from sqlalchemy.schema import DDL
import sqlalchemy as sa
from sqlalchemy import inspect
import re
import operator


class HANAConnectionIsDisconnectedTest(fixtures.TestBase):

    @testing.only_on('hana')
    @testing.skip_if('hana+pyhdb')
    def test_detection_by_error_code(self):
        from hdbcli.dbapi import Error

        dialect = testing.db.dialect
        assert dialect.is_disconnect(Error(-10709, 'Connect failed'), None, None)

    @testing.only_on('hana')
    @testing.skip_if('hana+pyhdb')
    def test_detection_by_isconnected_function(self):
        dialect = testing.db.dialect

        mock_connection = Mock(
            isconnected=Mock(return_value=False)
        )
        assert dialect.is_disconnect(None, mock_connection, None)

        mock_connection = Mock(
            isconnected=Mock(return_value=True)
        )
        assert not dialect.is_disconnect(None, mock_connection, None)


class ComponentReflectionTest(_ComponentReflectionTest):
    # @testing.provide_metadata
    # def _test_get_check_constraints(self, schema=None):
    #     orig_meta = self.metadata
    #     Table(
    #         'sa_cc', orig_meta,
    #         Column('a', Integer()),
    #         sa.CheckConstraint('a > 1 AND a < 5', name='cc1'),
    #         sa.CheckConstraint('a = 1 OR (a > 2 AND a < 5)', name='cc2'),
    #         schema=schema
    #     )
    #
    #     orig_meta.create_all()
    #
    #     inspector = inspect(orig_meta.bind)
    #     reflected = sorted(
    #         inspector.get_check_constraints('sa_cc', schema=schema),
    #         key=operator.itemgetter('name')
    #     )
    #
    #     reflected = [
    #         {"name": item["name"],
    #          # trying to minimize effect of quoting, parenthesis, etc.
    #          # may need to add more to this as new dialects get CHECK
    #          # constraint reflection support
    #          "sqltext": re.sub(r"[`'\(\)]", '', item["sqltext"].lower())}
    #         for item in reflected
    #     ]
    #     eq_(
    #         reflected,
    #         [
    #             {'name': 'cc1', 'sqltext': '  "a" > 1 and "a" < 5  '},
    #             {'name': 'cc2', 'sqltext': '  "a" = 1 or   "a" > 2 and "a" < 5    '}
    #         ]
    #     )
    #
    @classmethod
    def define_temp_tables(cls, metadata):

        if testing.against("hana"):
            kw = {
                'prefixes': ["GLOBAL TEMPORARY"],
            }
        else:
            kw = {
                'prefixes': ["TEMPORARY"],
            }

        user_tmp = Table(
            "user_tmp", metadata,
            Column("id", sa.INT, primary_key=True),
            Column('name', sa.VARCHAR(50)),
            Column('foo', sa.INT),
            sa.UniqueConstraint('name', name='user_tmp_uq'),
            sa.Index("user_tmp_ix", "foo"),
            **kw
        )
        if testing.requires.view_reflection.enabled and \
                testing.requires.temporary_views.enabled:
            event.listen(
                user_tmp, "after_create",
                DDL("create temporary view user_tmp_v as "
                    "select * from user_tmp")
            )
            event.listen(
                user_tmp, "before_drop",
                DDL("drop view user_tmp_v")
            )

    @testing.provide_metadata
    def _test_get_table_oid(self, table_name, schema=None):
        meta = self.metadata
        users, addresses, dingalings = self.tables.users, \
            self.tables.email_addresses, self.tables.dingalings
        insp = inspect(meta.bind)
        oid = insp.get_table_oid(table_name, schema)
        self.assert_(isinstance(oid, int))