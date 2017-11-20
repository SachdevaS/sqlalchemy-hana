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
    @testing.provide_metadata
    def _test_get_check_constraints(self, schema=None):
        orig_meta = self.metadata
        Table(
            'sa_cc', orig_meta,
            Column('a', Integer()),
            sa.CheckConstraint('a > 1 AND a < 5', name='cc1'),
            sa.CheckConstraint('a = 1 OR (a > 2 AND a < 5)', name='cc2'),
            schema=schema
        )

        orig_meta.create_all()

        inspector = inspect(orig_meta.bind)
        reflected = sorted(
            inspector.get_check_constraints('sa_cc', schema=schema),
            key=operator.itemgetter('name')
        )

        reflected = [
            {"name": item["name"],
             # trying to minimize effect of quoting, parenthesis, etc.
             # may need to add more to this as new dialects get CHECK
             # constraint reflection support
             "sqltext": re.sub(r"[`'\(\)]", '', item["sqltext"].lower())}
            for item in reflected
        ]
        eq_(
            reflected,
            [
                {'name': 'cc1', 'sqltext': '  "a" > 1 and "a" < 5  '},
                {'name': 'cc2', 'sqltext': '  "a" = 1 or   "a" > 2 and "a" < 5    '}
            ]
        )
