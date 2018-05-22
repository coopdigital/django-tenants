import re
import warnings
from django.conf import settings
from importlib import import_module

from django.core.exceptions import ImproperlyConfigured, ValidationError
from django_tenants.utils import get_public_role_name, get_limit_set_calls
from django_tenants.postgresql_backend.introspection import DatabaseSchemaIntrospection
import django.db.utils
import psycopg2


DatabaseError = django.db.utils.DatabaseError
IntegrityError = psycopg2.IntegrityError

ORIGINAL_BACKEND = getattr(settings, 'ORIGINAL_BACKEND', 'django.db.backends.postgresql_psycopg2')

original_backend = import_module(ORIGINAL_BACKEND + '.base')

EXTRA_SEARCH_PATHS = getattr(settings, 'PG_EXTRA_SEARCH_PATHS', [])

# from the postgresql doc
SQL_IDENTIFIER_RE = re.compile(r'^[_a-zA-Z][_a-zA-Z0-9]{,62}$')
SQL_SCHEMA_NAME_RESERVED_RE = re.compile(r'^pg_', re.IGNORECASE)


def _is_valid_identifier(identifier):
    return bool(SQL_IDENTIFIER_RE.match(identifier))


def _check_identifier(identifier):
    if not _is_valid_identifier(identifier):
        raise ValidationError("Invalid string used for the identifier.")


def _is_valid_role_name(name):
    # TODO: Implement restrictions/validation on role name (e.g. no spaces or special characters) 
    return True 

def _check_role_name(name):
    if not _is_valid_role_name(name):
        raise ValidationError("Invalid string used for the schema name.")


class DatabaseWrapper(original_backend.DatabaseWrapper):
    """
    Adds the capability to manipulate the search_path using set_tenant and set_schema_name
    """
    include_public_schema = True
    _previous_cursor = None

    def __init__(self, *args, **kwargs):
        self.search_path_set = None
        self.tenant = None
        self.role_name = None
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        # Use a patched version of the DatabaseIntrospection that only returns the table list for the
        # currently selected schema.
        self.introspection = DatabaseSchemaIntrospection(self)
        self.set_role_to_public()

    def close(self):
        self.search_path_set = False
        super(DatabaseWrapper, self).close()

    def set_tenant(self, tenant, include_public=True):
        """
        Main API method to current database role,
        but it does not actually modify the db connection.
        """
        self.tenant = tenant
        self.role_name = tenant.role_name
        self.include_public_role = include_public
        self.set_settings_role(self.role_name)
        self.search_path_set = False

    def set_role(self, role_name, include_public=True):
        """
        Main API method to current database role,
        but it does not actually modify the db connection.
        """
        self.tenant = FakeTenant(role_name=role_name)
        self.role_name = role_name
        self.include_public_role = include_public
        self.set_settings_role(role_name)
        self.search_path_set = False

    def set_role_to_public(self):
        """
        Instructs to stay in the common 'public' role.
        """
        self.tenant = FakeTenant(role_name=get_public_role_name())
        self.role_name = get_public_role_name()
        self.set_settings_role(self.role_name)
        self.search_path_set = False

    def set_settings_role(self, role_name):
        self.settings_dict['ROLE'] = role_name

    def get_role(self):
        warnings.warn("connection.get_role() is deprecated, use connection.role_name instead.",
                      category=DeprecationWarning)
        return self.role_name

    def get_tenant(self):
        warnings.warn("connection.get_tenant() is deprecated, use connection.tenant instead.",
                      category=DeprecationWarning)
        return self.tenant

    def _cursor(self, name=None):
        """
        Here it happens. We hope every Django db operation using PostgreSQL
        must go through this to get the cursor handle. We change the path.
        """
        if name:
            # Only supported and required by Django 1.11 (server-side cursor)
            cursor = super(DatabaseWrapper, self)._cursor(name=name)
        else:
            cursor = super(DatabaseWrapper, self)._cursor()

        # optionally limit the number of executions - under load, the execution
        # of `set search_path` can be quite time consuming
        if (not get_limit_set_calls()) or not self.search_path_set or self._previous_cursor != cursor:
            # Store the cursor pointer to check if it has changed since we
            # last validated.
            self._previous_cursor = cursor
            # Actual search_path modification for the cursor. Database will
            # search schemata from left to right when looking for the object
            # (table, index, sequence, etc.).
            if not self.role_name:
                raise ImproperlyConfigured("Database schema not set. Did you forget "
                                           "to call set_role() or set_tenant()?")
            _check_role_name(self.role_name)
            public_role_name = get_public_role_name()
            search_paths = []

            if self.role_name == public_role_name:
                search_paths = [public_role_name]
            elif self.include_public_role:
                search_paths = [self.role_name, public_role_name]
            else:
                search_paths = [self.role_name]

            search_paths.extend(EXTRA_SEARCH_PATHS)

            if name:
                # Named cursor can only be used once
                cursor_for_search_path = self.connection.cursor()
            else:
                # Reuse
                cursor_for_search_path = cursor

            # In the event that an error already happened in this transaction and we are going
            # to rollback we should just ignore database error when setting the search_path
            # if the next instruction is not a rollback it will just fail also, so
            # we do not have to worry that it's not the good one
            try:
                cursor_for_search_path.execute('SET search_path = {0}'.format(','.join(search_paths)))
            except (django.db.utils.DatabaseError, psycopg2.InternalError):
                self.search_path_set = False
            else:
                self.search_path_set = True
            if name:
                cursor_for_search_path.close()
        return cursor


class FakeTenant:
    """
    We can't import any db model in a backend (apparently?), so this class is used
    for wrapping role names in a tenant-like structure.
    """
    def __init__(self, role_name):
        self.role_name = role_name
