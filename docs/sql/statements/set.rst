.. highlight:: psql
.. _ref-set:

=====================
``SET`` and ``RESET``
=====================

Change and restore settings at runtime. To get an overview of available CrateDB
settings, see :ref:`config`. Only settings documented with *Runtime:* ``yes``
can be changed.

Synopsis
========

::

    SET [ SESSION | LOCAL ] setting_ident { = | TO } { setting_value | 'setting_value' | DEFAULT }

    SET GLOBAL [ PERSISTENT | TRANSIENT ] { setting_ident [ = | TO ] { value | ident } } [, ...]

    RESET GLOBAL setting_ident [, ...]

.. _ref-set-desc:

Description
===========

``SET GLOBAL`` can be used to change a global cluster setting, see
:ref:`conf-cluster-settings`, to a different value. Using ``RESET`` will reset
the cluster setting to its default value or to the setting value defined in the
configuration file, if it was set on a node start-up. The global cluster
settings can be applied to a cluster using ``PERSISTENT`` and ``TRANSIENT``
keywords to set a persistent level.

``SET/SET SESSION`` may affect the current session if the setting is supported.
Setting the unsupported settings will be ignored and logged with the ``INFO``
logging level. See :ref:`search_path <conf-session-search-path>`, to get an
overview of the supported session setting parameters.

``SET LOCAL`` does not have any effect on CrateDB configurations. All ``SET
LOCAL`` statements will be ignored by CrateDB and logged with the ``INFO``
logging level.

``SET SESSION/LOCAL`` are introduced to be compliant with third-party
applications which use the PostgresSQL wire protocol.

Parameters
==========

:setting_ident:
  The full qualified setting ident of the setting to set / reset.

:value:
  The value to set for the setting.

:ident:
  The ident to set for the setting.

:setting_value:
  The new value for the setting. It can be specified as string
  constants, identifiers, numbers, or comma-separated list of these, as
  appropriate for the particular setting.

:DEFAULT:
  Used for resetting the parameter to its default value.

Persistence
===========

The default is ``TRANSIENT``. Settings that are set using the ``TRANSIENT``
keyword will be discarded if the cluster is stopped or restarted.

Using the ``PERSISTENT`` keyword will persist a value of the setting to a disk,
so that the setting will not be discarded if the cluster restarts.

.. NOTE::

   The persistence keyword can only be used within a ``SET`` statement.
