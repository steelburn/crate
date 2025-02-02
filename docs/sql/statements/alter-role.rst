.. highlight:: psql
.. _ref-alter-role:

==============
``ALTER ROLE``
==============

Alter an existing database user or role.

.. rubric:: Table of contents

.. contents::
   :local:

Synopsis
========

::

    ALTER ROLE name
      SET ( parameter = value [, ...] )


Description
===========

``ALTER ROLE`` applies a change to an existing database user or role. Only
existing superusers or the user itself have the privilege to alter an existing
database user.


Arguments
=========

``name``
--------

The name by which the user or role is identified inside the database.

``SET``
-------

Changes a user parameter to a different value. The following ``parameter``
are supported to alter an existing user account:

:password:
  The password as cleartext entered as string literal.

  ``NULL`` removes the password from the user.

.. CAUTION::

    Passwords cannot be set for the ``crate`` superuser.

    For security reasons it is recommended to authenticate as ``crate`` using a
    client certificate.

.. NOTE::

   Passwords can be set only to existing database users, but not to roles.
