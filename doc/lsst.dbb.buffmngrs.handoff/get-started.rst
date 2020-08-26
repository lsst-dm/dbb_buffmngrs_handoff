Quickstart
----------

Overview
^^^^^^^^

Data Backbone (DBB) buffer managers consist of two separate managers:
**handoff** and **endpoint**.  The handoff manager is  responsible for
transferring files from a **handoff site** to an **endpoint site**.  The
endpoint buffer manager is responsible for ingesting the file into the Data
Backbone.

Data Backbone (DBB) handoff buffer manager transfers files from a selected
directory, **a buffer**, on the **handoff site** (machine it runs on) to
another directory, also called a buffer, on the **endpoint site** (a possibly
remote location).  After successful transfer, DBB buffer manager moves the
files from the buffer to another directory located at the handoff site, so
called **holding area**.

Prerequisites
^^^^^^^^^^^^^

DBB handoff buffer manager does not have any internal mechanism for
transferring files to a remote location.  It will use whatever tool you
specify as a transfer command in its configuration file.  It is your
responsibility to make sure that the tool you specified, such as ``scp``,
``rsync``, or ``bbcp``, is installed on your system.

Aforementioned tools use SSH protocol to initiate connection to the remote
host which requires some form of authentication.  The DBB buffer manager
won't be able to operate unattended if a passwordless login is *not* enabled
for the user account on *the endpoint site*.

To enable passwordless login, you need to have a SSH key pair and copy the
public SSH key of the user who will be running the DBB handoff buffer manager
to ``~/.ssh/authorized_keys`` file of the user on *the endpoint* site.

The SSH key must allow for unsupervised logins. So either you need a one with
an empty passphrase or you need set up ``ssh-agent`` to manage your key.

You may find more information how to generate and manage SSH keys for example
`here`__ or `here`__.

.. __: https://wiki.archlinux.org/index.php/SSH_keys
.. __: https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent

Finally, to run the manager, you'll need a minimal LSST stack installation,
i.e, with `Miniconda`_, `EUPS`_ and ``base`` meta package installed.

.. _Miniconda: https://docs.conda.io/en/latest/miniconda.html
.. _EUPS: https://github.com/RobertLuptonTheGood/eups

Assuming that the variable ``LSSTSW`` points to your installation, load
the LSST software environment into your shell:

.. code-block:: bash

   source ${LSSTSW}/loadLSST.bash

Download and install DBB handoff buffer manager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a directory where you want to install DBB handoff buffer manager into.
For example:

.. code-block:: bash

   mkdir -p lsstsw/addons
   cd lsstsw/addons

Clone the repository from GitHub:

.. code-block:: bash

   git clone https://github.com/lsst-dm/dbb_buffmngrs_handoff .

Set it up and build:

.. code-block:: bash

   cd dbb_buffmngrs_handoff
   setup -r .
   scons

If you would like to select a specific version of the manager, run ``git
checkout <ver>`` *before* ``setup -r .`` where ``<ver>`` is either an existing
git tag or a branch.

Test DBB handoff buffer manager installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After you’ve installed DBB handoff buffer manager, you can run ``hdfmgr
--help`` to check if the installation was successful and see its CLI.

Configure DBB handoff buffer manger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before you can run DBB handoff buffer manager you must create a YAML
configuration file which specifies, at minimum, both handoff and endpoint
sites.  For example:

.. code-block:: yaml

   handoff:
     buffer: /data/buffer
     holding: /data/holding
   endpoint:
     buffer: /data/buffer
     staging: /data/staging
     commands:
       transfer: "scp -Bpq {file} {user}@{host}:{dest}"
       remote: "ssh {user}@{host} \"{command}\""
     user: jdoe
     host: example.edu

While configuration of the handoff site is essentially self-explanatory, the
specification of the endpoint site requires some clarification.

In the ``endpoint`` section, at minimum, you need to define:

* ``buffer``: endpoint's buffer, 
* ``staging``: staging area for files being transferred (see note below)
* ``commands``: commands describing how the manager should transfer files and
  execute Unix shell commands remotely.

.. note::

   The endpoint manager requires that writing files to the endpoint's buffer is
   an atomic operation.  Hence the handoff manager doesn't transfer files
   directly to the buffer on the endpoint site.  Initially, it starts writing
   each file to separate directory, **staging area**, mentioned above. Only
   after the transfer for a file is completed, the manager moves it to the
   buffer.

In the example provided above, the handoff manager is instructed to transfer
the files to the endpoint site's buffer located at ``/data/buffer`` using
``/data/staging`` as the staging area.  It will use ``scp`` command to transfer
files and OpenSSH client, ``ssh``, to execute any shell commands remotely, if
necessary.  As some settings (user and host name) are used in ``scp`` as well
as in ``ssh`` command, they were defined as parameters to make changing them
easier if it is ever needed.

In general, while defining commands for file transfer and remote command
execution, please keep in mind that:

#. You may define arbitrary parameters in the ``endpoint`` section, e.g.,
   ``port: 22``.  However, do **not** use ``batch``, ``file``, ``dest``, and
   ``command`` as a parameter name.  These are reserved keywords with special
   meaning.

#. You can use parameters you set while defining the commands described above,
   just enclose their name in curly braces, e.g., ``{port}``.  They will be
   substituted with provided value during the runtime.

#. The transfer command **must** contain ``{file}`` *and* ``{dest}`` keywords.
   During the runtime, the handoff manager will substitute these keywords with
   the name of the file being transfer and appropriate target location on the
   endpoint site.
   
#. The ``{file}`` can be replaced with ``{batch}``.  It will instruct the
   handoff manager to transfer files in batches when possible instead of
   executing the transfer command separately for each file.

#. The command describing how shell commands need to be executed on the
   endpoint site **must** contain ``{command}`` keyword which tells the handoff
   manager where the shell commands it needs to execute on the endpoint site
   must be placed.

#. The handoff manager may need to execute various shell commands on the
   endpoint site (e.g. ``mkdir`` of ``find``).  Make sure that your
   specification of the remote command does **not** put unnecessary
   restrictions on what shell command can be executed.

.. note::

   To see other supported configuration options, look at example
   configuration in ``etc/config.yaml`` in the DBB handoff buffer manager
   repository.

Configure DBB handoff manager's database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The manager uses `SQLite`_ database to store various information about files
it transferred as well as transfer attempts it made. You need to provide
location of this database in the configuration file. For example,

.. code-block:: yaml

   database:
     engine: "sqlite:///handoff.db"

will instruct the manager to look for the data base in ``handoff.db`` file
located in the current directory.

.. note::

   Specifying a valid connect string for SQLite database requires some care.
   Follow `this`__ link for more details.

.. __: https://docs.sqlalchemy.org/en/13/dialects/sqlite.html?highlight=sqlite#connect-strings

If the database doesn't exist yet, you can easily create it with

.. code-block:: bash

   hdfmgr initdb config.yaml

.. _SQLite: https://sqlite.org/index.html

Run DBB handoff buffer manager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Having the handoff and endpoint sites defined in the ``config.yaml``, you can
start DBB handoff buffer manager with:

.. code-block:: bash

   hdfmgr run config.yaml

.. note::

   By default, all warnings and error will be displayed on stderr. You can
   change this behavior by specifying a log file in buffer manager's
   configuration (see available options in *logging* section).

Stop DBB handoff buffer manager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once started, the DBB handoff buffer manager will keep monitoring the
buffer until it is explicitly terminated.

To stop DBB handoff buffer manager, find the process id and terminate it bu
sending SIGTERM.

.. code-block:: bash

   kill -15 `pidof hdfmgr`

.. warning::

   If you have multiple DBB handoff managers running on you system, find the
   process id of the specific handoff manager with

   .. code-block::

      ps aux | grep hdfmgr

   and terminate them selectively with

   .. code-block::

      kill -15 HDFMGR_ID

   instead where HDFMGR_ID is the process id of the handoff manager you
   want to terminate.
