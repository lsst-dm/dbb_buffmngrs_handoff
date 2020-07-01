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

DBB handoff buffer manager uses internally ``scp`` to transfer file to the
endpoint site, thus you will need:

#. have ``scp`` installed on you machine (Well, duh!),
#. a user account on *the endpoint site* with a passwordless login enabled.

Refer to the documentation of your Linux distribution how to install ``scp``.
For example, on Centos you can install it with

.. code-block::

   sudo yum install openssh-clients

To enable passwordless login, you need to have a SSH key pair and copy the
public SSH key of the user who will be running the DBB handoff buffer manager to
``authorized_keys`` of the user on *the endpoint* site.

The SSH key must allow for unsupervised logins. So either you need a one with
an empty passphrase or you need set up `ssh-agent` to manage your key.

You may find more information how to generate and manage SSH keys for example
`here`__ or `here`__.

.. __: https://wiki.archlinux.org/index.php/SSH_keys
.. __: https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent

Finally, to run the manager, you'll need a minimal LSST stack installation,
i.e, with `Miniconda`__, `EUPS`__ and ``base`` meta package installed.

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
checkout <ver>`` *before* ``setup -r .`` where ``<ver>`` is either a existing
git tag or branch.

Test DBB handoff buffer manager installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After you’ve installed DBB handoff buffer manager, you can run `transd.py
--help` to check if the installation was successful and see its usage.

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
     user: jdoe
     host: example.edu
     staging: /data/staging
     buffer: /data/buffer

While configuration of the handoff site is essentially self-explanatory, the
specification of the endpoint site requires some clarification.

The handoff manager doesn't transfer files directly to the buffer on the
endpoint site.  Initially, it starts writing each file to a separated
directory called a **staging area**. Only after the transfer for a file is
completed, the manager moves it to the buffer.  This approach ensures that
writing files to a buffer is an atomic operation which is required by the
endpoint manager to function properly.

.. note::

   To see other supported configuration options, look at example
   configuration in ``etc/trans.yaml``in the DBB handoff buffer manager
   repository.

Run DBB handoff buffer manager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Having the handoff and endpoint sites defined in the `transd.yaml`, you can
start DBB handoff buffer manager with:

.. code-block:: bash

   transd.py -c transd.yml

.. note::

   By default, all warnings and error will be displayed on stderr. You can
   change this behavior by specifing a log file in buffer manager's
   configuration (see available options in *logging* section).

Stop DBB handoff buffer manager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once started, DBB handoff buffer manager will keep monitoring the buffer until
it is explicitly terminated. You can stop it by pressing ``Ctrl+C``.
