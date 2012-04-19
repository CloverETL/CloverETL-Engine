What is this?
=============
This module is a wrapper over Lotus Notes/Domino remote access Java libraries. The libraries need
to be provided during the run-time of the module.

Purpose
=======
The LN Java libraries license does not permit us to distribute them together with Clover.
So when working with LN, the user of Clover needs to provide path to these libraries
(every LN installation contains them). After that, the libraries will be
dynamically loaded by this module.

How does it work?
=================
The module is compiled together with the LN libraries (Notes.jar / NCSO.jar), but the libraries
are then not part of the resulting jar package. During the run-time of the module, path to libraries
is specified dynamically by the user. A new classloader is instantiated with the libraries
on its path, along with the wrapper module, which provides access to LN remote access functionality.

Compiling
=========
prerequisities:
ant build in ../cloveretl.engine
ant build in ../cloveretl.lotusdomino.commercial

build:
ant clean build

What does it produce?
=====================
Among other produced things (such as .class files) it creates cloveretl.component.dominoloader.jar
and copies it into ../cloveretl.lotusdomino.commercial/lib/
