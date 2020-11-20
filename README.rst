=========================
EMMA Submission Processor
=========================
------------------------
Internet Archive edition
------------------------

.. contents::

Background
==========
The `EMMA application <https://emma.lib.virginia.edu>`_ supports academic DSOs
(disability services offices) by providing an interface to search for
accessibility-remediated content offered by member repositories Bookshare,
Internet Archive, and HathiTrust, as well as remediated materials uploaded
directly to the EMMA repository by participating DSOs.

Unlike these "EMMA-native" items, which are stored and managed by EMMA itself,
submissions derived from content supplied by one of the member repositories is
offered back to the original repository.

AWS S3 bucket "drop site"
=========================
Each of the member repositories has been allocated an AWS S3 bucket, which
serves as a "drop site" for submissions.  (This may also be referred to as the
"submission queue", however it is not really a "queue" in the classical sense
[and also note that it has nothing to do with AWS SQS].)

When an EMMA user submits a derived remediated file, the submission is made
available to the responsible member repository as a pair of files which
together make up the "submission":  The **package** file and the **data** file.

The names of the files (*i.e.*, their AWS object keys) contain their unique
EMMA submission ID, but each ends with a different file name extension.

Package file
------------
This file always as an object key name which ends with the extension ``.xml``;
it contains the submission metadata serving two purposes:

* To identify the original title-level member repository item on which the
  remediated file was based.
* To describe the nature of the specific remediation.

The submission schema does not explicitly name the location of the associated
data file -- this is implicit in the naming convention of the submission queue.

Data file
---------
This file will have a file extension related to its content MIME type;
frequently this will be ``.zip``.

EMMA currently only accepts single-file submissions, thus any submissions
comprised of multiple files will necessarily have to be wrapped in an archival
format file.  (There is no current requirement that this be ZIP format.)

Queue Processing
================
The Python program associated with this GitHub repository performs the basic
steps required to complete the transfer of submissions to the responsible
member repository:

* Scan the AWS S3 bucket for submissions
* Parse submission information packages
* Upload submission data files to Internet Archive
* Remove completed submissions

This program is intended to be run at fixed intervals to detect new submissions
after they have arrived, transmit them, and then remove them from the "drop
site".

Caveats
=======
* The mechanism for periodic operation has not yet been decided upon.
* I'm something of a Python novice, so I'm sure there's plenty of room for
  improvement in the project.
