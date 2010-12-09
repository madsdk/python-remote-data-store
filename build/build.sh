#!/bin/sh

SRCDIR=../src/

# Check for version number
if [ "$1" = "" ]; then
	echo "Usage: build.sh version_number";
	exit 1
else
	VERSION="$1"
fi
BUILDDIR=datastore-$VERSION

# sed hack...
if [ `uname -s` = "Darwin" ]; then 
	SED="sed -i \"\""
else
	SED="sed -i\"\""
fi

# Create the $BUILDDIR dir
if [ -d $BUILDDIR ]; then 
	rm -rf $BUILDDIR;
fi
mkdir -p $BUILDDIR/datastore

# Copy python files into the $BUILDDIR dir.
cp $SRCDIR/datastore/*.py $BUILDDIR/datastore
cp setup.py $BUILDDIR/
$SED s/VERSION/$VERSION/ $BUILDDIR/setup.py

tar cfz $BUILDDIR.tar.gz $BUILDDIR

exit 0
