#!/usr/bin/env bash
set -e
cd "$(dirname $0)/.."

UNPUSHED=$( git rev-list --count origin/master..master )
if [ $UNPUSHED -gt 0 ]; then
	echo "You have $UNPUSHED commit(s) on master which have not been pushed to origin"
	echo "Aborting deployment"
	exit 1
fi

echo "Pushing tags"
git push --tags

echo "Rebuild package repo"
curl 'https://bsts.bunnysites.com/api/composer/packages/run?script=build'

# TODO figure out how to verify the commit hash

echo "Done"
