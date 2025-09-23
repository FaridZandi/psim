#!/usr/bin/env bash

# This script merges the current backup branch (backup-XXX) back into master
# with a single commit, including a summary of all commits from the backup branch.

# Usage:
#   ./merge-backup.sh base-branch

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

# if there's a second argument, it's the branch to merge into. default is defined below
# Get the current branch name (should be something like backup-42)
if [ -n "$1" ]; then
  PARENT_BRANCH="$1"
else
  PARENT_BRANCH=$(git reflog | grep "$CURRENT_BRANCH" | grep 'checkout:' | head -n 1 | sed -E 's/.*checkout: moving from ([^ ]+) to .*/\1/')
fi

# Find the parent branch (the branch from which the backup branch was created)
if [ -z "$PARENT_BRANCH" ]; then
  echo "ERROR: Could not determine the parent branch."
  exit 1
fi

echo "Merging $CURRENT_BRANCH into $PARENT_BRANCH..."

# verify with the user that this is correct
read -p "Is this correct? (y/n) " -n 1 -r
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Aborted."
  exit 1
fi


# Check if the current branch name starts with "backup-"
if [[ "$CURRENT_BRANCH" != backup-* ]]; then
  echo "ERROR: You are not on a 'backup-*' branch."
  echo "Please switch to the correct backup branch before running this script."
  exit 1
fi

# Get the list of commits from master..CURRENT_BRANCH (exclude merges for clarity)
COMMITS=$(git log master..$CURRENT_BRANCH --pretty=format:"- %h: %s" --no-merges)

echo "Commits to merge:"    
echo "$COMMITS" 

# Confirm the merge
echo "About to merge $CURRENT_BRANCH into master with the following message:"
echo "$MESSAGE"

# ask for confirmation
read -p "Continue? (y/n) " -n 1 -r

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "Aborted."
  exit 1
fi



# Switch to master branch
git checkout master

# Pull any latest changes (optional but recommended)
# git pull origin master

# Merge the backup branch into master as a single commit (squash merge)
git merge --squash "$CURRENT_BRANCH"


# print a complete diff of what will be committed. print it all out. don't paginate
echo "The following changes will be committed:" 
git --no-pager diff --staged 

# ask for the message now 
# read the commit message from the user 
read -p "Enter a commit message: " MESSAGE

# Commit using the provided message, plus a summary of all commits from the backup branch
git commit -m "Merging $CURRENT_BRANCH: $MESSAGE"

# Push the merged result back to master
git push origin master

echo "Successfully merged $CURRENT_BRANCH into master and pushed changes."
