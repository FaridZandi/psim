#!/usr/bin/env bash

# This script merges the current backup branch (backup-XXX) back into master
# with a single commit, including a summary of all commits from the backup branch.

# Usage:
#   ./merge-backup.sh "Your descriptive commit message"

MESSAGE="$1"

# Make sure a commit message was provided
if [ -z "$MESSAGE" ]; then
  echo "Usage: $0 \"Commit message\""
  exit 1
fi

# Get the current branch name (should be something like backup-42)
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

echo "Merging $CURRENT_BRANCH into master..."


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

# Commit using the provided message, plus a summary of all commits from the backup branch
git commit -m "Merging $CURRENT_BRANCH: $MESSAGE"

# Push the merged result back to master
git push origin master

echo "Successfully merged $CURRENT_BRANCH into master and pushed changes."
