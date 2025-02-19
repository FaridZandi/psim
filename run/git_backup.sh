#!/usr/bin/env bash

# create a backup of the git repository

# if there's an error, stop the script  
set -e

# if we are the main branch 
current_branch=$(git branch | grep \* | cut -d ' ' -f2) 

if [ $current_branch == "master" ]; then
    # get the current number from number.txt
    if [ ! -f number.txt ]; then
        echo 0 > number.txt
    fi
    number=$(cat number.txt)
    number=$((number+1))
    echo $number > number.txt

    hostname=$(hostname)

    # create a new branch
    branch_name="backup-$hostname-$number"
    echo "Creating branch $branch_name"

    # push the changes 
    git checkout -b $branch_name
    git add -u 
    git commit -m "backup $(date)"
    git push --set-upstream origin $branch_name

else
    # just keep going on the current branch
    git add -u
    git commit -m "backup $(date)"
    git push --set-upstream origin $branch_name
fi

