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
    echo "running git checkout -b $branch_name"
    git checkout -b $branch_name

    echo "running git add -u"
    git add -u 

    echo "running git commit -m backup $(date)" 
    git commit -m "backup $(date)"

    echo "running git push --set-upstream origin $branch_name"  
    git push --set-upstream origin $branch_name

else
    # just keep going on the current branch
    echo "running git add -u"   
    git add -u

    echo "running git commit -m backup $(date)"
    git commit -m "backup $(date)"

    echo "running git push --set-upstream origin $current_branch"   
    git push --set-upstream origin $branch_name
fi

