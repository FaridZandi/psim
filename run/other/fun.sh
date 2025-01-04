# loop 1 to 40 
# for i in {1..39}
# do 
#     file_path="/tmp2/workers/worker-$i/run-1/runtime.txt"
#     # print the last line of the file
#     tail -n 1 $file_path | head -n 1 
# done 

# make a function out of the loop above 

worker_count=39

function get_runtime {
    for i in {0..worker_count}
    do 
        file_path="/tmp2/workers/worker-$i/run-1/runtime.txt"
        # print the last line of the file
        tail -n 1 $file_path | head -n 1 
    done 
}

# call the function every 2 seconds

while true
do 
    get_runtime
    sleep 2
done

