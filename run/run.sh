BASE_DIR=/home/faridzandi/git/psim
BUILD_PATH=$BASE_DIR/build
RUN_PATH=$BASE_DIR/run
PSIM_EXE=$BUILD_PATH/psim
INPUT_PATH=$BASE_DIR/input/128search-dpstart-2
ARGS="$@"
USE_GDB=0

cd $BUILD_PATH
make -j 48
if [ $? -ne 0 ]; then
    echo "Error: Make command failed. Exiting."
    exit 1
fi


cd $RUN_PATH
cmd="$PSIM_EXE --protocol-file-dir=$INPUT_PATH \
               --protocol-file-name=candle128-comm.txt \
               --step-size=1 \
               --link-bandwidth=20 \
               --initial-rate=20 \
               --min-rate=20 \
               --ft-core-count=8 \
               --ft-agg-per-pod=2 \
               --ft-server-tor-link-capacity-mult=1 \
               --ft-tor-agg-link-capacity-mult=1 \
               --ft-agg-core-link-capacity-mult=1 \
               --priority-allocator=fairshare \
               $ARGS"


if [ $USE_GDB -eq 1 ]; then
    cmd="gdb --args $cmd"
fi

echo $cmd
eval $cmd

