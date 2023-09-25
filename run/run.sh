BASE_DIR=/home/faridzandi/git/psim
BUILD_PATH=$BASE_DIR/build
RUN_PATH=$BASE_DIR/run
PSIM_EXE=$BUILD_PATH/psim
INPUT_PATH=$BASE_DIR/input/128search-dpstart-2
ARGS="$@"


cd $BUILD_PATH
make -j 48

cd $RUN_PATH
cmd="$PSIM_EXE --protocol-file-dir=$INPUT_PATH \
               --protocol-file-name=candle128-comm.txt \
               --step-size=1 \
               --console-log-level=2 \
               --link-bandwidth=100 \
               --initial-rate=50 \
               --min-rate=40 \
               --ft-server-tor-link-capacity-mult=1 \
               --ft-tor-agg-link-capacity-mult=1 \
               --ft-agg-core-link-capacity-mult=1 \
               --priority-allocator=priorityqueue \
               $ARGS"
echo $cmd
eval $cmd

