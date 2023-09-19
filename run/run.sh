BASE_DIR=/home/faridzandi/git/psim
BUILD_PATH=$BASE_DIR/build
RUN_PATH=$BASE_DIR/run
PSIM_EXE=$BUILD_PATH/psim
INPUT_PATH=$BASE_DIR/input/128search
ARGS="$@"


cd $BUILD_PATH
make -j 48

cd $RUN_PATH
cmd="$PSIM_EXE --protocol-file-dir=$INPUT_PATH \
               --protocol-file-name=candle128-simtime.txt \
               --step-size=1 \
               --console-log-level=4 \
               $ARGS"
echo $cmd
eval $cmd

