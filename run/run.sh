BASE_DIR=/home/faridzandi/git/psim
BUILD_PATH=$BASE_DIR/build
RUN_PATH=$BASE_DIR/run
PSIM_EXE=$BUILD_PATH/psim
INPUT_PATH=$BASE_DIR/input
ARGS="$@"

echo "Running PSim with args: $ARGS"

cd $BUILD_PATH
make -j 48
cd $RUN_PATH
cmd="$PSIM_EXE --protocol-file-path=$INPUT_PATH \
               --protocol-file-name=vgg.txt \
               $ARGS"
eval $cmd

