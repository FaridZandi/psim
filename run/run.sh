BASE_DIR=/home/faridzandi/git/psim
BUILD_PATH=$BASE_DIR/build
RUN_PATH=$BASE_DIR/run
PSIM_EXE=$BUILD_PATH/psim
INPUT_PATH=$BASE_DIR/input
ARGS="$@"


cd $BUILD_PATH
make -j 48

cd $RUN_PATH
cmd="$PSIM_EXE --protocol-file-dir=$INPUT_PATH \
               --protocol-file-name=transformer128.txt \
               --machine-count=128 \
               $ARGS"
echo $cmd
eval $cmd

