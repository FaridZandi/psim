#!/usr/bin/env bash
# create-py38-gcc94-boost.sh
# Conda env with Python 3.8, GCC/G++/GFortran 9.4, and Boost dev libs (headers + libs).

set -e

ENV_NAME=py38-gcc94

if conda env list | awk '{print $1}' | grep -Fxq "$ENV_NAME"; then
  echo "Conda environment '$ENV_NAME' already exists; activating it."
else
  echo "Creating conda environment '$ENV_NAME'."
  # Create the env from conda-forge only.
  conda create -y -n "$ENV_NAME" -c conda-forge \
    python=3.8 \
    gcc_linux-64=9.4.* \
    gxx_linux-64=9.4.* \
    gfortran_linux-64=9.4.* \
    binutils_linux-64 \
    boost-cpp \
    boost

  # Ensure the env uses the Conda toolchain and fixes addr2line var.
  conda run -n "$ENV_NAME" bash -lc '
    mkdir -p "$CONDA_PREFIX/etc/conda/activate.d"
    cat > "$CONDA_PREFIX/etc/conda/activate.d/compilers.sh" <<EOF
export CC=\$CONDA_PREFIX/bin/x86_64-conda-linux-gnu-gcc
export CXX=\$CONDA_PREFIX/bin/x86_64-conda-linux-gnu-g++
export FC=\$CONDA_PREFIX/bin/x86_64-conda-linux-gnu-gfortran
export ADDR2LINE=\$CONDA_PREFIX/bin/addr2line
EOF
'
fi

# Load the conda shell integration so "conda activate" works in scripts.
eval "$(conda shell.bash hook)"
conda activate "$ENV_NAME"

echo "Environment '$ENV_NAME' is active."
echo "Boost include dir example: $CONDA_PREFIX/include/boost"
echo "Boost libs example:        $CONDA_PREFIX/lib"
