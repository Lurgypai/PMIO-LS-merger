rm -r out
mkdir out && cd out
# cmake .. -DCMAKE_CXX_FLAGS="-fno-omit-frame-pointer"
cmake .. -DCMAKE_CXX_FLAGS="-g -O0"
# cmake .. -DCMAKE_CXX_FLAGS="-O3"
