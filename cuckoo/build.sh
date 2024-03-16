#!/bin/bash

third=`pwd`
prefix= $1
lualib=$prefix/rock/share
luajit=$prefix/luajit
luajit_inc=$prefix/luajit/include/luajit-2.1
luajit_lib=$prefix/luajit/lib

  if [ -z "$prefix" ]; then
      echo "not found openresty prefix path"
      exit -1
  fi

gcc -O2 -Ic -I$luajit_inc -L$luajit_lib -lluajit-5.1 *.c -fPIC -shared -o cuckoo.so

rm -rf $prefix/lualib/cuckoo.so

mv cuckoo.so $prefix/lualib/cuckoo.so

