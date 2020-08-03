ifeq ($(SCIDB),)
  X := $(shell which scidb 2>/dev/null)
  ifneq ($(X),)
    X := $(shell dirname ${X})
    SCIDB := $(shell dirname ${X})
  endif
endif

# A way to set the 3rdparty prefix path that is convenient
# for SciDB developers.
ifeq ($(SCIDB_VER),)
  SCIDB_3RDPARTY := $(SCIDB)
else
  SCIDB_3RDPARTY := /opt/scidb/$(SCIDB_VER)
endif

# A better way to set the 3rdparty prefix path that does
# not assume an absolute path. You can still use the above
# method if you prefer.
ifeq ($(SCIDB_THIRDPARTY_PREFIX),)
  SCIDB_THIRDPARTY_PREFIX := $(SCIDB_3RDPARTY)
endif

# Debug:
CFLAGS  := -DARROW_NO_DEPRECATED_API -DNDEBUG -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -O3 -Wall -Wextra -Wno-long-long -Wno-strict-aliasing -Wno-system-headers -Wno-unused -Wno-unused-parameter -Wno-variadic-macros -fPIC -fno-omit-frame-pointer -g -std=c++14
CFLAGS  += -DAWS_COMMON_USE_IMPORT_EXPORT -DAWS_EVENT_STREAM_USE_IMPORT_EXPORT -DAWS_SDK_VERSION_MAJOR=1 -DAWS_SDK_VERSION_MINOR=8 -DAWS_SDK_VERSION_PATCH=3

INC     := -DPROJECT_ROOT="\"$(SCIDB)\"" -I. -I"$(SCIDB)/include" -I"$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/include"
INC     += -I/opt/aws/include

LIBS    := -shared -Wl,-soname,libs3backer.so -L . -L "$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/lib" -L "$(SCIDB)/lib" -Wl,-rpath,$(SCIDB)/lib -lm -larrow
LIBS    += -rdynamic /opt/aws/lib/libaws-cpp-sdk-s3.so -lm -lrt -ldl -Wl,-rpath,/opt/aws/lib

SRCS    := plugin.cpp S3Common.cpp LogicalS3Save.cpp PhysicalS3Save.cpp LogicalS3Load.cpp PhysicalS3Load.cpp
HEADERS := S3Common.h S3SaveSettings.h S3LoadSettings.h
OBJS    := $(SRCS:%.cpp=%.o)


# Compiler settings for SciDB version >= 15.7
ifneq ("$(wildcard /usr/bin/g++-4.9)","")
  CC  := "/usr/bin/gcc-4.9"
  CXX := "/usr/bin/g++-4.9"
else
  ifneq ("$(wildcard /opt/rh/devtoolset-3/root/usr/bin/gcc)","")
   CC  := "/opt/rh/devtoolset-3/root/usr/bin/gcc"
   CXX := "/opt/rh/devtoolset-3/root/usr/bin/g++"
  endif
endif


%.o: %.cpp $(HEADERS)
	$(CXX) $(CFLAGS) $(INC) -c $< -o $@

all: libs3bridge.so

libs3bridge.so: $(OBJS)
	@if test ! -d "$(SCIDB)"; then echo  "Error. Try:\n\nmake SCIDB=<PATH TO SCIDB INSTALL PATH>"; exit 1; fi
	$(CXX) $(CFLAGS) $(INC) -o libs3bridge.so $(OBJS) $(LIBS)
	@echo "Now copy *.so to your SciDB lib/scidb/plugins directory and run"
	@echo "iquery -aq \"load_library('s3bridge')\" # to load the plugin."
	@echo
	@echo "Re-start SciDB if the plugin was already loaded previously."
	@echo "Remember to copy the plugin to all your nodes in the cluster."

clean:
	rm -f *.so *.o
