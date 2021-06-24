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

CPPFLAGS := -DARROW_NO_DEPRECATED_API -DNDEBUG -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -O3 -Wall -Wextra -Wno-long-long -Wno-strict-aliasing -Wno-system-headers -Wno-unused -Wno-unused-parameter -Wno-variadic-macros -fPIC -fno-omit-frame-pointer -g -std=c++14
CPPFLAGS += -DAWS_COMMON_USE_IMPORT_EXPORT -DAWS_EVENT_STREAM_USE_IMPORT_EXPORT -DAWS_SDK_VERSION_MAJOR=1 -DAWS_SDK_VERSION_MINOR=8 -DAWS_SDK_VERSION_PATCH=3
CPPFLAGS += -DPROJECT_ROOT="\"$(SCIDB)\"" -I. -I"$(SCIDB)/include" -I"$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/include"
CPPFLAGS += -I/opt/aws-sdk-cpp/include
LIBS     := -shared -Wl,-soname,libbridge.so -L . -L "$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/lib" -L "$(SCIDB)/lib" -Wl,-rpath,$(SCIDB)/lib -lm
LIBS     +=  -larrow -laws-cpp-sdk-s3
SRCS     := plugin.cpp LogicalXSave.cpp PhysicalXSave.cpp XSaveSettings.cpp LogicalXInput.cpp PhysicalXInput.cpp XArray.cpp XIndex.cpp S3Driver.cpp FSDriver.cpp Driver.cpp
HEADERS  := XSaveSettings.h XInputSettings.h XArray.h XIndex.h Driver.h FSDriver.h S3Driver.h
OBJS     := $(SRCS:%.cpp=%.o)

# Debug build
# CPPFLAGS += -Wfatal-errors

ifneq ("$(wildcard /opt/aws-sdk-cpp/lib)","")
  # -- - Ubuntu - --

  # AWS SDK CPP
  AWS_PATH := /opt/aws-sdk-cpp/lib

else ifneq ("$(wildcard /opt/aws-sdk-cpp/lib64)","")
  # -- - CentOS - --

  # AWS SDK CPP
  AWS_PATH := /opt/aws-sdk-cpp/lib64

  # Apache Arrow
  ifneq ("$(wildcard /opt/apache-arrow/lib64)","")
    ARROW_PATH := /opt/apache-arrow
    CPPFLAGS   += -I$(ARROW_PATH)/include
    LIBS       += -L$(ARROW_PATH)/lib64 -Wl,-rpath,$(ARROW_PATH)/lib64
    $(info Using Apache Arrow in $(ARROW_PATH))
  else
    $(warning CentOS/RHEL setup infered but custom Apache Arrow not found in /opt/apache-arrow/lib64)
  endif

  # cURL
  ifneq ("$(wildcard /opt/curl/lib)","")
    LIBS += -Wl,-rpath,/opt/curl/lib
    $(info Using cURL in /opt/curl/lib)
  else
    $(warning CentOS/RHEL setup infered but custom cURL not found in /opt/curl/lib. Make sure the system cURL is compiled with OpenSSL support and not NSS, otherwise the AWS connection will fail at runtime. Use curl -V to check.)
  endif
else
  $(error AWS SDK libraries not found in /opt/aws-sdk-cpp/lib[64])
endif
LIBS += -L$(AWS_PATH) -Wl,-rpath,$(AWS_PATH)
$(info Using AWS SDK CPP in $(AWS_PATH))

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

all: libbridge.so

plugin.o:

LogicalXInput.o: XInputSettings.h Driver.h
PhysicalXInput.o: XInputSettings.h Driver.h XIndex.h XArray.h

LogicalXSave.o:  XSaveSettings.h Driver.h
PhysicalXSave.o: XSaveSettings.h Driver.h XIndex.h XArray.h
XSaveSettings.o: XSaveSettings.h Driver.h

XArray.o: XArray.h XIndex.h XInputSettings.h Driver.h
XIndex.o: XIndex.h Driver.h
S3Driver.o: S3Driver.h Driver.h
FSDriver.o: FSDriver.h Driver.h
Driver.o: S3Driver.h FSDriver.h Driver.h

libbridge.so: $(OBJS)
	@if test ! -d "$(SCIDB)"; then echo  "Error. Try:\n\nmake SCIDB=<PATH TO SCIDB INSTALL PATH>"; exit 1; fi
	$(CXX) $(CPPFLAGS) -o libbridge.so $(OBJS) $(LIBS)
	@echo "Now copy *.so to your SciDB lib/scidb/plugins directory and run"
	@echo "iquery -aq \"load_library('bridge')\" # to load the plugin."
	@echo
	@echo "Re-start SciDB if the plugin was already loaded previously."
	@echo "Remember to copy the plugin to all your nodes in the cluster."

clean:
	rm -f *.so *.o
