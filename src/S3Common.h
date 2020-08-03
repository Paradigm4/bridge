#ifndef S3_COMMON
#define S3_COMMON

#include <map>
#include <system/UserException.h>
#include <aws/core/Aws.h>


#define S3BRIDGE_VERSION 1

#define S3_EXCEPTION_NOT_SUCCESS(operation)                             \
  {                                                                     \
      if (!outcome.IsSuccess()) {                                                       \
          ostringstream exception_output;                                               \
          exception_output << (operation) << " operation on s3://"                      \
                           << bucketName << "/" << objectName << " failed. ";           \
          auto error = outcome.GetError();                                              \
          exception_output << error.GetMessage() << ". ";                               \
          if (error.GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN)        \
              exception_output << "See https://aws.amazon.com/premiumsupport/"          \
                               << "knowledge-center/s3-troubleshoot-403/";              \
          throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,                                   \
                               SCIDB_LE_UNKNOWN_ERROR) << exception_output.str();       \
      }                                                                                 \
  }


using namespace std;


void getMetadata(const Aws::String bucketName,
                 const Aws::String objectName//,
                 //map<string, string> metadata
                 );

#endif //S3Common
