#include <map>

#include <system/ErrorCodes.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>


#ifndef S3_COMMON
#define S3_COMMON

#define S3BRIDGE_VERSION 1

#define S3_EXCEPTION_NOT_SUCCESS(operation)                                      \
  {                                                                              \
      if (!outcome.IsSuccess()) {                                                \
          ostringstream exceptionOutput;                                         \
          exceptionOutput << (operation) << " operation on s3://"                \
                           << bucketName << "/" << objectName << " failed. ";    \
          auto error = outcome.GetError();                                       \
          exceptionOutput << error.GetMessage();                                 \
          if (error.GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN) \
              exceptionOutput << "See https://aws.amazon.com/premiumsupport/"    \
                               << "knowledge-center/s3-troubleshoot-403/";       \
          throw USER_EXCEPTION(SCIDB_SE_NETWORK,                                 \
                               SCIDB_LE_UNKNOWN_ERROR) << exceptionOutput.str(); \
      }                                                                          \
  }


#define S3_EXCEPTION_OBJECT_NAME                                        \
  {                                                                     \
      ostringstream exceptionOutput;                                    \
      exceptionOutput << "Invalid object name '" << objectName << "'";  \
      throw USER_EXCEPTION(SCIDB_SE_METADATA,                           \
                                     SCIDB_LE_UNKNOWN_ERROR)            \
                                     << exceptionOutput.str();          \
  }


using namespace std;
using namespace scidb;


static void getMetadata(Aws::S3::S3Client& s3Client,
                        const Aws::String& bucketName,
                        const Aws::String& objectName,
                        map<string, string>& metadata) {
    // Download Metadata
    Aws::S3::Model::GetObjectRequest objectRequest;

    objectRequest.SetBucket(bucketName);
    objectRequest.SetKey(objectName);

    auto outcome = s3Client.GetObject(objectRequest);
    S3_EXCEPTION_NOT_SUCCESS("Get");

    // Parse Metadata
    auto& metadataStream = outcome.GetResultWithOwnership().GetBody();
    string line;
    while (getline(metadataStream, line)) {
        istringstream lineStream(line);
        string key, value;
        if (!getline(lineStream, key, '\t') || !getline(lineStream, value))
            throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                 SCIDB_LE_UNKNOWN_ERROR)
                << "Invalid metadata line '" << line << "'";
        metadata[key] = value;
    }
}

#endif //S3Common
