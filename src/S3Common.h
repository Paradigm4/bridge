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
          std::ostringstream exceptionOutput;                                         \
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
      std::ostringstream exceptionOutput;                                    \
      exceptionOutput << "Invalid object name '" << objectName << "'";  \
      throw USER_EXCEPTION(SCIDB_SE_METADATA,                           \
                                     SCIDB_LE_UNKNOWN_ERROR)            \
                                     << exceptionOutput.str();          \
  }


namespace scidb {

static void getMetadata(Aws::S3::S3Client const &s3Client,
                        Aws::String const &bucketName,
                        Aws::String const &objectName,
                        std::map<std::string, std::string>& metadata) {
    // Download Metadata
    Aws::S3::Model::GetObjectRequest objectRequest;

    objectRequest.SetBucket(bucketName);
    objectRequest.SetKey(objectName);

    auto outcome = s3Client.GetObject(objectRequest);
    S3_EXCEPTION_NOT_SUCCESS("Get");

    // Parse Metadata
    auto& metadataStream = outcome.GetResultWithOwnership().GetBody();
    std::string line;
    while (std::getline(metadataStream, line)) {
        std::istringstream lineStream(line);
        std::string key, value;
        if (!std::getline(lineStream, key, '\t')
            || !std::getline(lineStream, value))
            throw USER_EXCEPTION(SCIDB_SE_METADATA,
                                 SCIDB_LE_UNKNOWN_ERROR)
                << "Invalid metadata line '" << line << "'";
        metadata[key] = value;
    }
}


static std::string coord2ObjectName(std::string const &bucketPrefix,
                                    Coordinates const &pos,
                                    Dimensions const &dims) {
    std::ostringstream out;
    out << bucketPrefix << "/c";
    for (size_t i = 0; i < dims.size(); ++i)
        out << "_" << (pos[i] -
                       dims[i].getStartMin()) / dims[i].getChunkInterval();
    return out.str();
}

}

#endif //S3Common
