#include "S3Common.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>


namespace scidb
{
    void getMetadata(const Aws::String bucketName,
                     const Aws::String objectName//,
                     //map<string, string> metadata
        ) {
        // Download Metadata
        // Aws::S3::S3Client s3Client;
        // Aws::S3::Model::GetObjectRequest objectRequest;

        // objectRequest.SetBucket(bucketName);
        // objectRequest.SetKey(objectName);

        // auto outcome = s3Client.GetObject(objectRequest);
        // S3_EXCEPTION_NOT_SUCCESS("Get");

        // // Parse Metadata
        // auto& metadata_stream = outcome.GetResultWithOwnership().GetBody();
        // string line;
        // while (getline(metadata_stream, line)) {
        //     istringstream line_stream(line);
        //     string key, value;
        //     if (!getline(line_stream, key, '\t'))
        //         throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
        //                              SCIDB_LE_UNKNOWN_ERROR)
        //             << "Invalid metadata line '" << line << "'";
        //     if (!getline(line_stream, value))
        //         throw USER_EXCEPTION(SCIDB_SE_ARRAY_WRITER,
        //                              SCIDB_LE_UNKNOWN_ERROR)
        //             << "Invalid metadata line '" << line << "'";
        //     metadata[key] = value;
        // }
    }
} // end namespace scidb
