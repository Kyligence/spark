def call(repository,directory,filename,user,passwd){
 sh """
   curl -X POST "http://10.1.9.11:8081/service/rest/v1/components?repository=${repository}" \
      -H "accept: application/json" \
      -H "Content-Type: multipart/form-data" \
      -F "raw.directory=${directory}" \
      -F "raw.asset1=@target/${filename};type=application/java-archive" \
      -F "raw.asset1.filename=${filename}" \
      -u "${user}":"${passwd}"
"""
}


// curl -X POST "http://mvn.ops.kylincorp.com/service/rest/v1/components?repository=raw-tars-hosted" \
//       -H "accept: application/json" \
//       -H "Content-Type: multipart/form-data" \
//       -F "raw.directory=/devops/devops-web-service" \
//       -F "raw.asset1=@enterprise_v4.5.tar.gz;type=application/x-gzip" \
//       -F "raw.asset1.filename=enterprise_v4.5.tar.gz" \
//       -u [user]:[pass]

// curl -X POST "http://10.1.9.11:8081/service/rest/v1/components?repository=raw-tars-hosted" \
//       -H "accept: application/json" \
//       -H "Content-Type: multipart/form-data" \
//       -F "raw.directory=/devops/devops-web-service" \
//       -F "raw.asset1=@enterprise_v4.5.tar.gz;type=application/x-gzip" \
//       -F "raw.asset1.filename=enterprise_v4.5.tar.gz" \
//       -u [user]:[pass]


