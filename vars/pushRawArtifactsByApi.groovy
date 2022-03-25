def call(repository,directory,file,filename,user,passwd){
 sh """
   curl -X POST "http://10.1.9.200:8081/service/rest/v1/components?repository=${repository}" \
      -H "accept: application/json" \
      -H "Content-Type: multipart/form-data" \
      -F "raw.directory=${directory}" \
      -F "raw.asset1=@${file};type=application/java-archive" \
      -F "raw.asset1.filename=${filename}" \
      -u "${user}":"${passwd}"
"""
}
