def call(repository,directory,filename,user,passwd){
 sh """
  curl -u "${user}":"${passwd}" "http://repo-ofs.kyligence.tech:8081/repository/${repository}/${directory}/${filename}" -o ${filename} 
"""
}