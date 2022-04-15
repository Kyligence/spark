def call(repository,directory,filename,user,passwd){
 sh """
  curl -u "${user}":"${passwd}" "http://10.1.9.200:8081/repository/${repository}/${directory}/${filename}" -o ${filename} 
"""
}