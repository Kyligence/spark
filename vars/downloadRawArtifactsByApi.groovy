def call(repository,directory,filename,user,passwd){
 sh """
  curl -u "${user}":"${passwd}" "https://repo-ofs.kyligence.com/repository/${repository}/${directory}/${filename}" -o ${filename} 
"""
}