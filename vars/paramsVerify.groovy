// def call(String way, String keyword) {
//     switch (way) {
//         case 'VERSION':
//             versionVerify(keyword)
//             break
//     }
// }

def version(version) {
    println "versionVerify -> version[${version}]"
    def reg = /^\d(\.\d+){1,4}$/
    if (!version) {
        error 'version 版本号不能为空'
    }
    if (!version.matches(reg)) {
        error 'version 版本号格式有误'
    }
}
