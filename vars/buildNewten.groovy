def call(String version, boolean skipPatch = true, boolean skipLicense = true) {
    sh "git apply license.patch"
    
    sh "export release_version=${version} && sh build/script_newten/release.sh -noTimestamp"
}
