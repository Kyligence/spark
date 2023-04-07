package io.kyligence.devopslib.unit

import groovy.transform.builder.Builder
import io.kyligence.devopslib.Utils
import org.apache.commons.lang3.StringUtils
import org.kohsuke.github.GitHub

class NewtenModule2Test extends Unit<Module2TestParameter, List<NamedModuleList>> {

    NewtenModule2Test(String name) {
        super(name)
    }

    static class NamedModuleList {
        String name
        List<String> modules
    }

    @Builder
    static class Module2TestParameter {
        String ghprbPullId
        String fullRepo
        String githubCre

        boolean isDaily
    }


    @Override
    void run(Module2TestParameter parameter) {
        List<String> allSubModules = []

        def parentModules = ["", "kylin"]

        def changeFiles = []

        if (StringUtils.isNotBlank(parameter.ghprbPullId) && !parameter.isDaily) {
            Utils.ctx.withCredentials([Utils.ctx.usernamePassword(credentialsId: parameter.githubCre, passwordVariable: 'token', usernameVariable: 'login')]) {
                def github = GitHub.connect(Utils.ctx.login, Utils.ctx.token)
                def repo = github.getRepository(parameter.fullRepo)
                def pr = repo.getPullRequest(Integer.valueOf(parameter.ghprbPullId))

                def changeFilesReq = pr.listFiles().iterator()
                while (changeFilesReq.hasNext()) {
                    def file = changeFilesReq.next()
                    if (file.getStatus() in ["added", "removed", "modified", "renamed"]) {
                        changeFiles.add(file.getFilename())
                    }
                }
            }
        }

        changeFiles = changeFiles.stream().distinct().collect()
        if (changeFiles.size() >= 100 || StringUtils.isBlank(parameter.ghprbPullId) || parameter.isDaily) {
            changeFiles = Utils.ctx.sh(script: "find . -name 'pom.xml' -printf '%P,'", returnStdout: true)
                    .split(",")
                    .collect({ it.trim() })
        }
        Utils.log("change files: ${changeFiles}")

        def changeModules = Utils.ctx.sh(script: "java -jar /tools/mat-0.1.2.jar -b -r -lm -d `pwd` -cl ${changeFiles.join(",")} | grep -A 2 'build command:'", returnStdout: true)
                .trim()
                .split("\n")
                .last()
                .split(",")
                .collect({ it.trim() })

        Utils.log("change modules: ${changeModules}")
        allSubModules.addAll(changeModules)

        allSubModules.removeAll(parentModules)
        Utils.log "all submodules: ${allSubModules}"

        if (!parameter.isDaily) {
            // remove clickhouse-it modules
            def clickhouse_it_module = allSubModules.findAll({ it.contains('src/second-storage') })
            if (clickhouse_it_module) {
                Utils.log "clickhouse-it modules: ${clickhouse_it_module}"
                allSubModules.removeAll(clickhouse_it_module)
            }
        }


        // get slow modules
        def slowModules = slowModules(allSubModules)
        Utils.log("slow modules: ${slowModules}")

        Collections.reverse(allSubModules)
        Utils.log("ut modules: ${allSubModules}")

        this.setResult([new NamedModuleList(name: "UTest", modules: allSubModules), new NamedModuleList(name: "ITest", modules: slowModules)])
    }

    static List<String> slowModules(List<String> allModules) {

        // 特别注意： slow modules 中的模块顺序最好是按照从大到小排列，这样能保证消费时间相对均匀
        List<String> slowModules = []

        // kylin-it 一般需要 40 mins
        def kylinIt = allModules.findAll({ it.contains('src/kylin-it') }).getAt(0)
        if (kylinIt) {
            // remove kylin-it modules
            allModules.removeAll([kylinIt])

            slowModules.addAll([kylinIt])
        }

        // 分别是 30+ 和 20+ mins
        def kapIt = allModules.findAll({ it.contains('src/kap-it') }).getAt(0)
        if (kapIt) {
            // remove kap-it modules
            allModules.removeAll([kapIt])

            // expand kap-it modules
            slowModules.addAll(["${kapIt} -Dtest='!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'"])
            slowModules.addAll(["${kapIt} -Dtest='io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'"])
        }

        // 一般是 20 mins
        def sparkIt = allModules.findAll({ it.contains('spark-it') }).getAt(0)
        if (sparkIt) {
            // remove spark-it modules in all modules list
            allModules.removeAll([sparkIt])

            slowModules.addAll([sparkIt])
        }

        return slowModules
    }


    @Override
    boolean isSuccess() {
        return getResult() != null
    }
}
