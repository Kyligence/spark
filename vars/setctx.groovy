import io.kyligence.devopslib.Utils

def call() {
    echo "set context: ${this}"
    Utils.ctx = this
}
