package io.kyligence.devopslib.byzer.pojo


class ExecuteScript {
    String cell_id
    String notebook
    String sql

    ExecuteScript() {
    }

    ExecuteScript(String cell_id, String notebook, String sql) {
        this.cell_id = cell_id
        this.notebook = notebook
        this.sql = sql
    }

    static class Builder {
        String cell_id
        String notebook
        String sql

        Builder notebook(String notebook) {
            this.notebook = notebook
            return this
        }

        Builder cell_id(String cellId) {
            this.cell_id = cellId
            return this
        }

        Builder sql(String sql) {
            this.sql = sql
            return this
        }


        ExecuteScript build() {
            return new ExecuteScript(this.cell_id, this.notebook, this.sql)
        }
    }

    static Builder builder() {
        return new Builder()
    }
}
