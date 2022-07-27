package devopslib;

import org.apache.commons.io.IOUtils;
import org.apache.groovy.parser.antlr4.AstBuilder;
import org.codehaus.groovy.ast.CodeVisitorSupport;
import org.codehaus.groovy.ast.GroovyCodeVisitor;
import org.codehaus.groovy.ast.ModuleNode;
import org.codehaus.groovy.ast.expr.ArgumentListExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.control.SourceUnit;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

@Deprecated
public class SimpleTest {

    public static void main(String[] args) throws IOException {
        final File sourceFile = new File("/Users/cheng.zuo/Documents/devopslib/pipelines/local/Handbook-pdf-CD/Jenkinsfile");
        ModuleNode ast = SourceUnit.create(sourceFile.getPath(), IOUtils.toString(new FileReader(sourceFile))).buildAST();

        final AtomicReference<MethodCallExpression> pipeline = new AtomicReference<>();

        final GroovyCodeVisitor findPipelineBlock = new CodeVisitorSupport() {
            @Override
            public void visitMethodCallExpression(MethodCallExpression call) {
                if (call.getMethodAsString().equals("pipeline")) {
                    pipeline.set(call);
                    return;
                }
                super.visitMethodCallExpression(call);
            }

        };

        ast.getStatementBlock().visit(findPipelineBlock);

        if (pipeline.get() == null) {
            throw new IllegalArgumentException();
        }

        pipeline.get().visit(new CodeVisitorSupport() {
            @Override
            public void visitMethodCallExpression(MethodCallExpression call) {
                System.out.println(call);
                super.visitMethodCallExpression(call);
            }
        });


    }
}
