import com.lesfurets.jenkins.unit.declarative.*

class TestExampleDeclarativeJob extends DeclarativePipelineTest {
        @Test
        void should_execute_without_errors() throws Exception {
            def script = runScript("Jenkinsfile")
            assertJobStatusSuccess()
            printCallStack()
        }
}