package edu.unc.mapseq.commands.ncnexus38.demultiplex;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;

@Command(scope = "ncnexus-casava", name = "run-workflow", description = "Run NCNEXUS CASAVA Workflow")
@Service
public class RunWorkflowAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(RunWorkflowAction.class);

    @Argument(index = 0, name = "workflowRunName", description = "WorkflowRun.name", required = true, multiValued = false)
    private String workflowRunName;

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Reference
    private MaPSeqConfigurationService maPSeqConfigurationService;

    public RunWorkflowAction() {
        super();
    }

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                String.format("nio://%s:61616", maPSeqConfigurationService.getWebServiceHost("localhost")));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncnexus.casava");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            String format = "{\"entities\":[{\"entityType\":\"WorkflowRun\",\"name\":\"%s\"}]}";
            producer.send(session.createTextMessage(String.format(format, workflowRunName)));
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }
}
